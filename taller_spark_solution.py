import os
from decimal import Decimal
from functools import reduce

from pyspark.sql import SparkSession, DataFrame, Column, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

def get_spark(app_name="taller-data-engineer"):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


COLS = {
    "customers": ["customer_id", "first_name", "last_name", "email", "country", "signup_date", "segment"],
    "products": ["product_id", "product_name", "category", "unit_price", "active"],
    "orders": ["order_id", "customer_id", "order_ts", "status", "channel", "updated_at"],
    "order_items": ["line_id", "order_id", "product_id", "quantity", "discount_pct"],
}


def str_schema(cols):
    return StructType([StructField(c, StringType(), True) for c in cols])


def norm(df: DataFrame) -> DataFrame:
    return df.select(*[
        F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c))).alias(c)
        for c in df.columns
    ])


def read_csv(spark, name):
    return norm(spark.read.option("header", True).option("inferSchema", False)
                .schema(str_schema(COLS[name])).csv(f"datos/{name}.csv"))


def read_jsonl(spark, filename, name):
    return norm(spark.read.schema(str_schema(COLS[name])).json(f"datos/incremental/{filename}"))


def dedupe(df, key, order_cols):
    w = Window.partitionBy(key).orderBy(*order_cols)
    ranked = df.withColumn("_rn", F.row_number().over(w))
    return ranked.where("_rn = 1").drop("_rn"), ranked.where("_rn > 1").drop("_rn")


def to_q(df: DataFrame, entity: str, key: str, reason) -> DataFrame:
    r = reason if isinstance(reason, Column) else F.lit(reason)
    return df.select(
        F.lit(entity).alias("entity"),
        F.col(key).cast("string").alias("record_key"),
        r.alias("reason"),
        F.to_json(F.struct(*[F.col(c) for c in COLS[entity]])).alias("payload"),
    )


def union_all(dfs):
    return reduce(lambda a, b: a.unionByName(b), dfs)


def row_hash(cols):
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in cols]), 256)


def prepare_customers(raw):
    null_key = raw.where(F.col("customer_id").isNull())
    df = raw.where(F.col("customer_id").isNotNull())
    completeness = sum(F.col(c).isNotNull().cast("int") for c in raw.columns)
    df = df.withColumn("_complete", completeness).withColumn("_signup_ts", F.try_to_timestamp("signup_date"))
    kept, removed = dedupe(df, "customer_id", [F.col("_complete").desc(), F.col("_signup_ts").desc_nulls_last(),
                                                row_hash(raw.columns)])
    q = union_all([to_q(null_key, "customers", "customer_id", "missing_customer_id"),
                   to_q(removed, "customers", "customer_id", "duplicate_customer_id")])
    return kept.drop("_complete", "_signup_ts"), q


def prepare_products(raw):
    df = raw.withColumn("_price", F.col("unit_price").try_cast("decimal(12,2)"))
    reason = (F.when(F.col("product_id").isNull(), "missing_product_id")
               .when(F.col("unit_price").isNull(), "missing_price")
               .when(F.col("_price").isNull(), "malformed_price")
               .when(F.col("_price") <= 0, "non_positive_price"))
    df = df.withColumn("_reason", reason)
    bad = df.where(F.col("_reason").isNotNull())
    good = df.where(F.col("_reason").isNull())
    kept, removed = dedupe(good, "product_id", [F.col("product_name").asc_nulls_last(), row_hash(raw.columns)])
    q = union_all([to_q(bad, "products", "product_id", F.col("_reason")),
                   to_q(removed, "products", "product_id", "duplicate_product_id")])
    return kept.drop("_reason"), q


def prepare_orders(raw_all, valid_customers):
    df = (raw_all
          .withColumn("status", F.when(F.lower(F.col("status")) == "deliverd", F.lit("delivered"))
                                 .otherwise(F.lower(F.col("status"))))
          .withColumn("channel", F.lower(F.col("channel")))
          .withColumn("_order_ts", F.try_to_timestamp("order_ts"))
          .withColumn("_updated_ts", F.try_to_timestamp("updated_at"))
          .withColumn("_version_ts", F.coalesce(F.col("_updated_ts"), F.col("_order_ts"))))
    null_key = df.where(F.col("order_id").isNull())
    df = df.where(F.col("order_id").isNotNull())

    latest, superseded = dedupe(df, "order_id", [F.col("_version_ts").desc_nulls_last(), F.col("_src").desc(),
                                                  row_hash(COLS["orders"])])

    cust_ids = valid_customers.select("customer_id", F.lit(True).alias("_has_customer")).distinct()
    v = latest.join(cust_ids, "customer_id", "left")
    reason = (F.when(F.col("customer_id").isNull(), "missing_customer_id")
               .when(F.col("_order_ts").isNull(), "missing_or_malformed_order_ts")
               .when(F.col("_has_customer").isNull(), "orphan_customer_id"))
    v = v.withColumn("_reason", reason)
    good = v.where(F.col("_reason").isNull()).drop("_reason", "_has_customer")
    bad = v.where(F.col("_reason").isNotNull())
    q = union_all([to_q(null_key, "orders", "order_id", "missing_order_id"),
                   to_q(bad, "orders", "order_id", F.col("_reason"))])
    return good, superseded, q


def prepare_items(raw_all, valid_orders, valid_products, all_orders=None, all_products=None):
    null_key = raw_all.where(F.col("line_id").isNull())
    df = raw_all.where(F.col("line_id").isNotNull())
    kept, removed = dedupe(df, "line_id", [F.col("_src").desc(), row_hash(COLS["order_items"])])

    kept = (kept.withColumn("_qty", F.col("quantity").try_cast("int"))
                .withColumn("_disc", F.col("discount_pct").try_cast("decimal(6,4)")))

    ords_val = valid_orders.select("order_id", F.lit(True).alias("_val_ord")).distinct()
    prods_val = valid_products.select("product_id", F.lit(True).alias("_val_prod")).distinct()

    ords_raw = (all_orders if all_orders is not None else valid_orders).select("order_id", F.lit(True).alias("_raw_ord")).distinct()
    prods_raw = (all_products if all_products is not None else valid_products).select("product_id", F.lit(True).alias("_raw_prod")).distinct()

    kept = (kept.join(ords_val, "order_id", "left")
                .join(prods_val, "product_id", "left")
                .join(ords_raw, "order_id", "left")
                .join(prods_raw, "product_id", "left"))

    reason = (
        F.when(F.col("order_id").isNull(), "missing_order_id")
         .when(F.col("product_id").isNull(), "missing_product_id")
         .when(F.col("_qty").isNull() | (F.col("_qty") <= 0), "invalid_quantity")
         .when(F.col("_disc").isNull() | (F.col("_disc") < 0) | (F.col("_disc") > 1), "invalid_discount")
         .when(F.col("_raw_ord").isNull(), "orphan_order")
         .when(F.col("_val_ord").isNull(), "quarantine_order")
         .when(F.col("_raw_prod").isNull(), "orphan_product")
         .when(F.col("_val_prod").isNull(), "quarantine_product")
    )
    kept = kept.withColumn("_reason", reason)
    good = kept.where(F.col("_reason").isNull()).drop("_reason", "_val_ord", "_val_prod", "_raw_ord", "_raw_prod")
    bad = kept.where(F.col("_reason").isNotNull())
    q = union_all([
        to_q(null_key, "order_items", "line_id", "missing_line_id"),
        to_q(removed, "order_items", "line_id", "duplicate_line_id"),
        to_q(bad, "order_items", "line_id", F.col("_reason")),
    ])
    return good, q


def build_sales_detail(items_good, orders_ok, products_ok, customers_ok):
    o = orders_ok.select("order_id", "customer_id", "_order_ts", "status", "channel")
    p = products_ok.select("product_id", "product_name", "category", F.col("_price").alias("unit_price"))
    c = customers_ok.select("customer_id", "country")
    return (
        items_good.select("line_id", "order_id", "product_id", F.col("_qty").alias("quantity"),
                          F.col("_disc").alias("discount_pct"))
        .join(o, "order_id").join(p, "product_id").join(c, "customer_id")
        .withColumn("sales_month", F.date_format("_order_ts", "yyyy-MM"))
        .withColumn("net_amount",
                    F.round(F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount_pct")), 2)
                    .cast("decimal(18,2)"))
        .select("line_id", "order_id", "customer_id", "country", "channel", "status", "sales_month",
                "product_id", "product_name", "category", "quantity", "unit_price", "discount_pct", "net_amount")
    )


def run_pipeline(spark, df_cust_raw, df_prod_raw, df_ord_raw, df_items_raw,
                 df_ord_inc=None, df_items_inc=None, include_incremental=False, repeat_incremental=1):
    o_parts = [df_ord_raw.withColumn("_src", F.lit(0))]
    i_parts = [df_items_raw.withColumn("_src", F.lit(0))]
    if include_incremental and df_ord_inc is not None and df_items_inc is not None:
        for _ in range(repeat_incremental):
            o_parts.append(df_ord_inc.withColumn("_src", F.lit(1)))
            i_parts.append(df_items_inc.withColumn("_src", F.lit(1)))
    o_all, i_all = union_all(o_parts), union_all(i_parts)

    cust_ok, q_cust = prepare_customers(df_cust_raw)
    prod_ok, q_prod = prepare_products(df_prod_raw)
    ord_ok, superseded, q_ord = prepare_orders(o_all, cust_ok)
    items_ok, q_items = prepare_items(i_all, ord_ok, prod_ok, all_orders=o_all, all_products=df_prod_raw)
    detail = build_sales_detail(items_ok, ord_ok, prod_ok, cust_ok).cache()
    q = union_all([q_cust, q_prod, q_ord, q_items]).cache()
    return dict(o_all=o_all, i_all=i_all, cust_ok=cust_ok, prod_ok=prod_ok, ord_ok=ord_ok.cache(),
                superseded=superseded, items_ok=items_ok, detail=detail, quarantine=q)


def aggregate(spark, detail):
    detail.createOrReplaceTempView("ventas_detalle")
    return spark.sql("""
        SELECT sales_month, country, channel, category,
               COUNT(DISTINCT order_id) AS total_orders,
               SUM(quantity)            AS total_units_sold,
               CAST(SUM(net_amount) AS DECIMAL(18, 2)) AS total_net_sales
        FROM ventas_detalle
        GROUP BY sales_month, country, channel, category
        ORDER BY sales_month, country, channel, category
    """)


def total_of(df, col):
    v = df.agg(F.sum(col)).collect()[0][0]
    return v if v is not None else Decimal("0.00")


def save_quarantine_partitions(q_df, superseded_df, base_path="output/quarantine"):
    os.makedirs(base_path, exist_ok=True)
    q_df.where("entity = 'customers' AND reason = 'duplicate_customer_id'") \
        .write.mode("overwrite").parquet(f"{base_path}/customers_duplicates.parquet")
    q_df.where("entity = 'products'") \
        .write.mode("overwrite").parquet(f"{base_path}/products_invalid_price.parquet")
    q_df.where("entity = 'orders' AND reason IN ('orphan_customer_id', 'missing_customer_id', 'missing_or_malformed_order_ts')") \
        .write.mode("overwrite").parquet(f"{base_path}/orders_orphan_customer.parquet")
    superseded_df.select(*COLS["orders"]) \
        .write.mode("overwrite").parquet(f"{base_path}/orders_duplicates.parquet")
    q_df.where("entity = 'order_items' AND reason IN ('invalid_quantity', 'invalid_discount')") \
        .write.mode("overwrite").parquet(f"{base_path}/items_invalid_values.parquet")
    q_df.where("entity = 'order_items' AND reason IN ('orphan_order', 'quarantine_order')") \
        .write.mode("overwrite").parquet(f"{base_path}/items_orphan_order.parquet")
    q_df.where("entity = 'order_items' AND reason = 'orphan_product'") \
        .write.mode("overwrite").parquet(f"{base_path}/items_orphan_product.parquet")
    q_df.where("entity = 'order_items' AND reason = 'quarantine_product'") \
        .write.mode("overwrite").parquet(f"{base_path}/items_quarantine_product.parquet")


def main():
    spark = get_spark("taller-data-engineer")
    print("Spark versión:", spark.version)
    os.makedirs("output/quarantine", exist_ok=True)

    print("\n" + "=" * 75)
    print("[PASO 1] LECTURA DE FUENTES BRUTAS")
    print("=" * 75)

    df_customers_raw = read_csv(spark, "customers").cache()
    df_products_raw = read_csv(spark, "products").cache()
    df_orders_raw = read_csv(spark, "orders").cache()
    df_order_items_raw = read_csv(spark, "order_items").cache()
    df_orders_inc_raw = read_jsonl(spark, "orders_incremental.jsonl", "orders").cache()
    df_items_inc_raw = read_jsonl(spark, "order_items_incremental.jsonl", "order_items").cache()

    total_cust_raw = df_customers_raw.count()
    total_prod_raw = df_products_raw.count()
    total_ord_raw = df_orders_raw.count()
    total_items_raw = df_order_items_raw.count()

    print(f"  * Clientes brutos leídos:     {total_cust_raw}")
    print(f"  * Productos brutos leídos:    {total_prod_raw}")
    print(f"  * Pedidos brutos leídos:      {total_ord_raw}")
    print(f"  * Líneas de detalle leídas:   {total_items_raw}")
    print(f"  * Incremental pedidos:        {df_orders_inc_raw.count()}")
    print(f"  * Incremental detalle:        {df_items_inc_raw.count()}")

    print("\n" + "=" * 75)
    print("[PASO 2] PERFILAMIENTO EXHAUSTIVO Y DIAGNÓSTICO DE CALIDAD")
    print("=" * 75)

    def profile_nulls(df, name, key):
        total = df.count()
        distinct_keys = df.where(F.col(key).isNotNull()).select(key).distinct().count()
        print(f"\n--- {name}: filas={total} | claves distintas={distinct_keys} | "
              f"filas con clave repetida o nula={total - distinct_keys} ---")
        print("Nulos por columna:")
        df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

    profile_nulls(df_customers_raw, "Clientes", "customer_id")
    profile_nulls(df_products_raw, "Productos", "product_id")
    profile_nulls(df_orders_raw, "Pedidos", "order_id")
    profile_nulls(df_order_items_raw, "Detalle Pedidos", "line_id")

    print("\n--- Perfil de Dominios Categóricos ---")
    print("Estados en Pedidos:")
    df_orders_raw.groupBy("status").count().show(truncate=False)
    print("Canales en Pedidos:")
    df_orders_raw.groupBy("channel").count().show(truncate=False)
    print("Países en Clientes:")
    df_customers_raw.groupBy("country").count().show(truncate=False)

    print("--- Valores mal formados (hay texto pero no es convertible) ---")
    print("  unit_price  :", df_products_raw.where(F.col("unit_price").isNotNull() & F.col("unit_price").try_cast("decimal(12,2)").isNull()).count())
    print("  quantity    :", df_order_items_raw.where(F.col("quantity").isNotNull() & F.col("quantity").try_cast("int").isNull()).count())
    print("  discount_pct:", df_order_items_raw.where(F.col("discount_pct").isNotNull() & F.col("discount_pct").try_cast("decimal(6,4)").isNull()).count())
    print("  order_ts    :", df_orders_raw.where(F.col("order_ts").isNotNull() & F.try_to_timestamp("order_ts").isNull()).count())
    print("  updated_at  :", df_orders_raw.where(F.col("updated_at").isNotNull() & F.try_to_timestamp("updated_at").isNull()).count())

    print("\n--- Rangos observados ---")
    df_order_items_raw.select(
        F.min(F.col("quantity").try_cast("int")).alias("qty_min"), F.max(F.col("quantity").try_cast("int")).alias("qty_max"),
        F.min(F.col("discount_pct").try_cast("decimal(6,4)")).alias("disc_min"), F.max(F.col("discount_pct").try_cast("decimal(6,4)")).alias("disc_max"),
    ).show()

    print("--- Referencias huérfanas (crudo) ---")
    print("  pedidos -> cliente inexistente :", df_orders_raw.where(F.col("customer_id").isNotNull()).join(df_customers_raw.select("customer_id").distinct(), "customer_id", "left_anti").count())
    print("  líneas  -> pedido inexistente  :", df_order_items_raw.where(F.col("order_id").isNotNull()).join(df_orders_raw.select("order_id").distinct(), "order_id", "left_anti").count())
    print("  líneas  -> producto inexistente:", df_order_items_raw.where(F.col("product_id").isNotNull()).join(df_products_raw.select("product_id").distinct(), "product_id", "left_anti").count())

    print("\n" + "=" * 75)
    print("[PASO 3] POLÍTICA DE CALIDAD Y ENRUTAMIENTO A CUARENTENA")
    print("=" * 75)

    base = run_pipeline(spark, df_customers_raw, df_products_raw, df_orders_raw, df_order_items_raw,
                        include_incremental=False)
    base["quarantine"].write.mode("overwrite").parquet("output/quarantine/base.parquet")
    save_quarantine_partitions(base["quarantine"], base["superseded"], "output/quarantine")

    print("  Cuarentena por entidad y motivo:")
    base["quarantine"].groupBy("entity", "reason").count().orderBy("entity", "reason").show(50, truncate=False)
    print(f"  Estados 'deliverd' corregidos a 'delivered': "
          f"{df_orders_raw.where(F.lower('status') == 'deliverd').count()}")
    print(f"  [Pedidos] Válidos: {base['ord_ok'].count()} | Versiones antiguas descartadas (auditadas): {base['superseded'].count()}")

    q_base = base["quarantine"]
    n_q = lambda e, extra="": q_base.where(f"entity = '{e}'" + extra).count()
    assert total_ord_raw == base["ord_ok"].count() + base["superseded"].count() + n_q("orders"), "Falla conservación de pedidos"
    assert total_items_raw == base["detail"].count() + n_q("order_items"), "Falla conservación de líneas"
    assert total_prod_raw == base["prod_ok"].count() + n_q("products"), "Falla conservación de productos"
    assert total_cust_raw == base["cust_ok"].count() + n_q("customers"), "Falla conservación de clientes"
    print("  [OK] Conservación de filas verificada en clientes, productos, pedidos y líneas.")

    print("\n" + "=" * 75)
    print("[PASO 4] JOINS SEGUROS Y CÁLCULO DE IMPORTE NETO")
    print("=" * 75)

    valid_lines_count = base["detail"].count()
    total_net_detail = total_of(base["detail"], "net_amount")
    print(f"  * Total líneas de detalle validadas: {valid_lines_count}")
    print(f"  * Importe neto total en detalle:     ${total_net_detail:,.2f}")

    print("\n" + "=" * 75)
    print("[PASO 5] AGREGACIÓN MENSUAL CON SPARK SQL")
    print("=" * 75)

    df_agg = aggregate(spark, base["detail"]).cache()
    total_net_agg = total_of(df_agg, "total_net_sales")
    reconciliation_diff = abs(total_net_detail - total_net_agg)

    print(f"  * Grupos agregados generados:        {df_agg.count()}")
    print(f"  * Importe neto total agregado:       ${total_net_agg:,.2f}")
    print(f"  * Diferencia de reconciliación:      ${reconciliation_diff:.4f}")
    df_agg.show(5, truncate=False)

    df_agg.write.mode("overwrite").parquet("output/ventas_mensuales.parquet")
    print("  * Salida Parquet guardada en:        output/ventas_mensuales.parquet")

    print("\n" + "=" * 75)
    print("[PASO 6] PROCESAMIENTO INCREMENTAL COMPLETO E IDEMPOTENCIA")
    print("=" * 75)

    final = run_pipeline(spark, df_customers_raw, df_products_raw, df_orders_raw, df_order_items_raw,
                         df_orders_inc_raw, df_items_inc_raw, include_incremental=True)
    final_x2 = run_pipeline(spark, df_customers_raw, df_products_raw, df_orders_raw, df_order_items_raw,
                            df_orders_inc_raw, df_items_inc_raw, include_incremental=True, repeat_incremental=2)

    count_orders_1 = final["ord_ok"].count()
    count_orders_2 = final_x2["ord_ok"].count()
    net_sales_1 = total_of(final["detail"], "net_amount")
    net_sales_2 = total_of(final_x2["detail"], "net_amount")

    cmp_cols = COLS["orders"]
    diff_a = final["ord_ok"].select(*cmp_cols).exceptAll(final_x2["ord_ok"].select(*cmp_cols)).count()
    diff_b = final_x2["ord_ok"].select(*cmp_cols).exceptAll(final["ord_ok"].select(*cmp_cols)).count()
    idempotency_passed = (count_orders_1 == count_orders_2) and (net_sales_1 == net_sales_2) and diff_a == 0 and diff_b == 0

    print(f"  [Idempotencia] Ejecución 1 (1x Inc) - Pedidos: {count_orders_1} | Ventas: ${net_sales_1:,.2f}")
    print(f"  [Idempotencia] Ejecución 2 (2x Inc) - Pedidos: {count_orders_2} | Ventas: ${net_sales_2:,.2f}")
    print(f"  [Idempotencia] Diferencias fila a fila (exceptAll): {diff_a} / {diff_b}")
    print(f"  [Idempotencia] ¿Resultado idéntico?: {'SÍ [PASÓ]' if idempotency_passed else 'NO [FALLÓ]'}")

    late_ids = df_orders_inc_raw.join(df_orders_raw.select("order_id").distinct(), "order_id", "left_semi") \
        .select("order_id").distinct()
    print(f"\n  [Precedencia] Pedidos históricos actualizados por el incremental: {late_ids.count()}")
    cols_show = ["order_id", "status", "channel", "updated_at"]
    print("  Versión histórica:")
    df_orders_raw.join(late_ids, "order_id", "left_semi").select(*cols_show).show(truncate=False)
    print("  Versión final (debe ser la más reciente):")
    final["ord_ok"].join(late_ids, "order_id", "left_semi").select(*cols_show).show(truncate=False)

    df_agg_final = aggregate(spark, final["detail"]).cache()
    df_agg_final.write.mode("overwrite").parquet("output/ventas_mensuales_consolidado.parquet")
    final["ord_ok"].select(*COLS["orders"]).write.mode("overwrite").parquet("output/orders_current.parquet")
    final["quarantine"].write.mode("overwrite").parquet("output/quarantine/consolidado.parquet")
    save_quarantine_partitions(final["quarantine"], final["superseded"], "output/quarantine")
    print("  * Salidas consolidadas y cuarentena guardadas exitosamente en output/")

    print("\n" + "=" * 75)
    print("EVIDENCIA DE CONTROLES DE CALIDAD INDEPENDIENTES (QA REPORT)")
    print("=" * 75)

    price_rows = final["prod_ok"].select("product_id", "_price").collect()
    price_map = F.create_map(*[x for r in price_rows
                               for x in (F.lit(r["product_id"]), F.lit(r["_price"]).cast("decimal(12,2)"))])
    independent_total = final["items_ok"].agg(F.sum(
        F.round(F.col("_qty") * price_map[F.col("product_id")] * (F.lit(1) - F.col("_disc")), 2).cast("decimal(18,2)")
    )).collect()[0][0]
    tot_detail = total_of(final["detail"], "net_amount")
    tot_agg = total_of(df_agg_final, "total_net_sales")
    qa1_passed = (tot_detail == tot_agg == independent_total)

    qf = final["quarantine"]
    raw_items = final["i_all"].count()
    n_items_q = qf.where("entity = 'order_items'").count()
    n_detail = final["detail"].count()
    raw_orders = final["o_all"].count()
    n_ord_q = qf.where("entity = 'orders'").count()
    n_ord_ok = final["ord_ok"].count()
    n_ord_sup = final["superseded"].count()
    qa2_passed = (raw_items == n_items_q + n_detail) and (raw_orders == n_ord_ok + n_ord_sup + n_ord_q)

    dup_lines = final["detail"].groupBy("line_id").count().where("count > 1").count()
    qa3_passed = (dup_lines == 0) and (n_detail == final["items_ok"].count())

    qa4_passed = idempotency_passed

    max_v = final["o_all"].withColumn("_v", F.coalesce(F.try_to_timestamp("updated_at"), F.try_to_timestamp("order_ts"))) \
        .groupBy("order_id").agg(F.max("_v").alias("max_v"))
    wrong = final["ord_ok"].join(max_v, "order_id").where(F.col("_version_ts") != F.col("max_v")).count()
    qa5_passed = (wrong == 0)

    ok = lambda b: "[PASÓ]" if b else "[FALLÓ]"
    print(f" 1. Reconciliación detalle = agregado = recálculo independiente : {ok(qa1_passed)}")
    print(f"    Detalle: ${tot_detail:,.2f} | Agregado: ${tot_agg:,.2f} | Independiente: ${independent_total:,.2f}")
    print(f" 2. Conservación de filas (Zero Silent Drops)                   : {ok(qa2_passed)}")
    print(f"    Líneas: {raw_items} = {n_detail} + {n_items_q} | Pedidos: {raw_orders} = {n_ord_ok} + {n_ord_sup} + {n_ord_q}")
    print(f" 3. Joins sin multiplicar filas y line_id único                 : {ok(qa3_passed)}")
    print(f"    line_id repetidos: {dup_lines}")
    print(f" 4. Idempotencia (lote reprocesado 2 veces)                     : {ok(qa4_passed)}")
    print(f" 5. Precedencia de la versión más reciente por order_id         : {ok(qa5_passed)}")
    print(f"    Pedidos con versión incorrecta: {wrong}")
    print("=" * 75)

    print("\nResumen de cuarentena (consolidado):")
    qf.groupBy("entity", "reason").count().orderBy("entity", "reason").show(50, truncate=False)

    print("\n" + "=" * 75)
    print("[PASO 8] INVENTARIO DE SALIDAS PARQUET GENERADAS")
    print("=" * 75)
    for root, dirs, files in sorted(os.walk("output")):
        parquets = [f for f in files if f.endswith(".parquet")]
        level = root.replace("output", "").count(os.sep)
        indent = " " * 4 * level
        folder = os.path.basename(root)
        if parquets or folder.endswith(".parquet"):
            size = sum(os.path.getsize(os.path.join(root, f)) for f in files)
            print(f"{indent}📁 {root} ({size:,} bytes)")

    for d in (df_customers_raw, df_products_raw, df_orders_raw, df_order_items_raw,
              df_orders_inc_raw, df_items_inc_raw, df_agg, df_agg_final):
        d.unpersist()

    spark.stop()
    print("\nSesión Spark finalizada y memoria liberada exitosamente.")


if __name__ == "__main__":
    main()
