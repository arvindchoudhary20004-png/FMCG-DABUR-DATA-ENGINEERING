# Databricks notebook source
for cat in ["dabur_fmcg", "workspace"]:
    for schema in ["raw", "bronze", "silver", "gold", "default"]:
        try:
            vols = spark.sql(f"SHOW VOLUMES IN {cat}.{schema}").collect()
            for v in vols:
                path = f"/Volumes/{cat}/{schema}/{v['volume_name']}"
                try:
                    files = [f.name for f in dbutils.fs.ls(path)]
                    print(path, "->", files)
                except Exception as e:
                    print(path, "-> cannot list")
        except Exception:
            pass

# COMMAND ----------

CATALOG = "workspace"
base = f"/Volumes/{CATALOG}/raw/landing"

# COMMAND ----------

to_bronze("products.csv", "products_raw")

# COMMAND ----------

base = "/Volumes/dabur_fmcg/raw/landing/sales_data"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'products' t, COUNT(*) c FROM workspace.bronze.products_raw
# MAGIC UNION ALL SELECT 'distributors', COUNT(*) FROM workspace.bronze.distributors_raw
# MAGIC UNION ALL SELECT 'sales', COUNT(*) FROM workspace.bronze.sales_raw
# MAGIC UNION ALL SELECT 'inventory', COUNT(*) FROM workspace.bronze.inventory_raw;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

CATALOG = "workspace"     # use "dabur_fmcg" only if your files are there
base = f"/Volumes/{CATALOG}/raw/landing"

def to_bronze(file_name, table_name):
    df = (spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(f"{base}/{file_name}")
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path")))
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", True)
       .saveAsTable(f"{CATALOG}.bronze.{table_name}"))
    print(f"{table_name}: {df.count()} rows")

to_bronze("products.csv",     "products_raw")
to_bronze("distributors.csv", "distributors_raw")
to_bronze("sales.csv",        "sales_raw")
to_bronze("inventory.csv",    "inventory_raw")

# COMMAND ----------

