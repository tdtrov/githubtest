# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ----------------------------------------------------------
# 1) Imports – PySpark (Fabric runtime)
# ----------------------------------------------------------
from pyspark.sql import SparkSession
from notebookutils import mssparkutils

spark = SparkSession.builder.getOrCreate()

# ----------------------------------------------------------
# 2) Configuration – your workspace + lakehouse IDs
# ----------------------------------------------------------
workspace_id  = "36ddbf79-07fd-4594-b82d-dcf00fcefde6"
lakehouse_ids = [
    "7b8aa817-5545-4e50-b2f4-c5ef6e732cf5",
    "7148fc4d-89ff-4f63-9c64-baf035eb8811",
    "1927a70a-f7fe-4248-9bc1-580f8ececd67",
]

# ----------------------------------------------------------
# 3) Helper functions
# ----------------------------------------------------------
def list_dirs(path):
    try:
        return [info.name for info in mssparkutils.fs.ls(path) if info.isDir]
    except:
        return []

def has_delta_log(path):
    try:
        return any(info.name == "_delta_log" for info in mssparkutils.fs.ls(path))
    except:
        return False

# ----------------------------------------------------------
# 4) Main loop – build and PRINT metadata inline
# ----------------------------------------------------------
for lh_id in lakehouse_ids:
    base_uri    = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_id}"
    tables_root = f"{base_uri}/Tables"

    # 4a) Start the text blob
    lines = [f"Lakehouse ID: {lh_id}", "=" * 40, ""]

    # 4b) Inspect top-level tables vs. schemas
    for entry in list_dirs(tables_root):
        entry_path = f"{tables_root}/{entry}"

        # Flat Delta table?
        if has_delta_log(entry_path):
            df = spark.read.format("delta").load(entry_path)
            lines.append(f"Table: {entry}")
        else:
            # Treat as schema folder
            schema = entry
            schema_path = entry_path
            lines.append(f"Schema: {schema}")
            lines.append("-" * (8 + len(schema)))
            for tbl in list_dirs(schema_path):
                if tbl == "_delta_log":
                    continue
                tbl_path = f"{schema_path}/{tbl}"
                if has_delta_log(tbl_path):
                    df = spark.read.format("delta").load(tbl_path)
                else:
                    # infer format from first file
                    file_info = next((f for f in mssparkutils.fs.ls(tbl_path) if not f.isDir), None)
                    ext = file_info.name.split(".")[-1].lower() if file_info else ""
                    if ext == "parquet":
                        df = spark.read.parquet(tbl_path)
                    elif ext == "csv":
                        df = spark.read.option("header", True).csv(tbl_path)
                    elif ext == "json":
                        df = spark.read.json(tbl_path)
                    else:
                        df = spark.read.load(tbl_path)
                lines.append(f"Table: {tbl}")

        # common: columns + sample
        lines.append("Columns:")
        for col, typ in df.dtypes:
            lines.append(f"  - {col}: {typ}")
        lines.append("Sample Data:")
        for i, row in enumerate(df.limit(5).collect(), start=1):
            lines.append(f"  Row {i}: {row.asDict()}")
        lines.append("")  # blank line

    # 4c) Join and print it inline
    text_output = "\n".join(lines)
    print(text_output)
    print("\n" + ("#"*80) + "\n")   # separator between lakehouses


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
