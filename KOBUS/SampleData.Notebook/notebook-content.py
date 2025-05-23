# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7b8aa817-5545-4e50-b2f4-c5ef6e732cf5",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "36ddbf79-07fd-4594-b82d-dcf00fcefde6",
# META       "known_lakehouses": [
# META         {
# META           "id": "7b8aa817-5545-4e50-b2f4-c5ef6e732cf5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ----------------------------------------------------------
# 1) Imports – must be Fabric’s PySpark runtime
# ----------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from notebookutils import mssparkutils

# ----------------------------------------------------------
# 2) Enumerate all tables in the default Lakehouse
# ----------------------------------------------------------
tables = spark.catalog.listTables()

# ----------------------------------------------------------
# 3) Build the metadata + sample-data text
# ----------------------------------------------------------
lines = []
for t in tables:
    full_name = f"{t.database}.{t.name}"
    df = spark.table(full_name)

    lines.append(f"Table: {t.name}")
    lines.append("Columns:")
    for col_name, col_type in df.dtypes:
        lines.append(f"  - {col_name}: {col_type}")

    lines.append("Sample Data:")
    for i, row in enumerate(df.limit(5).collect(), start=1):
        lines.append(f"  Row {i}: {row.asDict()}")

    lines.append("")  # blank line between tables

text_output = "\n".join(lines)

# ----------------------------------------------------------
# 4) Write out to OneLake via the ABFSS path
# ----------------------------------------------------------
# Replace these GUIDs with yours (yours are already known):
workspace_id = "36ddbf79-07fd-4594-b82d-dcf00fcefde6"
lakehouse_id = "7b8aa817-5545-4e50-b2f4-c5ef6e732cf5"

abfss_path = (
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse_id}/Files/metadata/table_metadata.txt"
)

# Ensure the folder exists
mssparkutils.fs.mkdirs(
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse_id}/Files/metadata"
)

# Write (or overwrite) the file
mssparkutils.fs.put(abfss_path, text_output, overwrite=True)

print(f"✅ Metadata successfully written to:\n  {abfss_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for mp in mssparkutils.fs.mounts():
    if mp.mountPoint == "/lakehouse/default":
        print(mp.source)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
