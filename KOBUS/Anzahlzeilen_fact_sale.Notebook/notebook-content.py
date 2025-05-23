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

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Wenn du die Tabelle als DataFrame laden möchtest
fact_sale_df = spark.read.table("fact_sale")

# Zeilen zählen
row_count = fact_sale_df.count()

# Ausgabe
print(f"Anzahl der Zeilen in fact_sale: {row_count}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
