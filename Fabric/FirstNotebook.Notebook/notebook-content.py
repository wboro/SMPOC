# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ==== PARAMETERS YOU SET ====
server = "pocwb.privatelink.database.windows.net"   # e.g., "myserver.contoso.com" or "10.1.2.3"
port = 1433
database = "pocdb"

# Auth mode: choose ONE
auth_mode = "entra_passthrough"   # "sql" or "service_principal" (or "entra_passthrough" if applicable)

# SQL auth (if auth_mode == "sql")
sql_user = "YOUR_SQL_USERNAME"
sql_password = "YOUR_SQL_PASSWORD"  # consider pulling from Key Vault (see Option B)

# Service principal auth (if auth_mode == "service_principal")
tenant_id = "2ee4898e-0173-44a4-92bf-fca5f81a39ee"
client_id = "737d4175-c615-411b-add4-b3256fc2af1f"
client_secret = "PnJ8Q~9nycTXAuCNeI2XYMADBNvaSONmC1-v3dq4"  # consider pulling from Key Vault (see Option B)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F

jdbc_url = f"jdbc:sqlserver://{server}:{port};database={database};encrypt=true;trustServerCertificate=false;loginTimeout=30;"

# INFORMATION_SCHEMA.TABLES is widely supported in SQL Server
tables_query = """
(
  SELECT
    TABLE_CATALOG AS [database],
    TABLE_SCHEMA  AS [schema],
    TABLE_NAME    AS [table],
    TABLE_TYPE    AS [type]
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_TYPE IN ('BASE TABLE','VIEW')
) AS t
"""

reader = (spark.read
          .format("com.microsoft.sqlserver.jdbc.spark"))  # Spark connector for SQL databases [1](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-sql-connector)

if auth_mode == "sql":
    df_tables = (reader
        .option("url", jdbc_url)
        .option("dbtable", tables_query)
        .option("user", sql_user)
        .option("password", sql_password)
        .load()
    )
elif auth_mode == "service_principal":
    # The connector supports service principal via access token / Entra-based flows. [1](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-sql-connector)
    # If your environment already provides a token, you can pass it via 'accessToken'.
    # In many org setups, you'd acquire a token separately and set it here:
    access_token = "<ACCESS_TOKEN_FOR_SQL_RESOURCE>"
    df_tables = (reader
        .option("url", jdbc_url)
        .option("dbtable", tables_query)
        .option("accessToken", access_token)
        .load()
    )
else:
    # Entra passthrough works when Entra auth is enabled/configured on the SQL engine and Fabric can pass credentials. [1](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-sql-connector)
    df_tables = (reader
        .option("url", jdbc_url)
        .option("dbtable", tables_query)
        .load()
    )

df_tables = (df_tables
             .withColumn("full_name", F.concat_ws(".", F.col("schema"), F.col("table")))
             .orderBy("schema", "table"))

display(df_tables.select("database","schema","table","type","full_name"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
