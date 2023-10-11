# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "b2bdec31-c5cc-42d2-b37a-456c0b710652",
          "fs.azure.account.oauth2.client.secret": "Ghc8Q~e5r5DDOFouNMEI2LQy0.ItspAPX-xJTdxg",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/dce87315-8ffa-4a01-ab40-0de5a7214b2f/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://raw-data@team4adlsacc.dfs.core.windows.net/",
  mount_point = "/mnt/adlsfiles",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

dbutils.fs.mounts()
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %sh ls dbfs:/mnt/

# COMMAND ----------

display(dbutils.fs.ls('/mnt/adlsfiles/raw'))

# COMMAND ----------

# MAGIC %fs ls /mnt/adlsfiles/raw

# COMMAND ----------

data_path = "/mnt/adlsfiles/raw/call_records.csv"
call_records_df = spark.read.csv(path=data_path, header=True, inferSchema=True)
call_records_df.display()

# COMMAND ----------

call_records_df.printSchema()

# COMMAND ----------

call_records_df.createOrReplaceTempView("call_records_temp")

# COMMAND ----------

usage_path = "/mnt/adlsfiles/raw/customer_usage_data.csv"
customer_usage_df = call_records_df = spark.read.csv(path=usage_path, header=True, inferSchema=True)
customer_usage_df.display()

# COMMAND ----------

customer_usage_df.printSchema()

# COMMAND ----------

customer_usage_df.createOrReplaceTempView("usage_df")

# COMMAND ----------

# -- Join customer_usage and call_records on CustomerID
# -- Calculate the total duration of calls for each customer
# -- Add the total duration of calls to the voice minutes in customer_usage
silver_df = spark.sql('''SELECT
    cu.CustomerID,
    cu.Date,
    cu.DataUsage,
    cu.VoiceMinutes + IFNULL(SUM(cr.Duration), 0) AS TotalVoiceMinutes,
    cu.TextMessages
FROM
    usage_df cu
LEFT JOIN
    call_records_temp cr
ON
    cu.CustomerID = cr.CustomerID
GROUP BY
    cu.CustomerID, cu.Date, cu.DataUsage, cu.VoiceMinutes, cu.TextMessages
''')

# COMMAND ----------

from pyspark.sql.functions import col, round, coalesce

# COMMAND ----------

silver_df = silver_df.withColumn( "PaymentAmount", round(col("DataUsage") * 0.1 + col("TotalVoiceMinutes") * 0.01 + col("TextMessages") * 0.1, 2) )

# COMMAND ----------

silver_df.display()

# COMMAND ----------



# COMMAND ----------

type(silver_df)

# COMMAND ----------

silver_df.write.parquet('/mnt/adlsfiles/silver/', mode='overwrite')

# COMMAND ----------

# MAGIC %fs ls /mnt/adlsfiles/silver

# COMMAND ----------

august_path = "/mnt/adlsfiles/raw/billing_history8.csv"
august_bill = spark.read.csv(path=august_path, header=True, inferSchema=True)
august_bill.display()

# COMMAND ----------

august_bill.createOrReplaceTempView("billing_aug")
silver_df.createOrReplaceTempView("billing_sept")

# COMMAND ----------

billing_september = spark.sql('''
    SELECT 
        bs.CustomerID,
        bs.Date,
        bs.DataUsage,
        bs.TotalVoiceMinutes,
        bs.TextMessages,
        ba.OutstandingBalance,
        bs.PaymentAmount
    FROM billing_sept bs
    LEFT JOIN billing_aug ba
    ON bs.CustomerID = ba.CustomerID
''')

# COMMAND ----------

billing_september.display()

# COMMAND ----------

billing_september = billing_september.withColumn("BillingAmount", round(col("OutstandingBalance") + col("PaymentAmount"), 2))

# COMMAND ----------

billing_september.display()

# COMMAND ----------

billing_september.write.parquet('/mnt/adlsfiles/gold/', mode='overwrite')

# COMMAND ----------

# MAGIC %fs ls /mnt/adlsfiles/gold/

# COMMAND ----------


