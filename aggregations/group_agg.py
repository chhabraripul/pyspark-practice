from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_app_config = SparkConf().set("spark.app.name", "Group Aggregation Program").set("spark.master", "local[*]")
spark = SparkSession.builder.config(conf=spark_app_config).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

order_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "order_data.csv") \
    .load()

# calculate total quantity and total value of each invoice country-wise and sort it by country and invoice value

# column object style
order_df.groupby("Country", "InvoiceNo") \
    .agg(
    sum("Quantity").alias("total_qty"),
    sum(expr("Quantity * UnitPrice")).alias("invoice_value")) \
    .orderBy([col("Country").asc(), col("invoice_value").desc()]) \
    .show(truncate=False)

# column string style
order_df.groupby("Country", "InvoiceNo") \
    .agg(
    expr("sum(Quantity) as total_qty"),
    expr("sum(Quantity * UnitPrice) as invoice_value")) \
    .orderBy([asc("Country"), desc("invoice_value")]) \
    .show(truncate=False)

# spark sql style
order_df.createOrReplaceTempView("orders")

spark.sql("select Country, InvoiceNo, sum(Quantity) as total_qty, "
          "sum(Quantity * UnitPrice) as invoice_value from orders group by Country, InvoiceNo "
          "order by Country asc,invoice_value desc") \
    .show(truncate=False)
