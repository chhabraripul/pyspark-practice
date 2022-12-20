from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_app_config = SparkConf().set("spark.app.name", "Simple Aggregation Program").set("spark.master", "local[*]")
spark = SparkSession.builder.config(conf=spark_app_config).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

order_df = spark.read\
    .format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .option("path", "order_data.csv")\
    .load()

# count all rows,  avg unit price , distinct invoice number, quantity

# column object expression
order_df.select(
    count("*").alias("total_rows"),
    avg("UnitPrice").alias("avg_unit_price"),
    sum("Quantity").alias("total_qty"),
    countDistinct("InvoiceNo").alias("distinct_invoice_count")
).show(truncate=False)

# column string expression
order_df.select(
    expr("count(*) as total_rows"),
    expr("avg(UnitPrice) as avg_unit_price"),
    expr("sum(Quantity) as total_qty"),
    expr("count(distinct(InvoiceNo)) as distinct_invoice_count"),
).show(truncate=False)

# spark sql style
order_df.createOrReplaceTempView("orders")

spark.sql("select count(*) as total_rows, "
          "avg(UnitPrice) as avg_unit_price, "
          "sum(Quantity) as total_qty, "
          "count(distinct(InvoiceNo)) as distinct_invoice_count "
          "from orders").show(truncate=False)

spark.stop()
