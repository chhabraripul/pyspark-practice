from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

spark_app_config = SparkConf().set("spark.app.name", "DataFrame first program").set("spark.master", "local[*]")
spark = SparkSession.builder.config(conf=spark_app_config).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# Schema can be defined in 2 ways explicitly
# 1) Struct
orders_schema = StructType([
    StructField("orderid", IntegerType()),
    StructField("date", TimestampType()),
    StructField("customer_id", IntegerType()),
    StructField("status", StringType())
])
# 2) DDL string
orders_schema_ddl_str = "order_id Integer, order_date Timestamp, order_customer_id Integer, status String"

order_df = spark.read \
    .schema(orders_schema_ddl_str) \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "orders.csv") \
    .format("csv").load()

result_df = order_df\
    .where("order_customer_id > 10000")\
    .select("order_id", "order_customer_id") \
    .groupby("order_customer_id") \
    .count()

result_df.write.format("json").option("path", "orderoutput").mode("overwrite") \
    .partitionBy("count").save()

# Write MODES
# overwrite
# append
# ignore
# failIfExists
