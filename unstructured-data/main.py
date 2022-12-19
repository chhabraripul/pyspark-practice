from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import IntegerType, TimestampType

conf = SparkConf()
conf.set("spark.app.name", "Handle unstructured data when no format is given")
conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

reg_exp = r'(\S+) (\S+)\t(\S+),(\S+)'
raw_lines = spark.read.text("orders_new.csv")

order_df = raw_lines.select(regexp_extract("value", reg_exp, 1).cast(IntegerType()).alias("order_id"),
                            regexp_extract("value", reg_exp, 2).cast(TimestampType()).alias("date"),
                            regexp_extract("value", reg_exp, 3).cast(IntegerType()).alias("customer_id"),
                            regexp_extract("value", reg_exp, 4).alias("status"))

# order_df.printSchema()

order_df.createOrReplaceTempView("orders")

spark.sql("select status, count(*) as total from orders group by status order by total desc").show()
