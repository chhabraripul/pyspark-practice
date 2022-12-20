from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, lag

spark_app_config = SparkConf().set("spark.app.name", "Group Aggregation Program").set("spark.master", "local[*]")
spark = SparkSession.builder.config(conf=spark_app_config).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

order_df = spark.read \
    .format("csv") \
    .option("inferSchema", True) \
    .option("path", "windowdata.csv") \
    .load().toDF("country", "weekno", "numinvoices", "totalqty", "invoicevalue")

my_window = Window.partitionBy("Country").orderBy("weekno")

order_df.withColumn("prevval", lag("invoicevalue").over(my_window)) \
    .withColumn("trend", when(col("prevval").isNull(), "NA")
                .when(col("prevval") > col("invoicevalue"), "High").otherwise("Low")).show()
