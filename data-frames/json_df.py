from pyspark import SparkConf
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark_app_config = SparkConf().set("spark.app.name", "DataFrame first program").set("spark.master", "local[*]")
spark = SparkSession.builder.config(conf=spark_app_config).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


json_schema = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("name", StringType()),
    StructField("image", StructType([
        StructField("url", StringType()),
        StructField("width", IntegerType()),
        StructField("height", IntegerType())
    ])),
    StructField("thumbnail", StructType([
        StructField("url", StringType()),
        StructField("width", IntegerType()),
        StructField("height", IntegerType())
    ]))
])
json_df = spark.read.\
    format("json")\
    .option("path", "json_data.json")\
    .option("multiline", True)\
    .load()
json_df.printSchema()

json_df.selectExpr("thumbnail.url as URL").show(truncate=False)