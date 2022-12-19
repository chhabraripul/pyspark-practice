from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

configuration = SparkConf() \
    .set("spark.app.name", "word count problem using dataframe") \
    .set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=configuration).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark.read.text("data.txt").toDF("lines")
# words_df = lines.withColumn("words", explode(split("lines", " "))).drop("lines")
# words_df.groupby("words").count().orderBy(col("count").desc()).show()

lines.createOrReplaceTempView("lines")

word_df = spark.sql("select words , count(*) as count from (select explode(split(lines, ' ')) as words from lines) t "
                    "group by words order by count desc").show()

