from pyspark import SparkContext

sc = SparkContext("local[*]", "movie data ratings")
sc.setLogLevel("ERROR")
input_file = sc.textFile("moviedata.data")
ratings = input_file.map(lambda x: (x.split("\t")[2], 1))
ratings_count = ratings.reduceByKey(lambda x, y: x + y).sortByKey()
print(ratings_count.collect())
