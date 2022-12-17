from pyspark import SparkContext


sc = SparkContext("local[*]", "cache and persist usage")
ratings_rdd = sc.textFile("ratings.dat")
paired_ratings = ratings_rdd.map(lambda x: (x.split("::")[1], (float(x.split("::")[2]), 1.0)))

total_ratings_by_movie = paired_ratings.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
    .filter(lambda x: x[1][1] > 100).mapValues(lambda x: x[0] / x[1]).filter(lambda x: x[1] > 4.5)

movies_rdd = sc.textFile("movies.dat")
movies_pair_rdd = movies_rdd.map(lambda x: (x.split("::")[0], x.split("::")[1]))

joined_ratings_movies = total_ratings_by_movie.join(movies_pair_rdd).map(lambda x: x[1][1])
result = joined_ratings_movies.collect()
for movie in result:
    print(movie)


