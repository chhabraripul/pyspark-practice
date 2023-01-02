from pyspark.streaming import *
from pyspark import *


def summary_func(x, y):
    return x + y


def inverse_func(x, y):
    return x - y


sc = SparkContext("local[2]", "stateful all rdd sum streaming program")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)
ssc.checkpoint(".")
lines = ssc.socketTextStream("localhost", 8877)
words = lines.flatMap(lambda x: x.split())
pairs = words.map(lambda x: (x, 1))
word_count = pairs.reduceByKeyAndWindow(summary_func, inverse_func, 10, 2) \
    .filter(lambda x: x[1] > 0)
word_count.pprint()
ssc.start()
ssc.awaitTermination()
