from pyspark.streaming import *
from pyspark import *

sc = SparkContext("local[2]", "first streaming program")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)
lines = ssc.socketTextStream("localhost", 8877)
words = lines.flatMap(lambda x: x.split())
pairs = words.map(lambda x: (x, 1))
word_count = pairs.reduceByKey(lambda x, y: x + y)
word_count.pprint()
ssc.start()
ssc.awaitTermination()

