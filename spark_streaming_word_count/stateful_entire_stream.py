from pyspark.streaming import *
from pyspark import *


def update_function(new_values, prev_state):
    if prev_state is None:
        prev_state = 0
    return sum(new_values, prev_state)


sc = SparkContext("local[2]", "stateful all rdd sum streaming program")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)
ssc.checkpoint(".")
lines = ssc.socketTextStream("localhost", 8877)
words = lines.flatMap(lambda x: x.split())
pairs = words.map(lambda x: (x, 1))
# word_count = pairs.reduceByKey(lambda x, y: x + y)
word_count = pairs.updateStateByKey(update_function)
word_count.pprint()
ssc.start()
ssc.awaitTermination()
