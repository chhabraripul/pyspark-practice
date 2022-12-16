from pyspark import SparkContext

sc = SparkContext("local[*]", "wordcount")
sc.setLogLevel("ERROR")
input_file = sc.textFile("./data.txt")
words = input_file.flatMap(lambda x: x.split(" "))
words_count = words.map(lambda x: (x, 1))
final_count = words_count.reduceByKey(lambda x, y: x + y) \
                .map(lambda x: (x[1], x[0])) \
                .sortByKey(False) \
                .map(lambda x: (x[1], x[0]))

result = final_count.collect()
for word in result:
    print(word)
