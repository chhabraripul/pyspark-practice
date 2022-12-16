from pyspark import SparkContext


def parse_line(line):
    data = line.split("::")
    age = int(data[2])
    num_of_friends = int(data[3])
    return age, (num_of_friends, 1)


sc = SparkContext("local[*]", "average friends by age")
input_file = sc.textFile("friendsdata.csv")

rdd1 = input_file.map(parse_line)

rdd2 = rdd1.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: x[0] / x[1])

print(rdd2.collect())
