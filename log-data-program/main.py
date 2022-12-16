from pyspark import SparkContext

sc = SparkContext("local[*]", "Log Data count Job")
sc.setLogLevel("ERROR")

if __name__ != "__main__":
    sample = [
        "ERROR: Thu Jun 04 10:37:51 BST 2015",
        "ERROR: Thu Jun 04 10:37:51 BST 2015",
        "INFO: Thu Jun 04 10:37:51 BST 2015",
        "ERROR: Thu Jun 04 10:37:51 BST 2015",
        "ERROR: Thu Jun 04 10:37:51 BST 2015",
        "ERROR: Thu Jun 04 10:37:51 BST 2015"
    ]
    input_logs = sc.parallelize(sample)
else:
    input_logs = sc.textFile("log.txt")
    print(input_logs.getNumPartitions())
    newRDD = input_logs.coalesce(20)
    print(newRDD.getNumPartitions())

# process the logs
log_level_rdd = input_logs.map(lambda x: (x.split(":")[0], 1)).reduceByKey(lambda x, y: x + y)

