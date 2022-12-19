from pyspark import SparkContext, StorageLevel
from sys import stdin

sc = SparkContext("local[*]", "cache and persist usage")

order_data = sc.textFile("customerorders.csv")

customer_cost = order_data.map(lambda x: (x.split(",")[0], float(x.split(",")[2]))).reduceByKey(lambda x, y: x + y)
premium_customer = customer_cost.filter(lambda x: x[1] > 5000)

doubled_amount = premium_customer.mapValues(lambda x: x * 2).persist(StorageLevel.MEMORY_ONLY)
print(doubled_amount.collect())
print(doubled_amount.count())