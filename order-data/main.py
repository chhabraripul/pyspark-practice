from pyspark import SparkContext

sc = SparkContext("local[*]", "Order Data Processing")
input_file = sc.textFile("customerorders.csv")
sc.setLogLevel("ERROR")

customer_order_price = input_file.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))
top_shopper = customer_order_price.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False).take(1)
print(top_shopper)
