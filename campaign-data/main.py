from pyspark import SparkContext


def get_name_cost(data):
    columns = data.split(",")
    name = columns[0]
    cost = float(columns[10])
    return cost, name


def load_boring_words_set():
    with open("boringwords.txt") as boring_word_file:
        return set(word.strip() for word in boring_word_file.readlines())


sc = SparkContext("local[*]", "campaign data analysis")
boring_word_bv = sc.broadcast(load_boring_words_set())
input_file = sc.textFile("campaigndata.csv")
name_cost_rdd = input_file.map(get_name_cost)\
                            .flatMapValues(lambda x: x.split(" "))\
                            .map(lambda x: (x[1], x[0]))\
                            .filter(lambda x: x[0] not in boring_word_bv.value)

word_total_cost_rdd = name_cost_rdd.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)

print(word_total_cost_rdd.collect())
