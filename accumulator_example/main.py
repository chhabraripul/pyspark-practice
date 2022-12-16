from pyspark import SparkContext


def is_blank_line(line):
    if len(line) == 0:
        space_counter.add(1)


sc = SparkContext("local[*]", "Accumulator Example")
input_file = sc.textFile("sample.txt")

# create an accumulator to count the number of spaces in the file
space_counter = sc.accumulator(0)

input_file.foreach(is_blank_line)

print(space_counter)

