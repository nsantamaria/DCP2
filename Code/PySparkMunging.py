import pandas as pd
import json
import pyspark


def getFrame(path):
    return pd.read_csv(path)


def splitter(row):
    return row.split(',')


def target_extractor(split_row):
    if split_row[15] != "[]":
        return split_row[15]
    else:
        return "empty"


file_path = "/home/fneffati/DataSets/propublica_1000.csv"

"""sc = pyspark.SparkContext("local[*]", "Test Context")
rdd = sc.textFile(file_path)"""

ss = pyspark.sq1.SparkSession.builder.getOrCreate()
rdd = ss.read.csv(file_path)

split_rows = rdd.map(splitter)
raw_targets = split_rows.map(target_extractor)

for row in raw_targets.take(10):
    print(row)


