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


file_path = "/home/fneffati/DataSets/dcp2data/fbpac-ads-en-US.csv"

sc = pyspark.SparkContext("local[*]", "Test Context")
rdd = sc.textFile(file_path)

split_rows = rdd.map(splitter)
raw_targets = split_rows.map(target_extractor)

for row in raw_targets.take(5):
    print(row[15])


