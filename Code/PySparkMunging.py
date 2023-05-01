import pandas as pd
import json
import pyspark


def getFrame(path):
    return pd.read_csv(path)


def splitter(row):
    return row.split(',')


file_path = "/home/fneffati/DataSets/dcp2data/fbpac-ads-en-US.csv"

sc = pyspark.SparkContext("local[*]", "Test Context")
rdd = sc.textFile(file_path)

for row in rdd.take(5):
    print(row)


