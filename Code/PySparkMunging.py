import pandas as pd
import json
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master('local[1]') \
                    .getOrCreate()

def getFrame(path):
    return pd.read_csv(path)


def splitter(row):
    return row.split(',')


def target_extractor(split_row):
    return split_row[15]


file_path = "/home/fneffati/DataSets/propublica_1000.csv"

sc = pyspark.SparkContext("local[*]", "Test Context")
rdd = spark.read.csv(file_path, header=True, inferSchema=True, multiLine=True, sep=',', escape='"',
                  ignoreLeadingWhiteSpace=True)

split_rows = rdd.map(splitter)
raw_targets = split_rows.map(target_extractor)

# View the dataframe
for row in raw_targets.take(5):
    print(row)
