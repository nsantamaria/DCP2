import pandas as pd
import json
import pyspark
# Create a spark session
from pyspark.sql import SparkSession

def getFrame(path):
    return pd.read_csv(path)

def splitter(row):
    return row.split(',')

def target_extractor(split_row):
    return split_row[15]


file_path = "/home/fneffati/DataSets/propublica_1000.csv"

"""sc = pyspark.SparkContext("local[*]", "Test Context")
rdd = sc.textFile(file_path)


split_rows = rdd.map(splitter)
raw_targets = split_rows.map(target_extractor)"""

spark = SparkSession.builder.appName('SparkExamples').getOrCreate()
df = spark.read.csv(file_path)

split_rows = df.map(splitter)
raw_targets = split_rows.map(target_extractor)

# View the dataframe
for row in raw_targets.take(5):
    print(row)

