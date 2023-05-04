import pandas as pd
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def getFrame(path):
    return pd.read_csv(path)


def splitter(row):
    return row.split(',')


def target_extractor(split_row):
    return split_row[15]


file_path = "/home/fneffati/DataSets/propublica_1000.csv"

spark = SparkSession.builder \
    .master('local[1]') \
    .getOrCreate()
df = spark.read.csv(file_path, header=True, inferSchema=True, multiLine=True, sep=',', escape='"',
                    ignoreLeadingWhiteSpace=True)

df.select("targets").show(5)
