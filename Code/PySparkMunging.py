import pandas as pd
import json
from pyspark.sql import SparkSession


def json_converter(row):
    return json.loads(row)


file_path = "/home/fneffati/DataSets/propublica_1000.csv"

spark = SparkSession.builder \
    .master('local[1]') \
    .getOrCreate()
df = spark.read.csv(file_path, header=True, inferSchema=True, multiLine=True, sep=',', escape='"',
                    ignoreLeadingWhiteSpace=True)

df.select("targets").show(5, truncate=False)
rdd = df.select("targets").rdd
rdd2= rdd.map(lambda x: x[0])
jsoned_set = rdd2.map(json_converter)
print(jsoned_set.take(5))
