import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import re


def quoter(row):
    """
    Takes a JSON row and will replace the occurrences of double-double quotes ("") with a single double quote (")
    :param row: A cell from the Targets Column
    :return: a valid JSON format String
    """
    return re.sub('""', '"', row)


def col_name_extractor(row):
    row = json.loads(row)
    result = []
    for item in row:
        if " " not in item["target"]:
            result.append("target" + "_" + item["target"])
    return result


def value_extractor(row):
    row = json.loads(row)
    result = []
    for item in row:
        result.append(item["segment"])
    return result


file_path = "/home/fneffati/DataSets/propublica_1000.csv"

spark = SparkSession.builder \
    .master('local[1]') \
    .getOrCreate()
df = spark.read.csv(file_path, header=True, inferSchema=True, multiLine=True, sep=',', escape='"',
                    ignoreLeadingWhiteSpace=True)

df.select("targets").show(5, truncate=False)
rdd = df.select("targets").rdd
rdd2 = rdd.map(lambda x: x[0])
rdd3 = rdd2.map(quoter)

cols = rdd3.map(col_name_extractor)
print(cols.take(5))

flattened_rdd = cols.flatMap(lambda x: x)

# Convert the flattened RDD into a set
unique_set = set(flattened_rdd.collect())
unique_set = list(unique_set)
print(unique_set)

for col_name in unique_set:
    df = df.withColumn(col_name, lit(" "))

df.show(5)

