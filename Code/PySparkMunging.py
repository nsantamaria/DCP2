import pandas as pd
import json
from pyspark.sql import SparkSession
import re


def quoter(row):
    """
    Takes a JSON row and will replace the occurrences of double-double quotes ("") with a single double quote (")
    :param row: A cell from the Targets Column
    :return: a valid JSON format String
    """
    return re.sub('""', '"', row)


def json_converter(row):
    return json.loads(row)


def col_name_extractor(row):
    return "target" + "_" + row["target"]


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

jsoned_set = rdd3.map(json_converter)
print(jsoned_set.take(5))

cols = jsoned_set.map(col_name_extractor)
print(cols.take(5))

