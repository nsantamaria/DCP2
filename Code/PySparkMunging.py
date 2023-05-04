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
        try:
            result.append(item["segment"])
        except:
            continue
    return result


def flattner(rdd):
    """

    :param rdd: the RDD Of either the Columns or the Values extracted from the targets column
    :return: A flat list of those values
    """
    flattened_rdd = rdd.flatMap(lambda x: x)
    unique_set = set(flattened_rdd.collect())

    return list(unique_set)


def json_ready(df):
    """
    Takes the Targets Columns and returns in
    :param df: The Dataframe of the targets column
    :return: a cleaned up and JSON processing ready PySpark RDD of that column
    """
    rdd = df.select("targets").rdd
    rdd2 = rdd.map(lambda x: x[0])
    rdd3 = rdd2.map(quoter)

    return rdd3


file_path = "/home/fneffati/DataSets/propublica_1000.csv"

spark = SparkSession.builder \
    .master('local[1]') \
    .getOrCreate()
df = spark.read.csv(file_path, header=True, inferSchema=True, multiLine=True, sep=',', escape='"',
                    ignoreLeadingWhiteSpace=True)
df.select("targets").show(5, truncate=False)

jsoned_rdd = json_ready(df)

cols = jsoned_rdd.map(col_name_extractor)
cols_unique_list = flattner(cols)
print(cols_unique_list)

vals = jsoned_rdd.map(value_extractor)
vals_unique_list = flattner(vals)
print(vals_unique_list)
