import pandas as pd
import json
import pyspark


def getFrame(path):
    return pd.read_csv(path)


url = "/home/fneffati/DataSets/dcp2data/fbpac-ads-en-US.csv"
dataframe = getFrame(url)
sc = pyspark.SparkContext("local[*]", "Test Context")
rdd = sc.textFile(dataframe)

for row in rdd.take(5):
    print(row)


