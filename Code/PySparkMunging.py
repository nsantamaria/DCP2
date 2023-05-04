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

cols = jsoned_rdd.map(col_name_extractor)      # 2D RDD of the columns per cell
flat_cols_unique_list = flattner(cols)         # list of unique column names
print(flat_cols_unique_list)

# Add the unique columns as empty columns to the Dataframe
for col_name in flat_cols_unique_list:
    df = df.withColumn(col_name, lit(" "))


vals = jsoned_rdd.map(value_extractor)         # 2D RDD of the values per cell
print(vals.take(5))
print("\n")
columns_list = cols.collect()
values_list = vals.collect()

print(columns_list)
print(values_list)
print("\n")

for i, row in enumerate(df.collect()):
    # Get the row values
    row_values = list(row)

    # Get the corresponding columns and values
    row_columns = columns_list[i]
    row_values_to_insert = values_list[i]

    # Populate the columns with values
    for col_name, col_value in zip(row_columns, row_values_to_insert):
        df = df.withColumn(col_name, col_value)

# Show the updated DataFrame
df.show()

"""
I don't need to flatten them 
All I need to do is get the list of unique column names 
Then Add them all as empty columns to the Dataframe 


Once i DO  THAT 



I need to also get another list of all the column names in a 2d array format, 
so that every index of that array is actually what every cell holds


Once I do that ,



I get the 2d array of the values 
so that every index of that array is actually what every cell holds

And then I iterate using both 
"""