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
    return [row[0], re.sub('""', '"', row[1])]


def col_name_extractor(row):
    row1 = json.loads(row[1])
    result = []
    for item in row1:
        if " " not in item["target"]:
            result.append("target" + "_" + item["target"])
    return [row[0], result]


def value_extractor(row):
    row1 = json.loads(row[1])
    result = []
    for item in row1:
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
    flattened_rdd = rdd.flatMap(lambda x: x[1])
    unique_set = set(flattened_rdd.collect())

    return list(unique_set)


def json_ready(df):
    """
    Takes the Targets Columns and returns in
    :param df: The Dataframe of the targets column
    :return: a cleaned up and JSON processing ready PySpark RDD of that column
    """
    rdd = df.select("targets").rdd
    rdd2 = rdd.zipWithIndex().map(lambda x: (x[1], x[0][0]))
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

# print(columns_list)
# print(values_list)

# Convert the DataFrame to an RDD
rdd = df.rdd

modified_rdd = rdd
# TODO: Turn this bit into MAP function
for index1, col_group in enumerate(columns_list):
    column_index = index1
    print(col_group, column_index)
    for index2, col_name in enumerate(col_group):
        row_index = index2
        print(col_name, row_index)
        # df.select(col_name)[row_index] = values_list[column_index][row_index]

        # Modify a specific cell in the RDD
        modified_rdd = rdd.zipWithIndex().map(lambda row: (row.col_name, values_list[column_index][row_index])if row[0] == row_index)


        # Convert the modified RDD back to a DataFrame
updated_df = modified_rdd.toDF(["Name", "Age"])

# Show the result DataFrame
updated_df.show()

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

Now that I have a 
"""