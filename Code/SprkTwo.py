import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
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
            result.append(" ")
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
jsoned_rdd = json_ready(df)

cols = jsoned_rdd.map(col_name_extractor)  # 2D RDD of the columns per cell
flat_cols_unique_list = flattner(cols)  # list of unique column names
print(flat_cols_unique_list)

# Add the unique columns as empty columns to the Dataframe
for col_name in flat_cols_unique_list:
    df = df.withColumn(col_name, lit(" "))


def new_quoter(row):
    """
    Takes a JSON row and will replace the occurrences of double-double quotes ("") with a single double quote (")
    :param row: A cell from the Targets Column
    :return: a valid JSON format String
    """
    row = row[15]
    return re.sub('""', '"', row)


def extract_target(row, target_name):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = ""
    for target in targets:
        try:
            if target["target"] == target_name:
                result = target["segment"]
        except:
            pass
    r = row
    updated_row = Row(id=r.id, html=r.html, political=r.political, not_political=r.not_political, title=r.title,
                      message=r.message, thumbnail=r.thumbnail, created_at=r.created_at, updated_at=r.updated_at,
                      lang=r.lang, images=r.images, impressions=r.impressions,
                      political_probability=r.political_probability, targeting=r.targeting, suppressed=r.suppressed,
                      targets=r.targets, advertiser=r.advertiser, entities=r.entities, page=r.page,
                      lower_page=r.lower_page, targetings=r.targetings, paid_for_by=r.paid_for_by,
                      targetedness=r.targetedness, listbuilding_fundraising_proba=r.listbuilding_fundraising_proba,
                      target_Interest=r.target_Interest, target_Region=r.target_Region, target_City=r.target_City,
                      target_Language=r.target_Language, target_MaxAge=r.target_MaxAge, target_Age=r.target_Age,
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting,
                      target_Gender=r.target_Gender,
                      target_Like=r.target_Like, target_List=r.target_List, target_Segment=r.target_Segment)
    updated_row[target_name] = result
    return updated_row


# Example usage: extract target_Interest
with_Website = df.rdd.map(lambda row: extract_target(row, "Website"))

# Example usage: extract target_Interest
with_Agency = with_Website.map(lambda row: extract_target(row, "Agency"))

# Example usage: extract target_Interest
with_MinAge = with_Agency.map(lambda row: extract_target(row, "MinAge"))

# Example usage: extract target_Interest
with_Interest = with_MinAge.map(lambda row: extract_target(row, "Interest"))

# Example usage: extract target_Region
with_Region = with_Interest.map(lambda row: extract_target(row, "Region"))

# Example usage: extract target_City
with_City = with_Region.map(lambda row: extract_target(row, "City"))

# Example usage: extract target_Language
with_Language = with_City.map(lambda row: extract_target(row, "Language"))

# Example usage: extract target_MaxAge
with_MaxAge = with_Language.map(lambda row: extract_target(row, "MaxAge"))

# Example usage: extract target_Age
with_Age = with_MaxAge.map(lambda row: extract_target(row, "Age"))

# Example usage: extract target_State
with_State = with_Age.map(lambda row: extract_target(row, "State"))

# Example usage: extract target_Retargeting
with_Retargeting = with_State.map(lambda row: extract_target(row, "Retargeting"))

# Example usage: extract target_Gender
with_Gender = with_Retargeting.map(lambda row: extract_target(row, "Gender"))

# Example usage: extract target_Like
with_Like = with_Gender.map(lambda row: extract_target(row, "Like"))

# Example usage: extract target_List
with_List = with_Like.map(lambda row: extract_target(row, "List"))

# Example usage: extract target_Segment
with_Segment = with_List.map(lambda row: extract_target(row, "Segment"))

print(with_Segment.take(5))
