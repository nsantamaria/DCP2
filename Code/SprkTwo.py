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


def website(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "Website":
                result = item["segment"]
        except:
            result = " "
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
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.
                      target_Gender, target_Like=r.target_Like, target_List=r.target_List, target_Segment=r.
                      target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      result)
    return updated_row


with_website = df.rdd.map(website)


def agency(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "Agency":
                result = item["segment"]
        except:
            result = " "
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
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.
                      target_Gender, target_Like=r.target_Like, target_List=r.target_List, target_Segment=r.
                      target_Segment, target_MinAge=r.target_MinAge, target_Agency=result, target_Website=
                      r.target_Website)
    return updated_row


with_Agency = with_website.map(agency)


def minage(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "MinAge":
                result = item["segment"]
        except:
            result = " "
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
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.
                      target_Gender, target_Like=r.target_Like, target_List=r.target_List, target_Segment=r.
                      target_Segment, target_MinAge=result, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_MinAge = with_Agency.map(minage)


def segment(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "segment":
                result = item["segment"]
        except:
            result = " "
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
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.
                      target_Gender, target_Like=r.target_Like, target_List=r.target_List, target_Segment=result,
                      target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_segment = with_MinAge.map(segment)


def t_list(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "List":
                result = item["segment"]
        except:
            result = " "
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
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.
                      target_Gender, target_Like=r.target_Like, target_List=result, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_list = with_segment.map(t_list)


def Like(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "Like":
                result = item["segment"]
        except:
            result = " "
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
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.
                      target_Gender, target_Like=result, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_Like = with_list.map(Like)


def Gender(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "Gender":
                result = item["segment"]
        except:
            result = " "
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
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=result,
                      target_Like=r.target_Like, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_Gender = with_Like.map(Gender)


def Retargeting(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "Retargeting":
                result = item["segment"]
        except:
            result = " "
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
                      target_State=r.target_State, target_Retargeting=result, target_Gender=r.target_Gender,
                      target_Like=r.target_Like, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_Retargeting = with_Gender.map(Retargeting)



def state(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "State":
                result = item["segment"]
        except:
            result = " "
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
                      target_State=result, target_Retargeting=r.target_Retargeting, target_Gender=r.target_Gender,
                      target_Like=r.target_Like, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_state = with_Retargeting.map(state)


def age(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "Age":
                result = item["segment"]
        except:
            result = " "
    r = row
    updated_row = Row(id=r.id, html=r.html, political=r.political, not_political=r.not_political, title=r.title,
                      message=r.message, thumbnail=r.thumbnail, created_at=r.created_at, updated_at=r.updated_at,
                      lang=r.lang, images=r.images, impressions=r.impressions,
                      political_probability=r.political_probability, targeting=r.targeting, suppressed=r.suppressed,
                      targets=r.targets, advertiser=r.advertiser, entities=r.entities, page=r.page,
                      lower_page=r.lower_page, targetings=r.targetings, paid_for_by=r.paid_for_by,
                      targetedness=r.targetedness, listbuilding_fundraising_proba=r.listbuilding_fundraising_proba,
                      target_Interest=r.target_Interest, target_Region=r.target_Region, target_City=r.target_City,
                      target_Language=r.target_Language, target_MaxAge=r.target_MaxAge, target_Age=result,
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.target_Gender,
                      target_Like=r.target_Like, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_age = with_state.map(age)


def MaxAge(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "MaxAge":
                result = item["segment"]
        except:
            result = " "
    r = row
    updated_row = Row(id=r.id, html=r.html, political=r.political, not_political=r.not_political, title=r.title,
                      message=r.message, thumbnail=r.thumbnail, created_at=r.created_at, updated_at=r.updated_at,
                      lang=r.lang, images=r.images, impressions=r.impressions,
                      political_probability=r.political_probability, targeting=r.targeting, suppressed=r.suppressed,
                      targets=r.targets, advertiser=r.advertiser, entities=r.entities, page=r.page,
                      lower_page=r.lower_page, targetings=r.targetings, paid_for_by=r.paid_for_by,
                      targetedness=r.targetedness, listbuilding_fundraising_proba=r.listbuilding_fundraising_proba,
                      target_Interest=r.target_Interest, target_Region=r.target_Region, target_City=r.target_City,
                      target_Language=r.target_Language, target_MaxAge=result, target_Age=r.target_Age,
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.target_Gender,
                      target_Like=r.target_Like, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_MaxAge = with_age.map(MaxAge)

def Language(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "Language":
                result = item["segment"]
        except:
            result = " "
    r = row
    updated_row = Row(id=r.id, html=r.html, political=r.political, not_political=r.not_political, title=r.title,
                      message=r.message, thumbnail=r.thumbnail, created_at=r.created_at, updated_at=r.updated_at,
                      lang=r.lang, images=r.images, impressions=r.impressions,
                      political_probability=r.political_probability, targeting=r.targeting, suppressed=r.suppressed,
                      targets=r.targets, advertiser=r.advertiser, entities=r.entities, page=r.page,
                      lower_page=r.lower_page, targetings=r.targetings, paid_for_by=r.paid_for_by,
                      targetedness=r.targetedness, listbuilding_fundraising_proba=r.listbuilding_fundraising_proba,
                      target_Interest=r.target_Interest, target_Region=r.target_Region, target_City=r.target_City,
                      target_Language=result, target_MaxAge=r.target_MaxAge, target_Age=r.target_Age,
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.target_Gender,
                      target_Like=r.target_Like, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_Language = with_MaxAge.map(Language)


def City(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "City":
                result = item["segment"]
        except:
            result = " "
    r = row
    updated_row = Row(id=r.id, html=r.html, political=r.political, not_political=r.not_political, title=r.title,
                      message=r.message, thumbnail=r.thumbnail, created_at=r.created_at, updated_at=r.updated_at,
                      lang=r.lang, images=r.images, impressions=r.impressions,
                      political_probability=r.political_probability, targeting=r.targeting, suppressed=r.suppressed,
                      targets=r.targets, advertiser=r.advertiser, entities=r.entities, page=r.page,
                      lower_page=r.lower_page, targetings=r.targetings, paid_for_by=r.paid_for_by,
                      targetedness=r.targetedness, listbuilding_fundraising_proba=r.listbuilding_fundraising_proba,
                      target_Interest=r.target_Interest, target_Region=r.target_Region, target_City=result,
                      target_Language=r.target_Language, target_MaxAge=r.target_MaxAge, target_Age=r.target_Age,
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.target_Gender,
                      target_Like=r.target_Like, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_City = with_Language.map(City)


def Region(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "Region":
                result = item["segment"]
        except:
            result = " "
    r = row
    updated_row = Row(id=r.id, html=r.html, political=r.political, not_political=r.not_political, title=r.title,
                      message=r.message, thumbnail=r.thumbnail, created_at=r.created_at, updated_at=r.updated_at,
                      lang=r.lang, images=r.images, impressions=r.impressions,
                      political_probability=r.political_probability, targeting=r.targeting, suppressed=r.suppressed,
                      targets=r.targets, advertiser=r.advertiser, entities=r.entities, page=r.page,
                      lower_page=r.lower_page, targetings=r.targetings, paid_for_by=r.paid_for_by,
                      targetedness=r.targetedness, listbuilding_fundraising_proba=r.listbuilding_fundraising_proba,
                      target_Interest=r.target_Interest, target_Region=result, target_City=r.target_City,
                      target_Language=r.target_Language, target_MaxAge=r.target_MaxAge, target_Age=r.target_Age,
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=r.target_Gender,
                      target_Like=r.target_Like, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_Region = with_City.map(Region)


def Interest(row):
    targets = new_quoter(row)
    targets = json.loads(targets)
    result = " "
    for item in targets:
        try:
            if item["target"] == "Interest":
                result = item["segment"]
        except:
            result = " "
    r = row
    updated_row = Row(id=r.id, html=r.html, political=r.political, not_political=r.not_political, title=r.title,
                      message=r.message, thumbnail=r.thumbnail, created_at=r.created_at, updated_at=r.updated_at,
                      lang=r.lang, images=r.images, impressions=r.impressions,
                      political_probability=r.political_probability, targeting=r.targeting, suppressed=r.suppressed,
                      targets=r.targets, advertiser=r.advertiser, entities=r.entities, page=r.page,
                      lower_page=r.lower_page, targetings=r.targetings, paid_for_by=r.paid_for_by,
                      targetedness=r.targetedness, listbuilding_fundraising_proba=r.listbuilding_fundraising_proba,
                      target_Interest=result, target_Region=r.target_Region, target_City=r.target_City,
                      target_Language=r.target_Language, target_MaxAge=r.target_MaxAge, target_Age=r.target_Age,
                      target_State=r.target_State, target_Retargeting=r.target_Retargeting, target_Gender=
                      r.target_Gender, target_Like=r.target_Like, target_List=r.target_List, target_Segment=
                      r.target_Segment, target_MinAge=r.target_MinAge, target_Agency=r.target_Agency, target_Website=
                      r.target_Website)
    return updated_row


with_Interest = with_Region.map(Interest)


# print(with_Interest.take(5))
df = with_Interest.toDF()
df.show(2, truncate=True)
df.write.option("header", True).csv("/home/fneffati/DataSets/output/")


