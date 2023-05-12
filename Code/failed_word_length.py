import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split, length, avg, size
from pyspark.sql.types import BooleanType

spark = SparkSession.builder.appName("WordLength").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data = spark.read.csv("message_data_with_year.csv", header=True, inferSchema=True)

data = data.filter(col("message").isNotNull())
data = data.filter(col("year").isNotNull())
#data = data.withColumn("message", col("message").cast("string"))

def filter_year(year):
    if year == 'NA':
        return False
    year = int(year)
    return 2017 <= year <= 2022


filter_udf = udf(filter_year, returnType=BooleanType())
filtered_data = data.filter(filter_udf(data.year))

result_df = filtered_data.groupBy("year").agg(
    avg(length(col("message")) / size(split(col("message"), " "))).alias("avg_word_length"),
    avg(size(split(col("message"), " "))).alias("avg_words_per_message")
)

result_df.show()

spark.stop()