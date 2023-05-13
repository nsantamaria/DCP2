import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, udf, concat_ws, avg, round
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def sentiment_score(text):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(text)
    return sentiment["compound"]

sentiment_score_udf = udf(sentiment_score, returnType=FloatType())

spark = SparkSession.builder.appName("WordCountSentiment").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data = spark.read.csv("trump_biden.csv", header=True, inferSchema=True)

data = data.filter(col("message").isNotNull() | col("advertiser").isNotNull() | col("year_month").isNotNull())
data = data.withColumn("message", col("message").cast("string"))

data = data.withColumn("sentiment_score", sentiment_score_udf("message"))

trump_data = data.filter(col("advertiser") == "Donald J. Trump")
biden_data = data.filter(col("advertiser") == "Joe Biden")

trump_sentiment_data = trump_data.groupBy("year_month").agg(round(avg("sentiment_score"), 4).alias("avg_sentiment_score")).orderBy("year_month")
biden_sentiment_data = biden_data.groupBy("year_month").agg(round(avg("sentiment_score"), 4).alias("avg_sentiment_score")).orderBy("year_month")

print("Donald J. Trump:")

all_trump_sentiment_data = trump_sentiment_data.collect()

for row in all_trump_sentiment_data:
    print(row)

print("Joe Biden:")
biden_sentiment_data.show()

spark.stop()