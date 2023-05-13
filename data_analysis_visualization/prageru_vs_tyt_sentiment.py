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

data = spark.read.csv("media.csv", header=True, inferSchema=True)

data = data.filter(col("message").isNotNull() | col("advertiser").isNotNull() | col("year_month").isNotNull())
data = data.withColumn("message", col("message").cast("string"))

data = data.withColumn("sentiment_score", sentiment_score_udf("message"))

prager_data = data.filter(col("advertiser") == "PragerU")
tyt_data = data.filter(col("advertiser") == "The Young Turks")

prager_sentiment_data = prager_data.groupBy("year_month").agg(round(avg("sentiment_score"), 4).alias("avg_sentiment_score")).orderBy("year_month")
tyt_sentiment_data = tyt_data.groupBy("year_month").agg(round(avg("sentiment_score"), 4).alias("avg_sentiment_score")).orderBy("year_month")

print("PragerU:")

all_prager_sentiment_data = prager_sentiment_data.collect()

for row in all_prager_sentiment_data:
    print(row)

print("The Young Turks:")
all_tyt_sentiment_data = tyt_sentiment_data.collect()

for row in all_tyt_sentiment_data:
    print(row)

spark.stop()