import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg, round
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def sentiment_score(text):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(text)
    return sentiment["compound"]

sentiment_score_udf = udf(sentiment_score, returnType=FloatType())

spark = SparkSession.builder.appName("WordCountSentiment").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data = spark.read.csv("english_message_year_month.csv", header=True, inferSchema=True)

data = data.filter(col("message").isNotNull() & col("year_month").isNotNull())
data = data.withColumn("message", col("message").cast("string"))

data = data.withColumn("sentiment_score", sentiment_score_udf("message"))

sentiment_data = data.groupBy("year_month").agg(round(avg("sentiment_score"), 4).alias("avg_sentiment_score")).orderBy("year_month")

all_sentiment_data = sentiment_data.collect()

for row in all_sentiment_data:
    print(row)

spark.stop()
