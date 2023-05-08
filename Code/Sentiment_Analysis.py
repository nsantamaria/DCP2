import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, udf, explode, count, row_number, when, avg, round
from pyspark.sql.types import BooleanType, FloatType
from pyspark.sql.window import Window
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# calculate sentiment score
def sentiment_score(text):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(text)
    return sentiment["compound"]

sentiment_score_udf = udf(sentiment_score, returnType=FloatType())

spark = SparkSession.builder.appName("WordCountSentiment").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data = spark.read.csv("message_data_with_year.csv", header=True, inferSchema=True)

data = data.filter(col("message").isNotNull())
data = data.filter(col("year").isNotNull())
data = data.withColumn("message", col("message").cast("string"))
data = data.filter(col("year").isNotNull() & (col("year") != "NA"))

# calculate sentiment score for each row
data = data.withColumn("sentiment_score", sentiment_score_udf("message"))

# group by year and calculate the avg sentiment score
sentiment_data = data.groupBy("year").agg(round(avg("sentiment_score"), 2).alias("avg_sentiment_score")).orderBy("year")

sentiment_data.show()
spark.stop()