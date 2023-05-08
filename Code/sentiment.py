import pandas as pd
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
import html2text
from pyspark.sql.functions import udf, col, size, regexp_replace, split, expr, concat_ws
from pyspark.sql.types import IntegerType, FloatType
import sys
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import explode, count, row_number, when, avg, round
from pyspark.sql.types import BooleanType, FloatType
from pyspark.sql.window import Window
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


file_path = "/home/dhwang/Project2/fbpac-ads-en-US.csv"

spark = SparkSession.builder.master("local[*]").appName("Prep").getOrCreate()

df = spark.read.csv(file_path, header=True, inferSchema=True, multiLine=True, sep=',', escape='"',
                    ignoreLeadingWhiteSpace=True)


def html_to_text(html):
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text()

html_to_text_udf = udf(html_to_text)

df2 = df.withColumn("message", html_to_text_udf(df["message"]))

def count_words(text):
    if text is not None:
        return len(text.split())
    else:
        return 0

# Define UDF for counting words
count_words_udf = udf(count_words, IntegerType())

# Add new column for word count
df3 = df2.withColumn("word_count", count_words_udf("message"))

# Remove non-word characters and split the message into words
df3 = df3.withColumn("words", split(regexp_replace(col("message"), "[^\w\s]", ""), " "))

# Calculate the average word length
df3 = df3.withColumn("avg_word_length", expr("aggregate(words, 0, (acc, word) -> acc + length(word), acc -> acc / size(words))"))


def sentiment_score(text):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(text)
    return sentiment["compound"]

sentiment_score_udf = udf(sentiment_score, returnType=FloatType())

df4 = df3.withColumn("sentiment_score", sentiment_score_udf("message"))

df5 = df4.withColumn("words", concat_ws(",", col("words").cast("string")))

df5.write.csv("sentiment.csv", header=True)

