import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, udf, explode, count, row_number, when
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("WordCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data = spark.read.csv("message_data_with_year.csv", header=True, inferSchema=True)

data = data.filter(col("message").isNotNull())
data = data.filter(col("year").isNotNull())
data = data.withColumn("message", col("message").cast("string"))

tokenizer = Tokenizer(inputCol="message", outputCol="words")
tokenized = tokenizer.transform(data)

# remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
cleaned_data = remover.transform(tokenized)

exploded_data = cleaned_data.select("year", explode(col("filtered_words")).alias("word"))

# filter out non-letter words
result_df = exploded_data.filter(col("word").rlike("^[a-zA-Z]+$"))

# count words and group by year
result_df = result_df.groupBy("year", "word").agg(count("*").alias("count"))

window_spec = Window.partitionBy("year").orderBy(col("count").desc())

# add a rank column and filter to keep only the top 10 words for each year
result_df = result_df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 10)

for year in range(2017, 2023):
    print(f"Year {year}:")
    result_df.filter(col("year") == year).show(10)

spark.stop()