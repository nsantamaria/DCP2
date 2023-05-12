import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, udf, explode, count, row_number
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

remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
cleaned_data = remover.transform(tokenized)

exploded_data = cleaned_data.select(explode(col("filtered_words")).alias("word"))

result_df = exploded_data.filter(col("word").rlike("^[a-zA-Z]+$"))

result_df = result_df.repartition(4).groupBy("word").agg(count("*").alias("count"))

window_spec = Window.orderBy(col("count").desc())

result_df = result_df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 50)

print("Top 50 most common words:")
result_df.show(50)

spark.stop()