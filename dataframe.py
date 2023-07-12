from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder.appName("MovieRatings").getOrCreate()

# Task-1: Define the schema for the "movies.dat" file
movies_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genre", StringType(), True)
])

# Read "movies.dat" file into a DataFrame
movies_df = spark.read \
    .option("delimiter", "::") \
    .schema(movies_schema) \
    .csv("ml-1m/movies.dat")
movies_df.show(10)

# Define the schema for the "ratings.dat" file
ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", IntegerType(), True)
])

# Read "ratings.dat" file into a DataFrame
ratings_df = spark.read \
    .option("delimiter", "::") \
    .schema(ratings_schema) \
    .csv("ml-1m/ratings.dat")

ratings_df.show()

# Task-2: Calculate max, min, and average ratings for each movie
rating_stats_df = ratings_df.groupBy("movieId") \
    .agg(min("rating").alias("min_rating"),
         max("rating").alias("max_rating"),
         avg("rating").alias("avg_rating"))

rating_stats_df.show()

# Join movies_df with rating_stats_df based on movieId
movie_ratings_df = movies_df.join(rating_stats_df, "movieId", "left")

# Show the resulted DataFrame
movie_ratings_df.show()

# Task-3: Rank movies based on ratings for each user
windowSpec = Window.partitionBy("userId").orderBy(col("rating").desc())
ranked_df = ratings_df.withColumn("row_number", row_number().over(windowSpec)).\
    filter(col('row_number') <= 3).drop('row_number')
ranked_df.show()
# Filter the top 3 movies for each user
user_top_three_movies_df = ranked_df.join(movies_df, "movieId")

# Show the resulting DataFrame
user_top_three_movies_df.show()

# Task-4 Write the original DataFrame to Parquet format
movies_df.write.mode("overwrite").parquet("movies.parquet")
ratings_df.write.mode("overwrite").parquet("ratings.parquet")

# Write the new DataFrame to Parquet format

movie_ratings_df.coalesce(1).write.mode("overwrite").parquet("movie_ratings_df.parquet")
user_top_three_movies_df.coalesce(1).write.mode("overwrite").parquet("user_top_three_movies.parquet")

# Spark Submit Command
# spark-submit --master local --name MovieRatingsApp dataframe.py
