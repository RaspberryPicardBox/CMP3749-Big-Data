import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()

ratings = spark.read.option("header", "true").csv("ratings.csv")
ratings.show(10)
ratings.printSchema()

movies = spark.read.option("header", "true").csv("movies.csv").withColumnRenamed("count(userId)", "num_ratings")
movies.show(10)
movies.printSchema()

most_popular = ratings.groupBy("movieID").agg(count("userId"))
most_popular.show()
