import pyspark
from pyspark.sql import SparkSession
import numpy as np
import matplotlib.pyplot as plt
import seaborn
import pandas

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", "true").csv("toy_dataset.csv").toPandas()
#df.show()

print(type(df["Income"][0]))