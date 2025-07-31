from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

myRange = spark.range(1000).toDF("number")

# print(myRange.show())

divisBy2 = myRange.where("number % 2 = 0")
# print(divisBy2.count())