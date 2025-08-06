import os
import glob

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col, date_format


spark = SparkSession.builder \
    .appName("CSV Load Test") \
    .master("local[*]") \
    .getOrCreate()

csv_path = os.path.abspath("../data/retail-data/by-day") + "/*.csv"

### STATIC
staticDataFrame = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)

staticDataFrame.show(5)

staticDataFrame.createOrReplaceTempView("retail_data")

staticSchema = staticDataFrame.schema


staticDataFrame\
    .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) AS total_cost",
    "InvoiceDate"
    )\
    .groupby(col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
    .sum("total_cost")\
    .show(5)


### STREAMING
"""# Uncomment the following lines to run the streaming example
# Note: This requires a Spark environment with streaming capabilities.
streamingDataFrame = spark.readStream \
    .schema(staticSchema) \
    .option("maxFilesPerTrigger", 1) \
    .format("csv") \
    .option("header", "true") \
    .load(csv_path)

purchaseByCustomerPerHour = streamingDataFrame \
    .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) AS total_cost",
    "InvoiceDate"
) \
    .groupby(col("CustomerId"), window(col("InvoiceDate"), "1 day")) \
    .sum("total_cost")

purchaseByCustomerPerHour.writeStream \
    .format("memory") \
    .queryName("customer_purchases") \
    .outputMode("complete") \
    .start()

purchaseByCustomerPerHour.writeStream \
    .format("console") \
    .queryName("customer_purchases_2") \
    .outputMode("complete") \
    .start()

spark.sql(
          SELECT *
          FROM customer_purchases
          ORDER BY `sum(total_cost)` DESC
          ) \
    .show(5)
"""

### ML and Analytics
"""
preppedDataFrame = staticDataFrame \
    .na.fill(0)\
    .withColumn("day_of_week",
                date_format(col("InvoiceDate"), "EEEE")) \
    .coalesce(5)

trainDataFrame = preppedDataFrame \
    .where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame \
    .where("InvoiceDate >= '2011-07-01'")

print("Training DataFrame Count: ", trainDataFrame.count())
print("Test DataFrame Count: ", testDataFrame.count())

print("========== INDEXER ===========")
indexer = StringIndexer(inputCol="day_of_week", outputCol="day_of_week_index")
print("========== ENCODER ===========")
encoder = OneHotEncoder(inputCol="day_of_week_index", outputCol="day_of_week_encoded")
print("========== Assembler ===========")
vectorAssembler = VectorAssembler(inputCols=["day_of_week_encoded", "UnitPrice", "Quantity"],
                                  outputCol="features")
print("========== PIPELINE ===========")
transformationPipeline = Pipeline(stages=[indexer, encoder, vectorAssembler])

print("========== FIT ===========")
fittedPipeline = transformationPipeline.fit(trainDataFrame)

print("========== TRANSFORM ===========")
transformedTraining = fittedPipeline.transform(trainDataFrame)
transformedTraining.cache()

print("========== TRAIN ===========")
kmeans = KMeans()\
    .setK(20)\
    .setSeed(1)


kmModel = kmeans.fit(transformedTraining)
transformedTest = fittedPipeline.transform(testDataFrame)
"""

# LOWER LEVEL API
