import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col


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

spark.sql("""
          SELECT *
          FROM customer_purchases
          ORDER BY `sum(total_cost)` DESC
          """) \
    .show(5)


### ML and Analytics
