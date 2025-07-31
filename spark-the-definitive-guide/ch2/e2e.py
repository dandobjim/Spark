from pyspark.sql import SparkSession
from pyspark.sql.functions import max, desc

spark = SparkSession.builder.getOrCreate()
# Set the number of shuffle partitions to 5
spark.conf.set("spark.sql.shuffle.partitions", "5")

# Load the 2015 flight data CSV file into a DataFrame
flightdata2015 = spark.read \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .csv("../data/flight-data/csv/2015-summary.csv")

# Show the first 3 rows of the DataFrame
flightdata2015.take(3)

# Explain the DataFrame's execution plan
flightdata2015.sort("count", ascending=False).explain()

# Create views for the DataFrame
flightdata2015.createOrReplaceTempView("flight_data_2015")

# Use SQL to aggregate the data by destination country
sqlWay = spark.sql("""
                   SELECT DEST_COUNTRY_NAME, sum(count) AS total_count
                   FROM flight_data_2015
                   GROUP BY DEST_COUNTRY_NAME
                   """)

# Use the DataFrame API to aggregate the data by destination country
dataFrameWay = flightdata2015\
    .groupby("DEsqlST_COUNTRY_NAME")\
    .count()

# Show the first 3 rows of the SQL result
spark.sql(
    """
    SELECT max(count) AS max_count
    FROM flight_data_2015
    """
).take(1)

# Show the first 3 rows of the DataFrame API result
flightdata2015.select(max("count")).take(1)

# TOP 5 destination countries
maxSQL = spark.sql("""
SELECT DEST_COUNTRY_NAME, 
sum(count) AS total_count
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
                   """)

maxSQL.show()

# Show the first 5 rows of the DataFrame API result

flightdata2015\
    .groupby("DEST_COUNTRY_NAME")\
    .sum("count")\
    .withColumnRenamed("sum(count)", "destination_total")\
    .sort(desc("destination_total"))\
    .limit(5)\
    .show()