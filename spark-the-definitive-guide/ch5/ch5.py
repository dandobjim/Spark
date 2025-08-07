from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType, LongType

spark = SparkSession.builder.master("local[*]").getOrCreate()

myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", LongType(), False, metadata={"hello": "world"})
])

df = spark.read \
    .format("json") \
    .schema(myManualSchema) \
    .load("../data/flight-data/json/2015-summary.json")

"""
df.printSchema()
print(df.columns)
print(df.first())
"""

"""
myRow = Row("Hello", None, 1, False)
print(myRow[0])
print(myRow[2])
"""

df.createOrReplaceTempView("dfTable")

# SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2
df.select("DEST_COUNTRY_NAME").show(2)

# SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

df.select(
    col("DEST_COUNTRY_NAME"),
    col("ORIGIN_COUNTRY_NAME")
).show(2)

# SELECT DEST_COUNTRY_NAME AS destination FROM dfTable LIMIT 2
df.select(
    col("DEST_COUNTRY_NAME")
    .alias("destination")
).show(2)

# selectExpr
df.selectExpr(
    "DEST_COUNTRY_NAME AS newColumnName",
    "DEST_COUNTRY_NAME"
).show(2)

"""
SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS withinCountry
FROM dfTABLE LIMIT 2
"""
df.selectExpr(
    "*",
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS withinCountry"
).show(2)

"""
SELECT avg(count),
       COUNT(distinct (DEST_COUNTRY_NAME))
FROM dfTable LIMIT 2
"""
df.selectExpr(
    "AVG(count)",
    "COUNT(DISTINCT(DEST_COUNTRY_NAME))"
).show(2)
