from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr
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

"""
SELECT *, 1 as One
FROM dfTable LIMIT 2
"""
df.select(expr("*"),
          lit(1).alias("One")).show(2)

df.withColumn("numberOne", lit(1)).show(2)

######## ADD COLUMNS

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME = DEST_COUNTRY_NAME")).show(2)

print(df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns)

######## Special Characters

dfWithColName = df.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
dfWithColName.selectExpr("`This Long Column-Name`",
                         "`This Long Column-Name` AS `new col`").show(2)

##### DROP

print(df.drop("ORIGIN_COUNTRY_NAME").columns)
print(dfWithColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").columns)

###### CAST
"""
SELECT *, CAST(count AS long) AS count2
FROM dfTable
"""
df.withColumn("count2", col("count").cast(LongType())).printSchema()

#### FILTERING
"""
SELECT *
FROM dfTable
WHERE count < 2 LIMIT 2
"""
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)

"""
SELECT *
FROM dfTable
WHERE count < 2
  AND ORIGIN_COUNTRY_NAME != 'Croatia'
LIMIT 2
"""

df.filter(col("count") < 2) \
    .filter(col("ORIGIN_COUNTRY_NAME") != 'Croatia') \
    .show(2)

df.where("count < 2") \
    .where("ORIGIN_COUNTRY_NAME != 'Croatia'") \
    .show(2)

df.where(col("count") < 2) \
    .where(col("ORIGIN_COUNTRY_NAME") != "Croatia") \
    .show(2)

###### UNIQUE
"""
SELECT COUNT(DISTINCT (ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME))
FROM dfTable
"""
print(df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count())

"""
SELECT COUNT DISTINCT ORIGIN_COUNTRY_NAME
FROM dfTable
"""

print(df.select("ORIGIN_COUNTRY_NAME").distinct().count())

####### RANDOM SAMPLES

seed = 5
withReplacement = False
fraction = 0.5
print(df.sample(withReplacement, fraction, seed).count())

#### RANDOM SPLITS
dataFrames = df.randomSplit([0.25, 0.75], seed=seed)
print(dataFrames[0].count() > dataFrames[1].count())

#### UNION
