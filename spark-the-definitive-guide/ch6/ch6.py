from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, instr, expr, pow, round, bround, corr, monotonically_increasing_id

spark = SparkSession.builder \
    .appName("Ch6 Example") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("../data/retail-data/by-day/2010-12-01.csv")

df.printSchema()

df.createOrReplaceTempView("dfTable")

### BOOLEAN EXPRESSIONS
df.where(col("InvoiceNo") != 536365) \
    .select(col("InvoiceNo").alias("Description")) \
    .show()

priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1

df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

DOTCodeFilter = col("StockCode") == "DOT"

df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter)) \
    .where("isExpensive") \
    .select("unitPrice", "isExpensive") \
    .show(5)

df.withColumn("isExpensive", expr("NOT UnitPrice <= 250")) \
    .where("isExpensive") \
    .selectExpr("Description", "UnitPrice").show(5)

#### NUMERIC EXPRESSIONS
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQu antity")) \
    .show(2)

df.select(round(lit(2.5)), bround(lit(2.5))).show()

df.stat.corr("Quantity", "UnitPrice")  # Pearson correlation coefficient
df.select(corr("Quantity", "UnitPrice").alias("correlation")).show()

#### STATISTICS
df.describe().show()

colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile(colName, quantileProbs, relError)

df.crosstab("StockCode", "Quantity").show()

df.stat.freqItems(["StockCode", "Quantity"]).show()

#### AUTO ID
df.select(monotonically_increasing_id()).show(2)
