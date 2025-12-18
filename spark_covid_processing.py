from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col

# =====================
# Spark Session
# =====================
spark = SparkSession.builder \
    .appName("COVID-19 Indonesia Analysis") \
    .getOrCreate()

# =====================
# Load data from HDFS
# =====================
input_path = "hdfs:///data/covid/covid_19_indonesia_time_series_all.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

# =====================
# Aggregation 1:
# Total cases per province
# =====================
cases_by_province = df.groupBy("Province") \
    .agg(_sum("New Cases").alias("Total_Cases")) \
    .orderBy(col("Total_Cases").desc())

cases_by_province.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("hdfs:///output/covid/cases_by_province")

# =====================
# Aggregation 2:
# Daily national trend
# =====================
daily_trend = df.groupBy("Date") \
    .agg(_sum("New Cases").alias("Daily_Cases")) \
    .orderBy("Date")

daily_trend.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("hdfs:///output/covid/daily_trend")

spark.stop()
