import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("mapping").getOrCreate()

# Read the source data
source_df = spark.read.option("header", "true").csv("trans.txt")

# Entry 1: Left pad the ACCOUNT_NO to make it 16 digit value
source_df = source_df.withColumn("ACCOUNT_NIUMBER", lpad(source_df["ACCOUNT_NO"], 16, "0"))

# Entry 2: Direct Move
source_df = source_df.withColumnRenamed("TRANS_AMT", "BALANCE_AMOUNT")

# Entry 3: Convert age to age group(e.g. , 20-30,31-40,etc.)
source_df = source_df.withColumn("AGE_GROUP", when(source_df["AGE"] < 20, "0-20").when(source_df["AGE"] < 30, "21-30").when(source_df["AGE"] < 40, "31-40").otherwise("41+"))

# Entry 4: Multiply quantity by unit price to get total price
source_df = source_df.withColumn("TOTAL_PRICE", source_df["QUANTITY"] * source_df["UNIT_PRICE"])

# Write to target
source_df.write.mode("overwrite").option("header", "true").csv("DIM_TRANS")