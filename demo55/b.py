Below are the pytest test cases for the above python code.

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

def test_read_source(spark_session):
    source_df = spark_session.read.option("header", "true").csv("trans.txt")
    assert source_df.count() > 0

def test_account_number(spark_session):
    source_df = spark_session.read.option("header", "true").csv("trans.txt")
    result_df = source_df.withColumn("ACCOUNT_NIUMBER", lpad(source_df["ACCOUNT_NO"], 16, "0"))
    assert "ACCOUNT_NIUMBER" in result_df.schema.names
    assert result_df.schema["ACCOUNT_NIUMBER"].dataType == "string"
    assert len(result_df.select("ACCOUNT_NIUMBER").first()[0]) == 16

def test_balance_amount(spark_session):
    source_df = spark_session.read.option("header", "true").csv("trans.txt")
    result_df = source_df.withColumnRenamed("TRANS_AMT", "BALANCE_AMOUNT")
    assert "BALANCE_AMOUNT" in result_df.schema.names
    assert result_df.schema["BALANCE_AMOUNT"].dataType == "decimal"

def test_age_group(spark_session):
    source_df = spark_session.read.option("header", "true").csv("trans.txt")
    result_df = source_df.withColumn("AGE_GROUP", when(source_df["AGE"] < 20, "0-20").when(source_df["AGE"] < 30, "21-30").when(source_df["AGE"] < 40, "31-40").otherwise("41+"))
    assert "AGE_GROUP" in result_df.schema.names
    assert result_df.schema["AGE_GROUP"].dataType == "string"
    assert result_df.select("AGE_GROUP").distinct().count() == 4

def test_total_price(spark_session):
    source_df = spark_session.read.option("header", "true").csv("trans.txt")
    result_df = source_df.withColumn("TOTAL_PRICE", source_df["QUANTITY"] * source_df["UNIT_PRICE"])
    assert "TOTAL_PRICE" in result_df.schema.names
    assert result_df.schema["TOTAL_PRICE"].dataType == "decimal"