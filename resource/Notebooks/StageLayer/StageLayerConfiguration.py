# Databricks notebook source
# importing the required libraries for the  the data processing
from pyspark.sql.functions import trim, col, cast
from pyspark.sql.types import DecimalType

# COMMAND ----------

# MAGIC %run ../Common/logging

# COMMAND ----------

# MAGIC %run ./stageTableCreation

# COMMAND ----------

NotebookName =( dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1])
Error_message = ""
user_name = spark.sql('select user').collect()[0][0]

# COMMAND ----------

# trimming all the white spaces from the string column in the given inbound data

# reading the  inbound  table as a data frame and trimming the string column to avoid the extra white spaces

try:
    inbound_df = spark.read.table("superstore.sales_reporting.sales_inbound")
    inbound_df = (
        inbound_df.withColumn("shipMode", trim(col("shipMode")))
        .withColumn("segment", trim(col("segment")))
        .withColumn("country", trim(col("country")))
        .withColumn("city", trim(col("city")))
        .withColumn("state", trim(col("state")))
        .withColumn("region", trim((col("Region"))))
        .withColumn("category", trim(col("category")))
        .withColumn("subCategory", trim(col("subCategory")))
    )
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)
    raise

# COMMAND ----------

try:
    inbound_df_columns = inbound_df.columns
    required_columns = [i for i in inbound_df_columns if i != "rowId"]
    inbound_df = inbound_df.select(*required_columns)
    # replacing the - values into the  None values
    inbound_df = inbound_df.replace("-", None)
    inbound_df = inbound_df.fillna(
        {
            "shipMode": "Unkown",
            "segment": "Unkown",
            "country": "Unkown",
            "city": "Unkown",
            "state": "Unkown",
            "postalCode": 0,
            "region": "Unkown",
            "category": "Unkown",
            "subCategory": "Unkown",
            "sales": 0.0,
            "quantity": 0,
            "discount": 0.0,
            "profit": 0.0,
        }
    )
    rejected_records = inbound_df.filter("country is null").filter(
        "sales < 0 or sales = 0 or quantity = 0"
    )
    # creating a temp view to load the data
    rejected_records.createOrReplaceTempView("rejectedRecordsTview")
    column_string = ",".join(required_columns)
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)
    raise

# COMMAND ----------

# Loading the reject records into the  rejected table
try:
    spark.sql(
        f"""
            
            Insert into superstore.sales_reporting.rejectedsalesRecords
            (
              {column_string}
            )
            
            select  {column_string}  from 
            rejectedRecordsTview
            """
    )
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)
    raise

# COMMAND ----------

# generating the stage layer columns
try:
    stageDf = inbound_df.withColumn(
        "revenueAfterDiscount",
         (col("sales") - (col("sales") * col("discount"))).cast(DecimalType(20, 4)),
    ).withColumn("profitMargin", (col("profit") / col("sales")) * 100)
    stageTableColumns = stageDf.columns
    stageColumnsString = ",".join(stageTableColumns)
    # print(stageColumnsString)
    # tempview creation onthe stage DF
    stageDf.createOrReplaceTempView("stageTableTView")

    # loading the data into the stage layer table
    truncate_query = " Truncate table superstore.sales_reporting.staging "

    query = f"""INSERT INTO superstore.sales_reporting.staging 
            (  
              {stageColumnsString}
            )
            SELECT 
            {stageColumnsString}
            FROM 
            stageTableTView
            """
    spark.sql(query)
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT  * FROM sales_reporting.staging
