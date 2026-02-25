# Databricks notebook source
from pyspark.sql.functions import col,sum,avg,rank,desc,count
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run /Workspace/Users/sarathazurelearning@gmail.com/superstore/Common/logging

# COMMAND ----------

# MAGIC %run /Workspace/Users/sarathazurelearning@gmail.com/superstore/outbound/outboundTableCreation

# COMMAND ----------

# Declaring the Local variables 
NotebookName =( dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1])
Error_message = ""
user_name = spark.sql('select user').collect()[0][0]

# COMMAND ----------

try:
  # reading the data from the stage table as dat frame 
  stageTableDF = spark.read.table("superstore.sales_reporting.staging")
  # aggregating the data 
  temp_df = (stageTableDF.
    groupBy("Region","country").
    agg(sum(col("sales")).alias("totalSales"),
        sum(col("profit")).alias("totalProfit"),
        sum(col("quantity")).alias("totalquantity"),
        avg("discount").cast(DecimalType(34,2)).alias("avgDiscountApplied"),
        avg("profitMargin").cast(DecimalType(34,2)).alias("avgProfitMargin"),
        sum("revenueAfterDiscount").cast(DecimalType(34,2)).alias("totalRevenueAfterDiscount"), 
        )
  )
  # Truncating the outbound layer as it is  daily refresh one
  trunacte_query  = "TRUNCATE table superstore.sales_reporting.sales_by_region_category"
  spark.sql(trunacte_query)
  # creating the temp view for the 
  temp_df.createOrReplaceTempView("tempDFTview")
  tempDF_columns = temp_df.columns
  column_string = ','.join(tempDF_columns)
  # appending the data into outbound table  
  query = f"""
  INSERT INTO  superstore.sales_reporting.sales_by_region_category
  (
  {column_string}
  )
  SELECT 
  {column_string}
  from 
  tempDFTview
  """
  spark.sql(query)
except Exception as e:
  Error_message = e
  exceptionLogLoad(NotebookName, Error_message)


# COMMAND ----------

try:
    window_spec = Window.partitionBy("category").orderBy(col("totalProfit").desc())
    # reading the data from the stage table as dat frame
    stageTableDF = spark.read.table("superstore.sales_reporting.staging")
    # aggregating the data
    # Top Performing Sub-Categories
    aggregated_df = (
        stageTableDF.groupBy("category", "subCategory")
        .agg(
            sum("sales").alias("totalSales"),
            sum("profit").alias("totalProfit"),
            sum("quantity").alias("totalquantity"),
        )
        .withColumn("rank", rank().over(window_spec))
    )
    # creating the temp view
    aggregated_df.createOrReplaceTempView("aggregatedDFTview")
    aggregated_columns = aggregated_df.columns
    aggregated_columnString = ",".join(aggregated_columns)
    # Truncating the superstore.sales_reporting.TopPerformingSubCategories daily refresh
    TopPerformingSubCategories_truncate = (
        " TRUNCATE TABLE superstore.sales_reporting.TopPerformingSubCategories "
    )
    spark.sql(TopPerformingSubCategories_truncate)
except Exception as e:
    Error_message = e
    exceptionLogLoad(NotebookName, Error_message)

# COMMAND ----------

# Processing the data into the TopPerformingSubCategories
try:
    insert_query = f"""
                    INSERT INTO  superstore.sales_reporting.TopPerformingSubCategories
                    (
                      {
                        aggregated_columnString
                      }
                      )
                    SELECT 
                    {
                      aggregated_columnString
                    }
                    FROM 
                    aggregatedDFTview
              """
except Exception as e:
    Error_message = e
    exceptionLogLoad(NotebookName, Error_message)

# COMMAND ----------

# Processing the data into the shippingMode table 
try:
    shippingModeDF = (
    stageTableDF.
    groupBy("shipMode").
    agg(count("shipMode").alias("totalOrders"),
        sum("sales").alias("totalSales"),
        avg("profitMargin").alias("avgProfitMargin")
        )
        )
    shippingModeColumnList = shippingModeDF.columns
    shippingModeColumnString =','.join(shippingModeColumnList)
    shippingModeDF.createOrReplaceTempView("shippingModeView")
except Exception as e:
    Error_message = e
    exceptionLogLoad(NotebookName, Error_message)

# COMMAND ----------

# Processing the data into the TopPerformingSubCategories
try:
    insert_query = f"""
                    INSERT INTO  superstore.sales_reporting.shippingModeAnalysis
                    (
                      {
                        shippingModeColumnString
                      }
                      )

                    SELECT 
                    {
                      shippingModeColumnString
                    }

                    FROM 
                    shippingModeView
              """
    spark.sql(insert_query)
except Exception as e:
    Error_message = e
    print(e)
