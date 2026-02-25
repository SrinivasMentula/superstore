# Databricks notebook source
# MAGIC %run /Workspace/Users/sarathazurelearning@gmail.com/superstore/Common/logging

# COMMAND ----------

try:
    NotebookName = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
        .split("/")[-1]
    )
    Error_message = ""
    user_name = spark.sql("select user").collect()[0][0]
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)
    raise e


# COMMAND ----------

try:
    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS  superstore.sales_reporting.sales_by_region_category
    (
    rowId BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY   1),
    region String,
    country string,
    totalSales  DECIMAL(34,2),
    totalProfit DECIMAL(34,2),
    totalquantity Decimal(34,2),
    avgDiscountApplied DECIMAL(34,2),
    avgProfitMargin DECIMAL(34,2),
    totalRevenueAfterDiscount DECIMAL(34,2),
    createdAt TIMESTAMP DEFAULT current_timestamp(),
    createdBy STRING DEFAULT current_user()
    )
    USING DELTA 
    PARTITIONED BY(region)
    TBLPROPERTIES(
    'delta.feature.allowColumnDefaults' = 'enabled',
        'delta.columnMapping.mode' = 'id'
    )

    """
    )
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)
    raise e

# COMMAND ----------

# table creation for  the  TopPerformingSubCategories
try:
    spark.sql(
        """
    CREATE TABLE IF NOT EXISTS superstore.sales_reporting.TopPerformingSubCategories
    (
    rowId  BIGINT GENERATED ALWAYS AS IDENTITY (INCREMENT BY 1 START WITH 1),
    category STRING , 
    subCategory STRING ,
    totalProfit DECIMAL(19,2),
    totalSales DECIMAL(19,2),
    totalQuantity DECIMAL(19,2),
    rank int,
    createBy STRING DEFAULT  current_user(),
    createdOn TIMESTAMP DEFAULT current_timestamp
    )
    USING DELTA 
    PARTITIONED BY(category)
    TBLPROPERTIES(
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.columnMapping.mode' = 'id'
    )
    """
    )
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)
    raise e

# COMMAND ----------

# Table creation script  Shipping Mode Analysis
try:
  spark.sql(
      """CREATE  TABLE IF NOT EXISTS  superstore.sales_reporting.shippingModeAnalysis
  (
    rowId  BIGINT GENERATED ALWAYS as IDENTITY(INCREMENT BY 1  START WITH 1) , 
    shipMode STRING, 
    totalOrders BIGINT , 
    totalSales  DECIMAL(34,2), 
    avgProfitMargin  DECIMAL(34,2),
    createdBy STRING DEFAULT current_user(), 
    createdAt Timestamp default current_timestamp()
  )
  USING DELTA 
  PARTITIONED BY(shipMode)
  TBLPROPERTIES(
      'delta.feature.allowColumnDefaults' = 'supported',
      'delta.columnMapping.mode' = 'id'
      )
  
  """
  )
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)
    raise e
