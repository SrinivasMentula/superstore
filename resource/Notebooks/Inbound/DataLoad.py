# Databricks notebook source
from pyspark.sql.types import StringType,IntegerType, DoubleType , StructField, StructType, DecimalType, ArrayType
from delta import DeltaTable
from pyspark.sql.functions import user


# COMMAND ----------

# MAGIC %run ../Common/LoggingTableCreation

# COMMAND ----------

# MAGIC %run ../Common/logging

# COMMAND ----------

NotebookName =( dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1])
Error_message = ""
user_name = spark.sql('select user').collect()[0][0]

# COMMAND ----------

try:
  spark.sql(
        """CREATE TABLE IF NOT EXISTS superstore.sales_reporting.sales_inbound
                  (
                    rowId  BIGINT  GENERATED ALWAYS AS IDENTITY(START WITH 1   INCREMENT BY 1 ),
                    shipMode STRING,
                    segment STRING,
                    country  STRING  NOT NULL, 
                    city    STRING , 
                    State   STRING , 
                    postalCode INT,
                    Region STRING , 
                    category STRING,
                    subCategory STRING,
                    sales  Decimal(9,2),
                    quantity INT,
                    discount Decimal(9,2),
                    profit Decimal(9,2),
                    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                  )
                  USING DELTA 
                  PARTITIONED  BY (country)
                  TBLPROPERTIES(
                  'delta.feature.allowColumnDefaults' = 'supported',
                  'delta.columnMapping.mode' = 'id'
                  )
                  """
    )
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)
    raise

# COMMAND ----------

# schema creation for the  supplier data 
try:
    _UserSchema = StructType( [
                StructField('shipMode',StringType(), True),
                StructField('segment',StringType(),True),
                StructField('country',StringType(),False),
                StructField('city',StringType(),True),
                StructField('state',StringType(),True),
                StructField('postalCode',IntegerType(),True),
                StructField('region',StringType(),True),
                StructField('category',StringType(),True),
                StructField('subCategory',StringType(),True),
                StructField('sales',DecimalType(9,2),True),
                StructField('quantity',IntegerType(),True),
                StructField('discount',DecimalType(9,2),True),
                StructField('profit',DecimalType(9,2),True),
        ]
    )
except Exception as e:
    Error_message = str(e)
    exceptionLogLoad(NotebookName, Error_message)
    raise

# COMMAND ----------

# Data
try:
    df_sales =(
                spark.read.
                format('csv').
                option("header",True).
                schema(_UserSchema)
                .option('sep',',').
                load('/Volumes/superstore/sales_reporting/inbound/inbound/*.csv')
    )

    # loading the data into the Bronzelayer
    
    df_sales.write.mode('overwrite').saveAsTable('superstore.sales_reporting.sales_inbound')

    dbutils.fs.mv('/Volumes/superstore/sales_reporting/inbound/inbound/SampleSuperstore.csv',
                  '/Volumes/superstore/sales_reporting/inbound/inbound/Archive/'
                  )
    executionLog(NotebookName,"Success",user_name)

except Exception as e:

    Error_message = str(e)
    
    exceptionLogLoad(NotebookName, Error_message)
    raise

