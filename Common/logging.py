# Databricks notebook source
# DBTITLE 1,function  to  save the execution log
#  method creation for logging the  information into the  execution log 

def executionLog(noteBookname ,status ,createdBy):
  spark.sql(f"""
       INSERT INTO   superstore.sales_reporting.executionLog
       (
        noteBookName,
        status,
        createAt,
        createBy
       )
       values 
       (
         '{noteBookname}',
         '{status}',
         current_timestamp(),
        '{createdBy}'
       )
      """
  )


# COMMAND ----------

# DBTITLE 1,function to save the Exception log
def exceptionLogLoad(noteBookName ,errorMessage):
  errorMessage = errorMessage.replace("'","''")
  spark.sql(f"""           
              Insert into  superstore.sales_reporting.exceptionLog
              (
                  noteBookName, 
                   error_message 
              )
              values 
            (

                '{noteBookName}',
                '{errorMessage}'
            )
              """
    )
