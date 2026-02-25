# Databricks notebook source
# MAGIC %sql 
# MAGIC --creating the  required unity catlog and database for  opeartions 
# MAGIC create catalog if not exists superstore;
# MAGIC create database if not exists superstore.sales_reporting;
# MAGIC use catalog superstore;
# MAGIC use database sales_reporting;
# MAGIC CREATE VOLUME IF NOT EXISTS superstore.sales_reporting.Inbound;

# COMMAND ----------

# MAGIC %sql
# MAGIC /**Table creation for the execution logs**/
# MAGIC create table if not exists superstore.sales_reporting.executionLog (
# MAGIC     logId BIGINT generated always AS identity,
# MAGIC     noteBookName varchar(100),
# MAGIC     status varchar(100),
# MAGIC     createAt timestamp default current_timestamp,
# MAGIC     createBy string
# MAGIC   ) using delta
# MAGIC   tblproperties ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC

# COMMAND ----------

# DBTITLE 1,Exception Log Table Creation
# MAGIC %sql
# MAGIC /**Tbale creation for the error logs**/
# MAGIC CREATE TABLE IF NOT EXISTS superstore.sales_reporting.exceptionLog(
# MAGIC     exceptionId  BIGINT GENERATED  ALWAYS as IDENTITY , 
# MAGIC     notebookName  varchar(100),
# MAGIC     error_message  STRING, 
# MAGIC     createdAt timestamp default current_timestamp
# MAGIC )
# MAGIC using delta 
# MAGIC tblproperties(
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported'
# MAGIC )

# COMMAND ----------

# creating the directory for the  inbound volume 
dbutils.fs.mkdirs('/Volumes/superstore/sales_reporting/inbound/inbound')
