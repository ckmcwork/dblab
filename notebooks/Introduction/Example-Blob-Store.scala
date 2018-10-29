// Databricks notebook source
val blobcontainer = dbutils.widgets.get("containername")
val blobaccount = dbutils.widgets.get("accountname")
val blobaccountkey = dbutils.widgets.get("accountkey")
val bloburl = "wasbs://" + blobcontainer + "@" + blobaccount + ".blob.core.windows.net"
val blobconf = "fs.azure.account.key." + blobaccount + ".blob.core.windows.net"

// COMMAND ----------

dbutils.fs.mount(
  source = bloburl,
  mountPoint = "/mnt/blobx2",
  extraConfigs = Map(blobconf -> blobaccountkey))

// COMMAND ----------

// MAGIC %fs ls /mnt/blobx2

// COMMAND ----------

// MAGIC %scala 
// MAGIC val airports = sqlContext.read.format("csv")
// MAGIC   .option("header", "true")
// MAGIC   .option("inferSchema", "true")
// MAGIC   .load("/mnt/blobx2/airports.csv")
// MAGIC 
// MAGIC display(airports)

// COMMAND ----------

// MAGIC %python
// MAGIC airportsPython = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('/mnt/blobx2/airports.csv')
// MAGIC 
// MAGIC display(airportsPython)

// COMMAND ----------

// MAGIC %r 
// MAGIC require("SparkR")
// MAGIC airportsR <- read.df("/mnt/blobx2/airports.csv", "csv", header="true", inferSchema = "true")
// MAGIC 
// MAGIC display(airportsR)

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table if exists airports_sql;
// MAGIC create table if not exists airports_sql
// MAGIC using csv
// MAGIC options (path "/mnt/blobx2/airports.csv", header "true", mode "FAILFAST");
// MAGIC   
// MAGIC select * from airports_sql limit 10;

// COMMAND ----------

// MAGIC %python
// MAGIC airportsPython.write.mode("overwrite").parquet("/mnt/blobx2/airports-parquet/")

// COMMAND ----------

