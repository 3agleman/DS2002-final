# Databricks notebook source
# Import necessary libraries
import os
import json
import pymongo
import pyspark.pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType

# COMMAND ----------

# Declare global variables
# Replace generic credentials with your own
# Azure MySQL Server Connection Information ###################
jdbc_hostname = "HOSTNAME"
jdbc_port = 3306
src_database = "sakila_dw"

connection_properties = {
  "user" : "UID",
  "password" : "PWD",
  "driver" : "org.mariadb.jdbc.Driver"
}

# MongoDB Atlas Connection Information ########################
atlas_cluster_name = "CLUSTERNAME"
atlas_database_name = "sakila_dw"
atlas_user_name = "UID"
atlas_password = "PWD"

# Data Files (JSON) Information ###############################
dst_database = "sakila_dlh"

base_dir = "dbfs:/FileStore/capstone_data"
database_dir = f"{base_dir}/{dst_database}"

data_dir = f"{base_dir}/sakila_rentals"
batch_dir = f"{data_dir}/batch"
stream_dir = f"{data_dir}/sakila"

rentals_stream_dir = f"{stream_dir}/rentals"

rentals_output_bronze = f"{database_dir}/fact_rentals/bronze"
rentals_output_silver = f"{database_dir}/fact_rentals/silver"
rentals_output_gold   = f"{database_dir}/fact_rentals/gold"

# Delete the Streaming Files ################################## 
dbutils.fs.rm(f"{database_dir}/fact_rentals", True) 

# Delete the Database Files ###################################
dbutils.fs.rm(database_dir, True)

# COMMAND ----------

print(rentals_output_bronze)

# COMMAND ----------

# GLobal functions
def get_mongo_dataframe(user_id, pwd, cluster_name, db_name, collection, conditions, projection, sort):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.mongodb.net/{db_name}"
    
    client = pymongo.MongoClient(mongo_uri)

    '''Query MongoDB, and fill a python list with documents to create a DataFrame'''
    db = client[db_name]
    if conditions and projection and sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection).sort(sort)))
    elif conditions and projection and not sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection)))
    else:
        dframe = pd.DataFrame(list(db[collection].find()))

    client.close()
    
    return dframe

def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.mongodb.net/{db_name}"
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS sakila_dlh CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating the main database we'll be working with here
# MAGIC CREATE DATABASE IF NOT EXISTS sakila_dlh
# MAGIC COMMENT "DS-2002 Capstone Database"
# MAGIC LOCATION "dbfs:/FileStore/capstone_data/sakila_dlh"
# MAGIC WITH DBPROPERTIES (contains_pii = true, purpose = "DS-2002 Capstone");

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Setting up the date table
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_date
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "SERVER", 
# MAGIC   dbtable "dim_date",
# MAGIC   user "UID",   
# MAGIC   password "PWD"  
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE sakila_dlh;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sakila_dlh.dim_date
# MAGIC COMMENT "Date Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/capstone_data/dim_date"
# MAGIC AS SELECT * FROM view_date

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila_dlh.dim_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila_dlh.dim_date LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now for the customer table (I probably should have named the following tables dim_XXXXX instead of just the names, but it works out in the end)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_customer
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "SERVER", 
# MAGIC   dbtable "dim_customer",
# MAGIC   user "UID",    
# MAGIC   password "PWD"  
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE sakila_dlh;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sakila_dlh.customer
# MAGIC COMMENT "Customer Table"
# MAGIC LOCATION "dbfs:/FileStore/lab_data/sakila_dlh/customer"
# MAGIC AS SELECT * FROM view_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila_dlh.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila_dlh.customer LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Film table
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_film
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "SERVER", 
# MAGIC   dbtable "dim_film",
# MAGIC   user "UID",    
# MAGIC   password "PWD"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE sakila_dlh;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sakila_dlh.film
# MAGIC COMMENT "Film Table"
# MAGIC LOCATION "dbfs:/FileStore/lab_data/sakila_dlh/film"
# MAGIC AS SELECT * FROM view_film

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila_dlh.film LIMIT 5

# COMMAND ----------

# Loading from MongoDB now
source_dir = '/dbfs/FileStore/capstone_data'
json_files = {"inventory" : 'dim_inventory.json'
              , "payment" : 'dim_payment.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, atlas_database_name, source_dir, json_files)

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC
# MAGIC val userName = "UID"
# MAGIC val pwd = "PWD"
# MAGIC val clusterName = "CLUSTERNAME"
# MAGIC val atlas_uri = s"mongodb+srv://$userName:$pwd@$clusterName.mongodb.net/?retryWrites=true&w=majority"

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val df_inventory = spark.read.format("com.mongodb.spark.sql.DefaultSource")
# MAGIC .option("spark.mongodb.input.uri", atlas_uri)
# MAGIC .option("database", "sakila_dw")
# MAGIC .option("collection", "inventory").load()
# MAGIC .select("inventory_key","inventory_id","film_id","store_id","last_update")
# MAGIC
# MAGIC display(df_inventory)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_inventory.write.format("delta").mode("overwrite").saveAsTable("sakila_dlh.inventory")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila_dlh.inventory

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val df_payment = spark.read.format("com.mongodb.spark.sql.DefaultSource")
# MAGIC .option("spark.mongodb.input.uri", atlas_uri)
# MAGIC .option("database", "sakila_dw")
# MAGIC .option("collection", "payment").load()
# MAGIC .select("payment_key","payment_id","customer_id","staff_id","rental_id","amount","payment_date","last_update")
# MAGIC
# MAGIC display(df_payment)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_payment.write.format("delta").mode("overwrite").saveAsTable("sakila_dlh.payment")

# COMMAND ----------

# Loading directly from DBFS
rental_json = f"{base_dir}/dim_rental.json"

df_rental = spark.read.format("json").options(header='true', inferSchema='true').load(rental_json)
display(df_rental)

# COMMAND ----------

df_rental = df_rental.drop('_corrupt_record')
df_rental.columns
# https://stackoverflow.com/questions/68764239/cant-drop-column-pyspark-databricks

# COMMAND ----------

df_rental = df_rental.dropna()

# COMMAND ----------

df_rental.write.format("delta").mode("overwrite").saveAsTable("sakila_dlh.rental")

# COMMAND ----------

# Beginning the streaming process, loading the json file(s)
# May or may not read multiple files, seemed kind of iffy on my end. If reading in the three partitioned fact_rentals files doesn't work, just use the main fact_rentals file in GitHub. 
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", rentals_output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load(rentals_stream_dir)
 .createOrReplaceTempView("rentals_raw_tempview"))

# COMMAND ----------

print(rentals_stream_dir)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating the bronze table temporary view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW rentals_bronze_tempview AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM rentals_raw_tempview
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rentals_bronze_tempview

# COMMAND ----------

(spark.table("rentals_bronze_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{rentals_output_bronze}/_checkpoint")
      .outputMode("append")
      .table("fact_rentals_bronze"))
# Pushing it to the system

# COMMAND ----------

# SILVER
(spark.readStream
  .table("fact_rentals_bronze")
  .createOrReplaceTempView("rentals_silver_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rentals_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED rentals_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fact_rentals_silver_tempview AS (
# MAGIC   SELECT r.fact_rental_key,
# MAGIC       r.rental_key,
# MAGIC       r.rental_date_key,
# MAGIC       renD.day_name_of_week AS rental_day_name_of_week,
# MAGIC       renD.day_of_month AS rental_day_of_month,
# MAGIC       renD.weekday_weekend AS rental_weekday_weekend,
# MAGIC       renD.month_name AS rental_month_name,
# MAGIC       renD.calendar_quarter AS rental_quarter,
# MAGIC       renD.calendar_year AS rental_year,
# MAGIC       r.payment_date_key,
# MAGIC       payD.day_name_of_week AS payment_day_name_of_week,
# MAGIC       payD.day_of_month AS payment_day_of_month,
# MAGIC       payD.weekday_weekend AS payment_weekday_weekend,
# MAGIC       payD.month_name AS payment_month_name,
# MAGIC       payD.calendar_quarter AS payment_quarter,
# MAGIC       payD.calendar_year AS payment_year,
# MAGIC       r.return_date_key,
# MAGIC       retD.day_name_of_week AS return_day_name_of_week,
# MAGIC       retD.day_of_month AS return_day_of_month,
# MAGIC       retD.weekday_weekend AS return_weekday_weekend,
# MAGIC       retD.month_name AS return_month_name,
# MAGIC       retD.calendar_quarter AS return_quarter,
# MAGIC       retD.calendar_year AS return_year,
# MAGIC       r.payment_key,
# MAGIC       p.amount AS payment_amount,
# MAGIC       r.film_key,
# MAGIC       f.release_year AS film_release_year,
# MAGIC       f.rental_duration,
# MAGIC       f.rental_rate,
# MAGIC       f.length AS film_length,
# MAGIC       f.replacement_cost AS film_replacement_cost,
# MAGIC       f.rating AS film_rating,
# MAGIC       r.inventory_key,
# MAGIC       i.inventory_id,
# MAGIC       i.store_id,
# MAGIC       r.customer_key,
# MAGIC       c.address_id,
# MAGIC       r.renter_first_name,
# MAGIC       r.renter_last_name,
# MAGIC       r.active
# MAGIC   FROM rentals_silver_tempview AS r
# MAGIC   INNER JOIN sakila_dlh.payment AS p
# MAGIC   ON p.payment_key = r.payment_key
# MAGIC   INNER JOIN sakila_dlh.film AS f
# MAGIC   ON f.film_key = r.film_key
# MAGIC   INNER JOIN sakila_dlh.inventory AS i
# MAGIC   ON i.inventory_key = r.inventory_key
# MAGIC   INNER JOIN sakila_dlh.customer AS c
# MAGIC   ON c.customer_key = r.customer_key
# MAGIC   LEFT OUTER JOIN sakila_dlh.dim_date AS renD
# MAGIC   ON renD.date_key = r.rental_date_key
# MAGIC   LEFT OUTER JOIN sakila_dlh.dim_date AS retD
# MAGIC   ON retD.date_key = r.return_date_key
# MAGIC   LEFT OUTER JOIN sakila_dlh.dim_date AS payD
# MAGIC   ON payD.date_key = r.payment_date_key
# MAGIC )

# COMMAND ----------

(spark.table("fact_rentals_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{rentals_output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_rentals_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_rentals_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold Table example
# MAGIC -- Payment amount and number of rentals by individual customer for rentals per month
# MAGIC CREATE OR REPLACE TABLE sakila_dlh.fact_monthly_rentals_by_customer_gold AS (
# MAGIC   SELECT customer_key AS CustomerID
# MAGIC     , renter_last_name AS LastName
# MAGIC     , renter_first_name AS FirstName
# MAGIC     , payment_month_name AS PaymentMonth
# MAGIC     , COUNT(film_key) AS FilmsRented
# MAGIC     , SUM(payment_amount) AS TotalPayment
# MAGIC   FROM sakila_dlh.fact_rentals_silver
# MAGIC   GROUP BY CustomerID, LastName, FirstName, PaymentMonth
# MAGIC   ORDER BY TotalPayment, FilmsRented DESC);
# MAGIC
# MAGIC SELECT * FROM northwind_dlh.fact_monthly_rentals_by_customer_gold;
# MAGIC
