# Databricks notebook source
# MAGIC %md
# MAGIC #Access Dataframes using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objectives
# MAGIC ###1.Create temporary View on dataframes
# MAGIC ###2.Access the view from sql cell 
# MAGIC ###3.Access the view from python cell

# COMMAND ----------

race_results_df = spark.read.parquet("/mnt/azureformula12dl/presentation/race_results")

# COMMAND ----------

#Here we are creating a temporary view
race_results_df.createTempView("v_race_results_new")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results_new;
# MAGIC

# COMMAND ----------

display(spark.sql("SELECT * FROM v_race_results_new"))
