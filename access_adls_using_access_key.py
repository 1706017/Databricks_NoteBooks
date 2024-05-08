
#  Access Azure Data lake using access keys and hiding the credentials via using Key vault + Secret scope
#  =======================================================================================================
# 
#  1.set the spark config fs.azure.account.key
#  2.List files from demo container
#  3.Read data from circuits.csv file


#Here we are passing the credentials via azure vault key + databricks secret scope so that credentials are never exposed 
azure_datalake_key = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1dl-account-key') 


#Here we are passing the second paramter as variable containing key we can also directly pass the key value here but not recomended 
spark.conf.set("fs.azure.account.key.azureformula12dl.dfs.core.windows.net",azure_datalake_key)

display(dbutils.fs.ls("abfss://demo@azureformula12dl.dfs.core.windows.net"))

# To read the data from container in Azure Datalake storage account 

display(spark.read.csv("abfss://demo@azureformula12dl.dfs.core.windows.net/circuits.csv"))
