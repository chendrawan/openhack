# Databricks notebook source
df1 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLInputSales/Orders.txt", header = True)
display(df1)

# COMMAND ----------

df2 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLInputSales/OrderDetails.txt", header = True)
display(df2)

# COMMAND ----------

df2 = df2.select('OrderDetailID','OrderID','MovieID','Quantity','UnitCost','LineNumber')

# COMMAND ----------

df3 = df1.join(df2, 'OrderID')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df3 = df3.withColumn("SourceSystemID", lit(1))

# COMMAND ----------

df3 = df3.withColumn("UniqueOrderID", (df3.SourceSystemID + df3.OrderDetailID))

# COMMAND ----------

df3 = df3.select(df3.UnitCost.cast("float"), 
df3.Quantity.cast("integer"), 
df3.LineNumber.cast("integer"),
df3.OrderDate.cast("timestamp"),
df3.CreatedDate.cast("timestamp"),
df3.UpdatedDate.cast("timestamp"))

# COMMAND ----------

df3 = df3.coalesce(1) 
df3.write.format("csv").mode('overwrite').option("header", "true").save('abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/OutputCSVs/SalesOrders.csv') 