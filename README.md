# Spark-Coding-Assignment

df1 = spark.read.option("delimiter","|").csv("C:/Users/Prafulla/Downloads/startup.csv",header=True).show()

df1.show()

df2 = spark.read.parquet("C:/Users/Prafulla/Downloads/consumerInternet.parquet")

df2.write.csv("consumerInternet.csv")

df2.show()

df = df1.union(df2).show()

df = df1.union(df2).show()



## How many startups are there in Pune City?
df3=df[df["City"]=="Pune"]
df3.show()
df3.count()

## How many startups in Pune got their Seed/ Angel Funding?


df[df["InvestmentnType"]=="Seed/ Angel Funding"].count()

## What is the total amount raised by startups in Pune City? Hint - use regex_replace to get rid of null

df.groupBy("Amount_in_USD").sum().show()


## What are the top 5 Industry_Vertical which has the highest number of startups in India?

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("Industry_Vertical").orderBy("Amount_in_USD")
df.withColumn("row_number",row_number().over(windowSpec)).show(truncate=False)

## Find the top Investor(by amount) of each year.
df.groupBy("Investors_Name").max().show()


