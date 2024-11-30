from pyspark.sql import SparkSession, functions as F
import os
import pandas as pd

caminho_csv = "/app/MOCK_DATA_CUSTOMER.csv"

spark = SparkSession.builder \
    .appName("Customer Cleaning Analysis") \
    .master("local") \
    .getOrCreate()

df = spark.read.csv(caminho_csv, header=True)

df_clean = df.dropna()
df_format = df_clean.withColumn("full_name", F.concat_ws(" ", df_clean["first_name"], df_clean["last_name"]))
df_final = df_format.drop("first_name", "last_name")

pasta_raiz = "/app" 

parquet_path = f"{pasta_raiz}/customer_data_cleaned.parquet"
df_final.write.mode("overwrite").parquet(parquet_path)

json_path = os.path.join(pasta_raiz, "customer_data_cleaned.json")
df_final.write.mode("overwrite").json(json_path)

csv_path = os.path.join(pasta_raiz, "customer_data_cleaned.csv")
df_final.write.mode("overwrite").csv(csv_path, header=True)

xlsx_path = os.path.join(pasta_raiz, "customer_data_cleaned.xlsx")
df_final.toPandas().to_excel(xlsx_path, index=False)

