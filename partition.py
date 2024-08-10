from pyspark.sql import SparkSession

import time



output_path=r"C:\Users\ACER\OneDrive\Desktop\spark data\partiton_data_30-7-24"

spark = SparkSession.builder \
    .appName("SQL Migration") \
    .getOrCreate()


data=r"C:\Users\ACER\OneDrive\Desktop\spark\basic_project\data.csv"
df=spark.read.csv(data,header='true',inferSchema='true')

df.show(5)

df.write \
    .mode("overwrite") \
    .partitionBy("age","gender") \
    .parquet(output_path)



time.sleep(120)