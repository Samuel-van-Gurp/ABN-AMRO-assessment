from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("costumer_data").getOrCreate()
spark = SparkSession.builder.master("local").appName("costumer_data").getOrCreate()

print("done")