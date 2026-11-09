from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FraudMonitoringSetupTest")
    .getOrCreate()
)

print("Spark Version:", spark.version)

data = [
    (1, "Account_A", 50000),
    (2, "Account_B", 75000),
    (3, "Account_C", 25000)
]

df = spark.createDataFrame(
    data,
    ["transaction_id", "account", "amount"]
)

df.show()

print("Total transactions:", df.count())

print("Total amount:",
      df.agg({"amount": "sum"}).collect()[0][0])

print("SPARK TEST SUCCESSFUL")

spark.stop()
