import argparse, time
from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--run", default=f"smoke{int(time.time())}", help="unique test name")
run = p.parse_args().run

spark = SparkSession.builder.appName(f"stream_smoke_{run}").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

table = f"iceberg_catalog.fraud.smoke_stream_{run}"
ckpt = f"hdfs://192.168.193.59:9000/checkpoints/smoke_{run}"

spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (timestamp TIMESTAMP, value BIGINT) USING iceberg")

df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
q = (df.writeStream.format("iceberg").outputMode("append")
       .trigger(processingTime="5 seconds")
       .option("path", table).option("checkpointLocation", ckpt).start())

steps = 6
for i in range(1, steps + 1):
    time.sleep(5)
    print(f"[{i}/{steps}] streaming to Iceberg ...", flush=True)
q.stop()

n = spark.sql(f"SELECT count(*) FROM {table}").first()[0]
print(f"RESULT rows written = {n}  (expected > 0)")
print(f"RESULT checkpoint = {ckpt}")
spark.sql(f"DROP TABLE {table}")
spark.stop()
