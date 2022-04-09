import pandas as pd
import json
import time

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkSQL").master('local').getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '300')
spark.conf.set('spark.default.parallelism', '300')

df = spark\
    .read\
    .option("inferSchema", "true")\
    .option('header', 'true')\
    .csv('./rawdata/2019-Oct.csv')    


# 카프카 토픽

start = time.time()

randInt = 100
 
 
randSplitList = [1/randInt] * randInt
temp = df.randomSplit(randSplitList, 333)
for idx in range(len(temp)):
    # temp[idx].repartition(1).write.format("parquet").mode('overwrite').save(f"./rawdata/oct_split/{idx}.parquet")
    # temp[idx].write.format("parquet").mode('overwrite').save(f"./rawdata/oct_split/2019-Oct-{idx}.parquet")
    temp[idx].write.format("parquet").mode('overwrite').save(f"./rawdata/test-{idx}.parquet")
    
print("elapsed :", time.time() - start)
