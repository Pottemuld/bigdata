from pyspark.sql.functions import *
from pyspark.sql import functions as F, DataFrameWriter, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def write_to_postgres(df, epochId):
    df.write.mode('append').jdbc("jdbc:postgresql://10.123.252.210:5432/bigdata", "public.data", properties={"user": "postgres", "password": "Test"})

spark = SparkSession \
    .builder \
    .appName("BigData") \
    .getOrCreate()

schema = StructType([StructField("date", StringType(), False), StructField("location", StringType(), False)])

input_df = (spark
            .readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', '10.123.252.210:9092')
            .option('subscribe', 'tweet.date-location.source')
            .load()
            .select(from_json(F.col('value').cast("string"), schema).alias('passed_data')))


counted = input_df.groupBy(F.col('passed_data.location'), F.col('passed_data.date')).count()

batched = counted.writeStream.outputMode('update').foreachBatch(write_to_postgres).start()

#out = counted.writeStream.outputMode('complete').format('console').start()

batched.awaitTermination()


