#from pyspark.shell import spark
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import pyspark
from pyspark.sql.types import StructType, StructField, StringType


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("BigData") \
    .getOrCreate()

schema = StructType([StructField("date", StringType(), False), StructField("location", StringType(), False)])
#schema = StructType().add('data', StringType()).add('location', StringType())

input_df = (spark
            .readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'localhost:9092')
            .option('subscribe', 'tweet.date-location.source')
            .load()
            .select(from_json(F.col('value').cast("string"), schema).alias('passed_data')))


input_df.printSchema()


count = input_df.groupBy(F.col('passed_data.location')).count()

out = count.writeStream.outputMode('complete').format('console').start()

out.awaitTermination()


#streaming_counts_df = (streaming_input_df\
#    .groupBy(streaming_input_df.date, streaming_input_df.location)\
#    .count())

