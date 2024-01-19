from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lower, lit, upper, initcap, length, from_unixtime, substring, length, expr, \
    sequence
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number
import pyspark.sql.functions as sf

# Create a SparkSession
spark = SparkSession.builder \
   .appName("My App") \
   .getOrCreate()

print("SPARK SESSION CREATED")

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "access_key")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "secret_access_key")
# spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
# spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
# spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "eu-central-1.amazonaws.com")
print('SPARK AWS CONFIGURED')

df1 = spark.read.csv('s3a://bucket_name/hot_submissions_data_timestamp.csv', header=True)
df2 = spark.read.csv('s3a://bucket_name/comments_data_timestamp.csv', header=True)

print('READ THE DATAFRMAE')
print('TRYING TO PROCESS THE DATA')


sub = df1.withColumn('created_date', from_unixtime('created_utc', 'yyyy-MM-dd')).select(
        col('author').cast('string'),
        col('author_flair_text').cast('string'),
        col('post_text').cast('string').alias('post_text'),
        col('likes').cast('int'),
        col('subreddit').cast('string'),
        col('subreddit_id').cast('string'),
        col('parent_id').cast('string'),
        col('created_date').cast('string'),
        col('score').cast('int'),
        col('post_url').cast('string').alias('post_url')).drop('_c0')
com = df2.withColumn('created_date', from_unixtime('created_utc', 'yyyy-MM-dd')).select(
        col('author').cast('string'),
        col('author_flair_text').cast('string'),
        col('post_text').cast('string').alias('post_text'),
        col('likes').cast('int'),
        col('subreddit').cast('string'),
        col('subreddit_id').cast('string'),
        col('parent_id').cast('string'),
        col('created_date').cast('string'),
        col('score').cast('int'),
        col('post_url').cast('string').alias('post_url')).drop('_c0')
src_df = sub.union(com)

print(f'SHOWING THE LENGHTH OF SUB DATAFRAME: {sub.count()}')
print(f'SHOWING THE LENGHTH OF COM DATAFRAME: {com.count()}')
print(f'SHOWING THE LENGHTH OF FINAL DATAFRAME: {src_df.count()}')

print(f'SHOWING THE COLUMNS OF SUB DATAFRAME: {sub.columns}')
print(f'SHOWING THE COLUMNS OF COM DATAFRAME: {com.columns}')
print(f'SHOWING THE COLUMNS OF FINAL DATAFRAME: {src_df.columns}')


print(f'SHOWING THE SUB DATAFRAME!: {sub.show()}')
print(f'SHOWING THE COMM DATAFRAME!: {com.show()}')
print(f'SHOWING THE FINAL DATAFRAME!: {src_df.show()}')

spark.stop()