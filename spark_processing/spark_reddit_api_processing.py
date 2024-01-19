from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
import os
import pandas as pd


# Create a SparkSession

class SparkRedditAPIProcessing:

    def __init__(self):
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_access_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.spark_app_name = os.getenv('SPARK_APP_NAME')

        self.spark = SparkSession.builder \
            .appName(self.spark_app_name) \
            .getOrCreate()

        self.submission_df = pd.DataFrame()
        self.comments_df = pd.DataFrame()

        print("SPARK SESSION CREATED")

    def set_spark_aws_options(self):
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.aws_access_key_id)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.aws_access_secret_key)

        print('SPARK AWS CONFIGURED')

    def get_spark_raw_dataframes(self):
        self.submission_df = self.spark.read.csv('s3a://bucket_name/hot_submissions_data_timestamp.csv', header=True)
        self.comments_df = self.spark.read.csv('s3a://bucket_name/comments_data_timestamp.csv', header=True)

    @staticmethod
    def create_new_dataframe(df):
        df.withColumn('created_date', from_unixtime('created_utc', 'yyyy-MM-dd')).select(
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
        return df

    def stop_spark_session(self):
        self.spark.stop()

    @staticmethod
    def create_final_dataframe(df1, df2):
        return df1.union(df2)

    def process(self):
        self.set_spark_aws_options()
        self.get_spark_raw_dataframes()
        sub = self.create_new_dataframe(self.submission_df)
        com = self.create_new_dataframe(self.comments_df)

        src_df = self.create_final_dataframe(sub, com)
        self.stop_spark_session()
        return src_df
