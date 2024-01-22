from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
import os
import pandas as pd
import configparser
from pipeline_utils import get_config_param


# Create a SparkSession

class SparkRedditAPIProcessing:

    def __init__(self):
        self.config_dict = self.create_config_dict(self.__class__.__name__)
        self.aws_access_key_id = get_config_param("AWS_ACCESS_KEY_ID", config_dict=self.config_dict)
        self.aws_access_secret_key = get_config_param("AWS_SECRET_ACCESS_KEY", config_dict=self.config_dict)
        self.spark_app_name = get_config_param("SPARK APP NAME", config_dict=self.config_dict)
        self.submission_df_path = get_config_param("SUBMISSION_DF", config_dict=self.config_dict)
        self.comments_df_path = get_config_param("COMMENTS_DF", config_dict=self.config_dict)
        self.final_df_path = get_config_param("FINAL_DF_PATH", config_dict=self.config_dict)

        self.spark = SparkSession.builder \
            .appName(self.spark_app_name) \
            .getOrCreate()

        self.submission_df = pd.DataFrame()
        self.comments_df = pd.DataFrame()

        print("SPARK SESSION CREATED")

    @staticmethod
    def create_config_dict(name):
        if 'local_config.ini' in os.listdir():
            print('Creating the config for the current class ...')
            config = configparser.ConfigParser()
            config.read('local_config.ini')
            return config[name]
        else:
            print("Config file not found")
            return None

    def set_spark_aws_options(self):
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.aws_access_key_id)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.aws_access_secret_key)

        print('SPARK AWS CONFIGURED')

    def get_spark_raw_dataframes(self):
        self.submission_df = self.spark.read.csv(self.submission_df_path, header=True)
        self.comments_df = self.spark.read.csv(self.comments_df_path, header=True)

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

    def write_to_s3(self, df):
        df.write.mode('overwrite').option("header", "true").csv(self.final_df_path)

    def process(self):
        self.set_spark_aws_options()
        self.get_spark_raw_dataframes()
        sub = self.create_new_dataframe(self.submission_df)
        com = self.create_new_dataframe(self.comments_df)

        df = self.create_final_dataframe(sub, com)
        self.write_to_s3(df)
        self.stop_spark_session()
        return df


if __name__ == '__main__':
    spark_process = SparkRedditAPIProcessing()
    src_df = spark_process.process()
    print(f'SHOWING THE LENGTH OF FINAL DATAFRAME: {src_df.count()}')
    print(f'SHOWING THE COLUMNS OF FINAL DATAFRAME: {src_df.columns}')
    print(f'SHOWING THE FINAL DATAFRAME!: {src_df.show()}')
