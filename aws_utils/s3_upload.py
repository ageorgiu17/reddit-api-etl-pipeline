import os
from pipeline_utils import get_config_param
import boto3
import configparser


class S3Upload:

    def __init__(self):
        self.config_dict = self.create_config_dict(self.__class__.__name__)
        self.aws_access_key_id = get_config_param("AWS_ACCESS_KEY_ID", config_dict=self.config_dict)
        self.aws_secret_access_key = get_config_param("AWS_SECRET_ACCESS_KEY", config_dict=self.config_dict)
        self.aws_region = get_config_param("AWS_REGION", config_dict=self.config_dict)
        self.raw_bucket_name = get_config_param("RAW_BUCKET_NAME", config_dict=self.config_dict)
        self.s3_client = boto3.client('s3',
                                      region_name=self.aws_region,
                                      aws_access_key_id=self.aws_access_key_id,
                                      aws_secret_access_key=self.aws_secret_access_key,
                                      )

    @staticmethod
    def create_config_dict(name):
        if 'config.ini' in os.listdir():
            print('Creating the config for the current class ...')
            config = configparser.ConfigParser()
            config.read('config.ini')
            return config[name]
        else:
            print("Config file not found")
            return None

    def upload_file(self, file_path, object_name):
        print(f'Uploading {file_path} to {self.raw_bucket_name}/{object_name}')
        self.s3_client.upload_file(file_path, self.raw_bucket_name, object_name)
