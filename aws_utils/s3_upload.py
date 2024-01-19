import os

import boto3


class S3Upload:

    def __init__(self):
        self.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.environ.get('AWS_REGION')
        self.s3_bucket = os.environ.get('S3_BUCKET')
        self.s3_client = boto3.client('s3',
                                      region_name=self.aws_region,
                                      aws_access_key_id=self.aws_access_key_id,
                                      aws_secret_access_key=self.aws_secret_access_key,
                                      )

    def upload_file(self, file_path, object_name):
        self.s3_client.upload_file(file_path, self.s3_bucket, object_name)


def main():
    s3 = S3Upload()
    print('uplodaing...')
    s3.upload_file('./files/comments_data.csv', 'test_upload')
    print('done')


if __name__ == '__main__':
    main()
