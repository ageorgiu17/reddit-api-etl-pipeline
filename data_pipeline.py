from datetime import datetime

from project_utils.data_export import DataExport
from aws_utils.s3_upload import S3Upload
import os


def main():
    TIMESTAMP = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    data_export = DataExport()
    data_export.export_data()
    DATA_DIR = "/Users/andreigeorgiu/Projects/ETL_Pipelines/reddit_data"

    s3_upload = S3Upload()
    for file in os.listdir(f"{DATA_DIR}/files"):
        if file.endswith(".csv"):
            print("Processing")
            file_path = f"{DATA_DIR}/files/{file}"
            s3_upload.upload_file(file_path, f"{file.strip('.csv')}_testing.csv")


if __name__ == '__main__':
    main()

    # TODO - process the data using spark
