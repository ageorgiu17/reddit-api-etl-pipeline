from download_data.data_csv import RawDataCsv
import pandas as pd


class DataExport:

    def __init__(self, timestamp='testing'):
        self.raw_data = RawDataCsv()
        self.df_comments_data = pd.DataFrame()
        self.df_new_submission_data = pd.DataFrame()
        self.df_hot_submission_data = pd.DataFrame()
        self.TIMESTAMP = timestamp

    def create_dataframes(self):
        self.df_comments_data = self.raw_data.creat_comments_csv()
        self.df_new_submission_data = self.raw_data.create_submission_csv("new")
        self.df_hot_submission_data = self.raw_data.create_submission_csv("hot")

    def export_data(self):
        self.create_dataframes()
        self.df_comments_data.to_csv(f'./files/comments_data_{self.TIMESTAMP}.csv')
        self.df_new_submission_data.to_csv(f'./files/new_submissions_data_{self.TIMESTAMP}.csv')
        self.df_hot_submission_data.to_csv(f'./files/hot_submissions_data_{self.TIMESTAMP}.csv')


def main():
    data_export = DataExport()
    data_export.export_data()


if __name__ == "__main__":
    main()
