from data_csv import RawDataCsv


def main():
    data = RawDataCsv()
    df_comments_data = data.creat_comments_csv()
    df_new_submission_data = data.create_new_submission_csv()
    df_hot_submission_data = data.create_hot_submissions_csv()

    df_comments_data.to_csv('./files/comments_data.csv')
    df_new_submission_data.to_csv('./files/new_submissions_data.csv')
    df_hot_submission_data.to_csv('./files/hot_submissions_data.csv')


if __name__ == '__main__':
    main()