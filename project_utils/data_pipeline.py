from data_export import DataExport


def main():
    data_export = DataExport()
    data_export.export_data()


if __name__ == '__main__':
    main()

    # TODO - upload the files to Amazon S3
