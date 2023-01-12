import boto3
import s3fs
import os


def clean_up(zip_filenames, paths_dic):
    SPSS_csv_res_path = "SPSS_CSV_Files/" + zip_filenames[0][2:]
    SPSS_spss_res_path = "SPSS_CSV_Files/" + zip_filenames[1][2:]

    s3 = boto3.resource('s3')
    BUCKET = "reptrak-perception-data"

    print("Uploading {} to {}".format(zip_filenames[0], SPSS_csv_res_path))
    s3.Bucket(BUCKET).upload_file(zip_filenames[0], SPSS_csv_res_path)
    print("Uploading {} to {}".format(zip_filenames[1], SPSS_spss_res_path))
    s3.Bucket(BUCKET).upload_file(zip_filenames[1], SPSS_spss_res_path)

    print("Archiving data file from {} to {}".format(paths_dic['filename_with_path'],
                                                     paths_dic['archive_path']))
    s3 = s3fs.S3FileSystem(anon=False)
    s3.mv(paths_dic['filename_with_path'], paths_dic['archive_path'])
    print("Deleting temp files.")

    for k, path in paths_dic.items():
        if (path.endswith('.csv') or path.endswith('.zip')) and (path.startswith('./tmp/')):
            if os.path.isfile(path):
                print("Deleting: {}".format(path))
                os.remove(path)
    print("Data has completely processed.")
    print("------------------------------")