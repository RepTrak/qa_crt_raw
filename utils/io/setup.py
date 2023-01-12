import os
import subprocess
import s3fs

from utils.io.io import transfer_from_s3, open_codebooks

def get_logistics(PATH):
    # -------------------------
    # Make sure tmp directory exists
    if not os.path.exists('./tmp/'):
        os.makedirs('./tmp/')

    # -------------------------
    s3 = s3fs.S3FileSystem()
    # These are debugging files generated throughout the script. Will be deleted later.
    all_paths = {'input_path':'Raw_Data/Zip_Files/Input/',
                 'midpoint':'./tmp/MIDPOINT-SAVE.csv',
                 'pre_awareness':'./tmp/PRE-AWARENESS.csv',
                 'pre_rolling': './tmp/PRE-ROLLING-SAVE.csv',
                 'post_rolling': './tmp/POST-ROLLING-SAVE.csv',
                 'post_rolling2': './tmp/Post-Rolled.csv',
                 'tmp_file': './tmp/Temp_File_for_Merging.csv',
                 }

    input_path = PATH + "Raw_Data/Zip_Files/Input/"
    print("\n---\nReading file from: {}".format(input_path))

    # -------------------------
    # Copy files over
    filename_with_path = "s3://" + [x for x in s3.ls(input_path) if x.endswith(".zip")][0]
    all_paths['filename_with_path'] = filename_with_path
    filename = filename_with_path.split("/")[-1]
    all_paths['filename'] = filename
    archive_path = PATH + "Raw_Data/Zip_Files/Archive/" + filename
    all_paths['archive_path'] = archive_path
    tmp_path = './tmp/{}'.format(filename)
    all_paths['tmp_path'] = tmp_path
    export_from_s3_cmd = 'aws s3 cp {} {}'.format(filename_with_path.replace(" ", "\ "),
                                                  tmp_path.replace(" ", "\ "))
    print(f"Tempoary processing file location: {tmp_path}")

    all_paths['complete_filename'] = "~/" + filename.split(".csv")[0] + "_Final Results.csv"

    # -------------------------
    # Get Codebook + Constructs
    logistics = open_codebooks(2022)
    codebook = logistics[0]
    constructs = logistics[1]
    return([all_paths,codebook,constructs,export_from_s3_cmd])
