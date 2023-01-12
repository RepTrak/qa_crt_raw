import argparse
from utils.main import run

parser = argparse.ArgumentParser()
parser.add_argument("--start", help="Specify which stage the processor will start.",
                    type=int,const=0, nargs='?')
parser.add_argument("--stop", help="Bool. False == run the entire processor.",
                    dest='stop', action='store_true')
parser.add_argument("--dont_upload", help="Bool. Trigger = don't upload to redshift.",
                    dest='upload', action='store_false')
parser.add_argument("--dont_roll", help="Bool. Trigger = don't apply 3 month rolling process.",
                    dest='roll', action='store_false')
args = parser.parse_args()


def main(path_, start, stop, upload, roll):
    run(path_, start, stop, upload, roll)


if __name__ == '__main__':
    path_ = 's3://reptrak-perception-data/'
    stages={0:"Begining of Process",
            1:"Cleaning the dataset.",
            2:"Computing Awareness.",
            3:"Computing Scores, Constructs, and rolling data.",
            4:"Uploading computations to S3.",
            5:"Computing Imputations.",
            6:"Uploading Imputations to RedShift",
            7:"Cleaning files."}
    if args.start:
        if args.start>7 or args.start<0:
            raise ValueError("Error: the start argument must be between [0-7]")
    else:
        args.start = 0
    str1 = "Starting at stage {}\n{} ".format(args.start,stages[args.start])
    if not args.stop:
        str1 += "and not halting."
    else:
        str1 += "and halting."
    if args.upload:
        str1 += "\nUploading."
    else:
        str1 += "\nNot uploading."
    if args.roll:
        str1 += "\nApplying 3 months rolling."
    else:
        str1 += "\nNot rolling data."
    print("\n\n====================================")
    print("**       PROCESSOR STARTING       **")
    print(str1)
    print("====================================\n")
    main(path_, args.start, args.stop, args.upload, args.roll)
