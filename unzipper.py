from argparse import ArgumentParser
import os
import zipfile
from tqdm import tqdm

parser = ArgumentParser()
parser.add_argument("dir") 
args = parser.parse_args()

with os.scandir(args.dir) as files:
    for file in tqdm(sorted(files, key=lambda file: file.name)):
        if file.name.endswith(".zip") and file.is_file():
            try:
                with zipfile.ZipFile(file.path, 'r') as zip_ref:
                    zip_ref.extractall(args.dir)
                    os.remove(file.path)
            except Exception as e:
                print("Encountered exception while downloading {}: {}".format(file.name, e))