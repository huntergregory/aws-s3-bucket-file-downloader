from argparse import ArgumentParser
import re
import requests
import xml.etree.ElementTree as ET
import urllib.request
import os
import shutil
import zipfile
from tqdm import tqdm

## Argument Parsing
parser = ArgumentParser()
parser.add_argument("url", help="Bucket URL (show's an XML file in your browser).") 
parser.add_argument("-D", dest="should_download", required=False, action="store_true", default=False, help="If turned on, downloads files to the current directory. Otherwise, filenames and sizes are printed only.")
parser.add_argument("--dir", required=False, default="./", help="Directory to save downloads.")
parser.add_argument("--max-filesize-KB", "-m", type=int, required=False, default=1000, help="Maximum file size for each download in KiloBytes. Default is 1000 KB.")
parser.add_argument("--download-capacity", "-c", type=int, required=False, default=250, help="Maximum number of MegaBytes for total download (sum of all files). Default is 250 MB.")
parser.add_argument("--filetypes", "-t", required=False, help="Comma-separated file types to include (leave out the '.')")
parser.add_argument("--filename-pattern", "-p", required=False, help="Regular expression for filenames to include. The file extension is handled automatically if you specify filetypes. Pattern must match the filename completely. For example, with a filetype of 'txt', the pattern 'abc' wouldn't match 'abc1.txt', but 'abc[0-9]' would match.")
parser.add_argument("--verbose", "-v", required=False, action="store_true", default=False, help="Prints files found but not matched.")
parser.add_argument("--silence-large-files", "-s", required=False, action="store_true", default=False, help="Silences printing of matched files with unknown or to large a size.")
parser.add_argument("--unzip-zips", "-z", required=False, action="store_true", default=False, help="Unzips zip files and deletes the original zip files. WARNING: Never extract archives from untrusted sources without prior inspection.")

args = parser.parse_args()
print("Arguments:")
print(args)
print()
if args.should_download:
    if not os.path.isdir(args.dir):
        os.mkdir(args.dir)
    os.chdir(args.dir)

## Filename Regex Helper
if args.filetypes:
    start = args.filename_pattern if args.filename_pattern else ".*"
    filename_regex_strings = ["{}\.{}$".format(start, filetype) for filetype in args.filetypes.split(",")]
elif args.filename_pattern:
    filename_regex_strings = [args.filename_pattern]
else:
    filename_regex_strings = [".*"]
filename_regexes = [re.compile(regex_string) for regex_string in filename_regex_strings]

print("Filename regular expressions: {}".format(filename_regex_strings))

def is_good_filename(filename):
    return any([filename_regex.match(filename) is not None for filename_regex in filename_regexes])

## Looping through XML Tree
def trim_xml_tag(tag):
    right_bracket = tag.find("}")
    return tag[right_bracket+1:]

# url = "https://s3.amazonaws.com/biketown-tripdata-public"
result = requests.get(args.url)
status_message = "HTTP GET status code: {}".format(result.status_code)
if result.status_code != 200:
    raise RuntimeError(status_message)
print(status_message)
print()

matched_files = []
large_files = []
unmatched_files = []
found_files = False
root = ET.fromstring(result.text)
for child in root:
    if trim_xml_tag(child.tag) == "Contents":
        filename = ""
        filesize = -1
        haveFilename = False
        haveFilesize = False
        for grandchild in child:
            trimmed_tag = trim_xml_tag(grandchild.tag)
            if not haveFilename and trimmed_tag == "Key":
                filename = grandchild.text
            if not haveFilesize and trimmed_tag == "Size":
                filesize = int(grandchild.text)
            haveFilename = len(filename) > 0
            haveFilesize = filesize > 0
            if haveFilename and haveFilesize:
                break

        if haveFilename:
            found_files = True
            fileBundle = (filename, filesize)
            if is_good_filename(filename):
                if haveFilesize and filesize < args.max_filesize_KB * 1000:
                    matched_files.append(fileBundle)
                else:
                    large_files.append(fileBundle)
            elif args.verbose:
                unmatched_files.append(fileBundle)

## Print Information about Results
def bytes_to_MB(count):
    return round(count / 1000 / 1000, 2)

def get_total_size(bundles):
    return bytes_to_MB(sum([size for _, size in bundles]))
    
total_download_size = get_total_size(matched_files)
over_capacity = total_download_size > args.download_capacity
if over_capacity:
    raise RuntimeError("Prevented download of {} files since their combined size is {} MB. Capacity set to {} MB.".format(len(matched_files), total_download_size, args.download_capacity))

def print_file_bundle(bundle):
    name, size = bundle
    size = -1 if size == -1 else bytes_to_MB(size) 
    print("{:25} | {:8} MB".format(name, size))

if len(matched_files) > 0:
    print("{} files were matched with a combined size of {} MB:".format(len(matched_files), total_download_size))
    for bundle in matched_files:
        print_file_bundle(bundle)
    print()
elif not found_files:
    print("Couldn't find files at {}".format(args.url))
elif not len(large_files) > 0:
    print("No files matched the criteria.")

if len(large_files) > 0 and not args.silence_large_files:
    print("{} files were matched but had unknown or too large a size. They had a combined size of {} MB: ".format(len(large_files), get_total_size(large_files)))
    for bundle in large_files:
        print_file_bundle(bundle)
    print()

if args.verbose and len(unmatched_files) > 0:
    print("{} files were not matched. They had a combined size of {} MB: ".format(len(unmatched_files), get_total_size(unmatched_files)))
    for bundle in unmatched_files:
        print_file_bundle(bundle)
    print()

## Downloading
dot_regex = re.compile("\.")
def get_good_filename(name):
    splits = re.split("\.", name)
    if len(splits) == 1: 
        extension = ""
        start = name
    else:
        extension = "." + splits[-1]
        start = name[:-len(extension)]

    if not os.path.exists(name):
        return name, extension

    for k in range(1,10):
        new_name = "{}({}){}".format(start, k, extension)
        if not os.path.exists(new_name):
            return new_name, extension
    return None, None

if args.should_download and len(matched_files) > 0:
    print("Beginning Downloads")
    clean_url = args.url
    if clean_url[-1] != "/":
        clean_url += "/"
    for name, size in tqdm(matched_files):
        download_url = clean_url + name
        good_name, extension = get_good_filename(name)
        print("extension: {}".format(extension))
        if not good_name:
            print("Couldn't download {} because the file and similar names for it already exist.".format(name))
            continue
        if good_name != name:
            print("{} already exists. Renaming downloaded file to {}".format(name, good_name))
        try:
            with urllib.request.urlopen(download_url) as response, open(good_name, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
                if extension == ".zip" and args.unzip_zips:
                    with zipfile.ZipFile(good_name, 'r') as zip_ref:
                        zip_ref.extractall()
                        os.remove(good_name)
        except Exception as e:
            print("[WARN] Encountered exception while downloading {}: {}".format(good_name, e))

    print("Done downloading.")
