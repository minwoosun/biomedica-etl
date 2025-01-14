import os
import ftplib
import argparse
from pmc_oa.download_batch import download_media_from_batch_of_filelists, create_subset_based_on_file_diff, compare_csv_row_counts

# Parse arguments
parser = argparse.ArgumentParser(description="Download and preprocess PMC OA file list")
parser.add_argument('--data_dir', type=str, required=False, default='./data', help="Path to the data directory")
parser.add_argument('--license', type=str, required=True, choices=['commercial', 'noncommercial', 'other', 'toy'], help="Type of license for the article: commercial, noncommercial, or other.")
parser.add_argument('--filelist_batch_dir', type=str, required=True, help="Path to directory with batched file lists")
parser.add_argument('--batch_idx', type=int, required=True, help="batch index")

args = parser.parse_args()
data_dir = args.data_dir
filelist_batch_dir = args.filelist_batch_dir
batch_idx = args.batch_idx
license = args.license 

# check progress
batch_filelist_fixed =  filelist_batch_dir + "filelist_" + license + "_batch_" + str(batch_idx) + "_fixed.csv" 
completed_list = data_dir + "/log_completed/" + license + "_complete_batch_" + str(batch_idx) + ".csv" 

if os.path.exists(completed_list):
    # check if nrows match
    complete_status = compare_csv_row_counts(batch_filelist_fixed, completed_list)
else:
    # this means first attempt
    complete_status = False

if complete_status:
    # update central file that keeps track of batches??
    print(f"Batch {batch_idx} complete")

else: 
    if os.path.exists(completed_list):
        # if not complete, create filelist subset and continue download
        output_file_path = filelist_batch_dir + "filelist_" + license + "_batch_" + str(batch_idx) + ".csv" 
        create_subset_based_on_file_diff(batch_filelist_fixed, completed_list, output_file_path)

    # initialize ftp
    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login("anonymous", "")

    # process batch i
    download_media_from_batch_of_filelists(data_dir = data_dir, 
                                           filelist_batch_dir = filelist_batch_dir, 
                                           batch_index = batch_idx, 
                                           license=license,
                                           ftp = ftp)

    ftp.quit()