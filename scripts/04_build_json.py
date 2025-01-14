import os
import time
import argparse
from pmc_oa.build_json import build_json
from pmc_oa.download_batch import create_subset_based_on_file_diff, compare_csv_row_counts

# Start measuring time
start_time = time.time()

parser = argparse.ArgumentParser(description="Build json for batch of filelist")
parser.add_argument('--data_dir', type=str, required=False, default='./data', help="Path to the data directory")
parser.add_argument('--license', type=str, required=True, choices=['commercial', 'noncommercial', 'other', 'toy'], help="Type of license for the article: commercial, noncommercial, or other.")
parser.add_argument('--filelist_batch_dir', type=str, required=True, help="Path to directory with batched file lists")
parser.add_argument('--batch_idx', type=int, required=True, help="batch index")
parser.add_argument('--batch_size', type=int, default=200, required=False, help="batch size; 200 matches entrez max")

args = parser.parse_args()
data_dir = args.data_dir
filelist_batch_dir = args.filelist_batch_dir
batch_idx = args.batch_idx
license = args.license 
batch_size = args.batch_size

# set paths 
batch_filelist_fixed = filelist_batch_dir + "filelist_" + license + "_batch_" + str(batch_idx) + "_fixed.csv" 
file_list_path = filelist_batch_dir + "filelist_" + license + "_batch_" + str(batch_idx) + "_fixed.csv" 
completed_list = data_dir + "/log_json/json_" + license + "_complete_batch_" + str(batch_idx) + ".csv" 
error_list = data_dir + "/log_json/json_" + license + "_error_batch_" + str(batch_idx) + ".csv" 

# log files
log_files = {}
log_files['complete'] = completed_list
log_files['error'] = error_list

# create directories and files if doesn't exist
json_log_dir = os.path.join(data_dir, "log_json")
os.makedirs(json_log_dir, exist_ok=True)

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
        os.makedirs(filelist_batch_dir + "/filelist_json/", exist_ok=True)
        file_list_path = filelist_batch_dir + "/filelist_json/" + "json_filelist_" + license + "_batch_" + str(batch_idx) + ".csv" 
        create_subset_based_on_file_diff(batch_filelist_fixed, completed_list, file_list_path)

    build_json(data_dir=data_dir, 
            file_list_path=file_list_path,
            batch_size=batch_size,
            license=license,
            log_files=log_files,
            log=True)

# End time measurement
end_time = time.time()

# Calculate and print runtime
runtime = end_time - start_time
print(f"Script runtime: {runtime:.2f} seconds")