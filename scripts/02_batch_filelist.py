import argparse
import os
import pandas as pd
from pathlib import Path
from pmc_oa.download_batch import create_batched_filelist, create_fixed_filelists


# Parse arguments
parser = argparse.ArgumentParser(description="Preprocess and split PMC OA file list into batches")
parser.add_argument('--data_dir', type=str, required=False,  default='./data', help="Directory to save the downloaded file list")
parser.add_argument('--file_list_path', type=str, required=True, help="Path to file list")
parser.add_argument('--output_dir', type=str, required=True, help="Path to output directory")
parser.add_argument('--batch_count', type=int, required=True, help="Number of batches to create (this determines number of separated file lists)")

args = parser.parse_args()
filelist_path = args.file_list_path
data_dir = args.data_dir
output_dir = args.output_dir

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Preprocess file list
filelist_df = pd.read_csv(filelist_path)
main_filelist_name = Path(filelist_path).stem

# Batch the filelist
create_batched_filelist(filelist_df=filelist_df,
                        main_filelist_name=main_filelist_name,
                        batch_count=args.batch_count, 
                        filelist_dir=output_dir, 
                        save=True)

create_fixed_filelists(output_dir)