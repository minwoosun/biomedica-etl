import argparse
import pandas as pd
from pathlib import Path
from pmc_oa.download import download_filelist_csv


def create_subset_csv(path_to_file, n, output_file='subset.csv'):
    # Load the original CSV file
    df = pd.read_csv(path_to_file)
    
    # Create a subset of n rows
    subset_df = df.head(n)
    
    # Write the subset to a new CSV file
    subset_df.to_csv(output_file, index=False)
    
    print(f"Subset of {n} rows saved to {output_file}")

# Parse arguments
parser = argparse.ArgumentParser(description="Download and split file list by license type.")
parser.add_argument('--data_dir', type=str, required=False, default='./data/', help="Directory to save the downloaded file list")

args = parser.parse_args()
data_dir = args.data_dir

# Download file list
filelist_dir = Path(args.data_dir) / "file_lists"
filelist_path = filelist_dir / "oa_file_list.csv"
media_filelist_csv_url = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.csv"

print("--> Download main file list: oa_file_list.csv")
download_filelist_csv(media_filelist_csv_url, filelist_path)

df = pd.read_csv(filelist_path)
commercial = ['CC0', 'CC BY', 'CC BY-SA', 'CC BY-ND']
noncommercial = ['CC BY-NC', 'CC BY-NC-SA', 'CC BY-NC-ND']

# rename columns
df.rename(columns={
    "File": "File",
    "Article Citation": "Citation",
    "Accession ID": "Accession_ID",
    "Last Updated (YYYY-MM-DD HH:MM:SS)": "Date",
    "PMID": "PMID",
    "License": "License"
}, inplace=True)

# create file list for commercial
print("--> Creating commercial file list split.")
df_commercial = df[df['License'].isin(commercial)]
filelist_path = data_dir + 'file_lists/filelist_commercial.csv'
df_commercial.to_csv(filelist_path, index=False)

# create file list for noncommercial
print("--> Creating noncommercial file list split.")
df_noncommercial = df[df['License'].isin(noncommercial)]
filelist_path = data_dir + 'file_lists/filelist_noncommercial.csv'
df_noncommercial.to_csv(filelist_path, index=False)

# create file list for other
print("--> Creating other file list split.")
df_other = df[~df['License'].isin(commercial + noncommercial)]
filelist_path = data_dir + 'file_lists/filelist_other.csv'
df_other.to_csv(filelist_path, index=False)

# create a toy file list (subset 5)
print("--> Creating toy file list.")
create_subset_csv(data_dir + "file_lists/filelist_commercial.csv", 1000, data_dir + "file_lists/filelist_toy.csv")
