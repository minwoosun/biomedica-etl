import os
import csv
import time
import ftplib
import pandas as pd
import numpy as np
import shutil

from pathlib import Path
from typing import Optional, Union
from pmc_oa.download import download_media


def download_media_from_batch_of_filelists(data_dir: str, filelist_batch_dir: str, batch_index: int, license: str, ftp: ftplib.FTP) -> None:
    """
    Processes a batch of CSV file lists to download media files via FTP and manages logs for completed and error downloads.

    This function identifies and processes a specific batch of file lists from a directory based on the 
    provided batch index. For each batch, it calls `download_media_from_filelist` to download media files 
    corresponding to article metadata. It also creates log files for completed and error cases, ensuring 
    that logs are properly categorized and saved for later review.

    Parameters:
    - data_dir (str): Path to the main directory where downloaded media files and log files will be stored.
    - filelist_batch_dir (str): Directory containing CSV files for each batch of article lists to be processed.
    - batch_index (int): Index specifying which batch of files to process.
    - ftp (ftplib.FTP): An active FTP connection used to download media files for the articles.

    Returns:
    - Noneo
    """
    # List all files in the directory
    all_files = sorted([f for f in os.listdir(filelist_batch_dir) if f.endswith('.csv')])

    # Create completed and error log directories if not exist
    log_complete_dir = data_dir + "/log_completed/" + license + "/"
    log_error_dir = data_dir + "/log_error/"  + license + "/"
    os.makedirs(log_complete_dir, exist_ok=True)
    os.makedirs(log_error_dir, exist_ok=True)
    
    # Set file name and path
    last_directory = os.path.basename(os.path.normpath(filelist_batch_dir))
    file_name = f'filelist_{last_directory}_batch_{batch_index}.csv'  # <-- I have to make sure to save the batched filelists like this
    file_path = os.path.join(filelist_batch_dir, file_name)
    
    # Create the initial log files
    error_count = 0
    main_batch_index = get_main_batch_index(batch_index)
    file_path_error = create_log_file_path(filelist_batch_dir, main_batch_index, f'error_{error_count}', log_error_dir)
    file_path_complete = create_log_file_path(filelist_batch_dir, main_batch_index, 'complete', log_complete_dir)

    log_files = {'complete':file_path_complete,
                 'error':file_path_error}

    # Check if the file exists in the directory
    if file_name in all_files:

        print(f"Processing {file_name}")
        
        download_media_from_filelist(data_dir=data_dir, 
                                     file_list_path=file_path, 
                                     ftp=ftp, 
                                     license=license,
                                     log_files=log_files)

    else:
        print(f"{file_name} not found in the directory.")


def download_media_from_filelist(data_dir: str,  file_list_path: str, ftp: ftplib.FTP, license: str, out_dir: Optional[Path] = None, log_files: Optional[dict[str, str]] = None, logging: bool = True) -> None:
    """
    Downloads media files for multiple articles in a file list, processes metadata, and manages log entries for success and errors.

    This function processes a file list by downloading media files associated with each article's metadata 
    using FTP. It extracts relevant metadata and figure captions, creates JSON files for each article, 
    and manages logging for both successful and error cases. If errors occur (e.g., FTP errors or other exceptions), 
    they are logged in a separate error log file, while successful downloads are logged in a completion log file.

    Parameters:
    - data_dir (str): Directory where the downloaded media files and associated JSON metadata will be saved.
    - file_list_path (str): Path to the CSV file containing metadata and file paths for the articles to be processed.
    - ftp (ftplib.FTP): An active FTP connection used to download the media files.
    - out_dir (Optional[Path], optional): Directory where the media files will be saved. If not provided, defaults to 
      `data_dir/media_files/`. Defaults to None.
    - log_files (Optional[dict[str, str]], optional): A dictionary containing paths to log files, with keys for 'complete' 
      and 'error' logs. Required if `logging` is set to True. Defaults to None.
    - logging (bool): Whether or not to log the results of the media download process. Defaults to True.

    Raises:
    - ValueError: If logging is enabled but `log_files` is not provided.

    Returns:
    - None
    """
 
    # Set paths
    out_dir = Path(data_dir) / license / "media_files/" 

    # column names for log csv files
    error_columns = ['File', 'Citation', 'Accession_ID', 'Date', 'License', 'Traceback']
    complete_columns = ['Accession_ID']

    for article_row in path_generator(file_list_path):
        
        # ftp remote media file path
        file_path = "/pub/pmc/" + article_row["File"]

        ###########################
        #   Download media files  #
        ###########################

        try: 
            _,ftp = download_media(ftp=ftp, 
                                   remote_filepath=file_path, 
                                   keep_compressed=False, 
                                   out_dir=out_dir)
            
            # double check directory exists
            pmcid_directory_path = out_dir / article_row["Accession_ID"]
            if not os.path.exists(pmcid_directory_path): 
                raise Exception(f"Path doesn't exist {pmcid_directory_path}")
     
        except (ftplib.error_temp, ftplib.error_perm) as ftp_error:
            # Log the FTP-specific error and continue to the next file
            article_row['Traceback'] = ftp_error
            create_log_file(log_files['error'], error_columns)
            append_row_to_csv(log_files['error'], article_row)
            time.sleep(1)
            print(f"FTP error occurred while downloading {file_path}: {ftp_error}")
            continue

        except Exception as e:
            # Catch any other unforeseen errors
            article_row['Traceback'] = e
            create_log_file(log_files['error'], error_columns)
            append_row_to_csv(log_files['error'], article_row)
            time.sleep(1)
            print(f"An error occurred while downloading {file_path}: {e}")
            continue

        # log successful articles
        if logging:
            log_complete = {'Acession_ID':article_row['Accession_ID']}
            create_log_file(log_files['complete'], complete_columns)
            append_row_to_csv(log_files['complete'], log_complete)


def path_generator(csv_file_path: str):
    """
    Generates dictionaries from each row of the CSV file containing article metadata.

    Parameters:
    - csv_file_path (str): Path to the CSV file containing the article data.

    Yields:
    - dict[str, str]: A dictionary for each row containing 'File', 'Citation', 'Accession_ID', 'Date', and 'License' keys.
    """
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield {
                'File': row['File'],
                'Citation': row['Citation'], 
                'Accession_ID': row['Accession_ID'],
                'Date': row['Date'],
                'License': row['License'],
            }


def create_log_file_path(directory: str, batch_index: int, log_type: str, log_dir: str) -> str:
    """
    Generates the file path for log files (error or complete) for a specific batch.

    Parameters:
    - directory (str): Path to the batch directory.
    - batch_index (int): Index of the current batch.
    - log_type (str): Type of log file ('error' or 'complete').
    - log_dir (str): Directory where the log files will be saved.

    Returns:
    - str: The generated file path for the log file.
    """
    # Extract the last directory name
    last_directory = os.path.basename(os.path.normpath(directory))
    file_name = f'{last_directory}_{log_type}_batch_{batch_index}.csv'
    file_path = log_dir + file_name
    return file_path


def create_log_file(file_path: str, columns: list[str]) -> None:
    """
    Creates a CSV log file with specified columns.

    Parameters:
    - file_path (str): Path to the CSV file to be created.
    - columns (list[str]): List of column names for the CSV file.

    Returns:
    - None
    """
    # Check if the file already exists
    if os.path.exists(file_path):
        return
    else: 
        df = pd.DataFrame(columns=columns)
        df.to_csv(file_path, index=False)


def append_row_to_csv(file_path: str, row: dict[str, str]) -> None:
    """
    Appends a single row of data to an existing CSV file.

    Parameters:
    - file_path (str): Path to the existing CSV file.
    - row (dict[str, str]): A dictionary representing the row of data to append.

    Returns:
    - None
    """
    # Create a DataFrame with the row and append it to the CSV
    df = pd.DataFrame([row])
    df.to_csv(file_path, mode='a', header=False, index=False)


def get_main_batch_index(sub_batch_id: str) -> str:
    """
    Extracts the main batch index from a sub-batch identifier.

    Parameters:
    - sub_batch_id (str): Sub-batch identifier which may contain an underscore.

    Returns:
    - str: The main batch index if present, otherwise the original sub-batch identifier.
    """
    # Split the sub-batch ID by the underscore if it exists
    parts = str(sub_batch_id).split('_')
    
    # If the underscore exists, return the first part (main batch index)
    # Otherwise, return the sub_batch_id itself as it's already the main batch index
    if len(parts) > 1:
        return parts[0]
    else:
        return sub_batch_id
    

def compare_csv_row_counts(file1_path: str, file2_path: str) -> bool:
    """
    Compares the number of rows between two CSV files.

    Parameters:
    file1_path (str): Path to the first CSV file.
    file2_path (str): Path to the second CSV file.

    Returns:
    bool: True if the number of rows in both files match, False otherwise.

    Prints:
    A message indicating whether the row counts match or, if not, the row count of each file.
    """
    def get_row_count(file_path):
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            row_count = sum(1 for row in reader)
        return row_count

    # Get the row counts for both files
    row_count_file1 = get_row_count(file1_path)
    row_count_file2 = get_row_count(file2_path)

    # Compare the row counts
    if row_count_file1 == row_count_file2:
        print(f"The number of rows match: {row_count_file1} rows.")
        return True
    else:
        print(f"Row count mismatch: File 1 has {row_count_file1} rows, File 2 has {row_count_file2} rows.")
        return False


def create_subset_based_on_file_diff(file1_path: str, file2_path: str, output_file_path: str) -> None:
    """
    Creates a subset of the first CSV file based on the difference in 'Accession_ID' 
    column values between two CSV files.

    Parameters:
    file1_path (str): Path to the first CSV file.
    file2_path (str): Path to the second CSV file.
    output_file_path (str): Path where the subset CSV file will be saved.

    Raises:
    ValueError: If either file does not contain the 'Accession_ID' column.

    Writes:
    A new CSV file containing rows from the first file whose 'Accession_ID' values are 
    not present in the second file.

    Prints:
    A message indicating the number of rows in the generated subset.
    """
    # Read both CSV files into pandas DataFrames
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    # Ensure both DataFrames have the "File" column
    if 'Accession_ID' not in df1.columns or 'Accession_ID' not in df2.columns:
        raise ValueError("Both files must contain a 'Accession_ID' column.")
    
    # Get the set difference based on the "File" column
    file_diff = set(df1['Accession_ID']) - set(df2['Accession_ID'])

    # Create a subset of the first DataFrame based on the set difference
    subset_df = df1[df1['Accession_ID'].isin(file_diff)]

    # Write the subset to a new CSV file
    subset_df.to_csv(output_file_path, index=False)

    print(f"Subset created and saved to {output_file_path} with {len(subset_df)} rows.")


# --- batching functions --- 
def create_batched_filelist(filelist_df, main_filelist_name, batch_count, filelist_dir, save=True):
    """
    Splits a file list DataFrame into smaller batches and saves them as separate CSV files.

    Parameters:
    - filelist_df (pd.DataFrame): The original file list DataFrame.
    - main_filelist_name (str): The base name for the output batch files.
    - batch_count (int): The number of batches to create.
    - filelist_dir (str): Directory where the batched files will be saved.
    - save (bool, optional): Whether to save the batched file lists to disk. Defaults to True.

    Returns:
    - None
    """
    nrows = filelist_df.shape[0]
    batch_sequence = create_batch_sequence(batch_count, nrows)
    
    if(len(batch_sequence) == nrows):
        filelist_df['batch_id'] = batch_sequence
    else: 
        print("error")

    # Save file lists
    if save:
        for batch_id in filelist_df['batch_id'].unique():
            batch_df = filelist_df[filelist_df['batch_id'] == batch_id]
            file_name = f"{main_filelist_name}_batch_{batch_id}.csv"
            output_path = os.path.join(filelist_dir, file_name)
            batch_df.to_csv(output_path, index=False)
            print(f"Saved {file_name} to {filelist_dir}")
    else: 
        return
    

def create_batch_sequence(batch_count: int, n_rows: int) -> np.ndarray:
    """
    Creates a sequence of batch indices to assign rows to batches.

    Parameters:
    - batch_count (int): The number of batches to create.
    - n_rows (int): The total number of rows to assign to batches.

    Returns:
    - np.ndarray: An array containing the batch indices for each row.
    """
    n_repeat = np.floor(n_rows / batch_count) 
    n_remainder = n_rows % batch_count
    sequence = np.repeat(np.arange(0, batch_count), n_repeat)
    sequence = np.concatenate([np.repeat(0, n_remainder), sequence])
    return sequence


def create_fixed_filelists(directory: Union[str, os.PathLike]) -> None:
    """
    This function duplicates all .csv files in the specified directory and renames
    the duplicates by appending '_fixed' before the file extension.
    
    :param directory: The path to the directory containing the .csv files.
    """
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):  # Only process CSV files
            # Create the source file path
            source_file = os.path.join(directory, filename)
            
            # Create the destination file path with "_fixed" appended before the ".csv"
            new_filename = filename.replace(".csv", "_fixed.csv")
            destination_file = os.path.join(directory, new_filename)
            
            # Copy the file to the new location with the updated name
            shutil.copyfile(source_file, destination_file)

if __name__ == "__main__":
    # example for how do download from one file list 
    # for batch example use script 03_batch_ftp_download.py and .sh

    # set paths and create output directory
    license = "toy"
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(current_script_dir, '..', '..', 'data')
    data_dir = os.path.abspath(target_dir)
    out_dir = data_dir / license / Path("media_files/")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)

    # toy file list with 5 articles
    file_list_path = data_dir / Path("file_lists/toy/filelist_toy_batch_4_fixed.csv")
    
    # establish ftp connection
    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login("anonymous", "")

    download_media_from_filelist(data_dir=data_dir, 
                                 out_dir=out_dir,
                                 file_list_path=file_list_path, 
                                 ftp=ftp,
                                 logging=False)
    
    ftp.quit()
    print("Download complete. Check toy/media_files/ for the 5 article media file directories.")