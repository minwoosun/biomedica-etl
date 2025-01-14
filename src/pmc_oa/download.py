import os
import glob
import ftplib
import tarfile
import requests
import time
import pandas as pd
from pathlib import Path


def download_media(ftp: ftplib.FTP, remote_filepath: str, keep_compressed: bool, out_dir: str, retry_delay: int = 3, max_retry: int = 3) -> tuple[str, ftplib.FTP]:
    """
    Downloads an article from an FTP server, optionally decompresses the file, and handles retries in case of failure.

    Parameters:
    - ftp (ftplib.FTP): An active FTP connection.
    - remote_filepath (str): Path to the file on the FTP server.
    - keep_compressed (bool): Whether to keep the file compressed (tar.gz) or decompress it.
    - out_dir (str): Directory where the downloaded file will be saved.
    - retry_delay (int, optional): Delay in seconds before retrying a failed download. Defaults to 3.
    - max_retry (int, optional): Maximum number of retries for failed downloads. Defaults to 3.

    Returns:
    - tuple: The output file path and the FTP connection object.

    Raises:
    - Exception: If the download fails after the maximum number of retries.
    """  
    retries = 0
    filename = os.path.basename(remote_filepath)
    output_file_path = os.path.join(out_dir, filename)

    # Get PMCID
    filename_without_gz = os.path.splitext(remote_filepath)[0]
    pmcid = os.path.splitext(os.path.basename(filename_without_gz))[0]
    # print(out_dir)
    pmcid_directory_path = os.path.dirname(out_dir) + '/media_files/' + pmcid
    print(pmcid_directory_path)

    # Create the output directory if it doesn't exist
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # check if file already exists
    if keep_compressed:
        check_path = output_file_path
    else:
        check_path = os.path.splitext(os.path.splitext(output_file_path)[0])[0]
    if os.path.exists(check_path):
        return

    while retries < max_retry:
        try:
            # Actual download logic
            with open(output_file_path, 'wb') as output_file:
                ftp.retrbinary(f'RETR {remote_filepath}', output_file.write)
                print(f"Successfully downloaded: {remote_filepath}")
                break

        except (ftplib.error_temp, ftplib.error_perm) as ftp_error:
            print(f"FTP error occurred: {ftp_error}. Retrying {retries + 1}/{max_retry}")
            retries += 1
            time.sleep(retry_delay)  # Wait before retrying
            ftp = reconnect_ftp(ftp)  # Reconnect and update FTP
            with open(output_file_path, 'wb') as output_file:
                ftp.retrbinary(f'RETR {remote_filepath}', output_file.write)

        except Exception as e:
            print(f"An unforeseen error occurred: {e}")
            retries += 1
            time.sleep(retry_delay)
            ftp = reconnect_ftp(ftp)  # Reconnect and update FTP
            with open(output_file_path, 'wb') as output_file:
                ftp.retrbinary(f'RETR {remote_filepath}', output_file.write)

    if retries == max_retry:
        raise Exception(f"Failed to download {remote_filepath} after {max_retry} retries.")
    
    # Unzip and delete the tar.gz file
    if not keep_compressed:
        unzip_file(output_file_path, out_dir)
        remove_file(output_file_path)

        # delete .pdf, .xlsx, .doc 
        print(pmcid_directory_path)
        delete_files(pmcid_directory_path)

    # 1 second delay
    time.sleep(1)
    
    return output_file_path, ftp


def reconnect_ftp(ftp: ftplib.FTP) -> ftplib.FTP:
    """
    Reconnects to the FTP server after closing the previous connection.

    Parameters:
    - ftp (ftplib.FTP): The existing FTP connection to be closed and reconnected.

    Returns:
    - ftplib.FTP: A new FTP connection to the same server.
    """
    try:
        ftp.quit()  # Close the existing connection
    except Exception:
        pass  # Ignore any errors when quitting

    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login("anonymous", "")
    print("Reconnected to FTP server")
    return ftp


def download_filelist_csv(url: str, output_file_path: str) -> None:
    """
    Downloads a CSV file from a given URL and saves it to a specified file path.

    Parameters:
    - url (str): The URL to download the CSV file from.
    - output_file_path (str): The path where the downloaded file will be saved.

    Returns:
    - None

    Raises:
    - requests.exceptions.HTTPError: If an HTTP error occurs during the request.
    """
    # Ensure that the directory exists
    directory = os.path.dirname(output_file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

    if os.path.isfile(output_file_path):
        print(f"{output_file_path} already exists.")
        return
    try:
        response = requests.get(url)
        response.raise_for_status()

        with open(output_file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {output_file_path}")

        # Count the number of rows in the CSV
        with open(output_file_path, 'r', encoding='utf-8') as file:
            num_rows = sum(1 for _ in file)
        print(f"{output_file_path} contains {num_rows} rows.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")


def remove_file(file_path: str) -> None:
    """
    Deletes a file from the filesystem.

    Parameters:
    - file_path (str): Path to the file to be deleted.

    Returns:
    - None
    """
    os.remove(file_path)


def unzip_file(file_path: str, extract_to_directory: str) -> None:
    """
    Extracts a tar.gz file to a specified directory.

    Parameters:
    - file_path (str): Path to the tar.gz file to be extracted.
    - extract_to_directory (str): Directory where the extracted files will be saved.

    Returns:
    - None
    """
    with tarfile.open(file_path, 'r:gz') as tar:
        tar.extractall(path=extract_to_directory)


def load_and_process_media_filelist_csv(path_to_filelist: str) -> pd.DataFrame:
    """
    Loads a media file list from a CSV and processes it by extracting the file IDs.

    Parameters:
    - path_to_filelist (str): Path to the CSV file containing the media file list.

    Returns:
    - pd.DataFrame: A DataFrame with the processed file paths and IDs.
    """
    df = pd.read_csv(path_to_filelist, usecols=['File'])
    df['id'] = df['File'].apply(extract_id)
    return df


def extract_id(path: str) -> str:
    """
    Extracts the file ID from a given file path.

    Parameters:
    - path (str): The file path from which to extract the ID.

    Returns:
    - str: The extracted file ID.
    """
    return path.split('/')[-1].split('.')[0]


def pmcid_to_path(list_of_pmcids: list[str], file_list_df: pd.DataFrame) -> list[str]:
    """
    Maps a list of PMCIDs to their corresponding file paths using a DataFrame.

    Parameters:
    - list_of_pmcids (list[str]): A list of PMCIDs to map.
    - file_list_df (pd.DataFrame): A DataFrame containing file paths and their corresponding IDs.

    Returns:
    - list[str]: A list of file paths corresponding to the given PMCIDs.
    """
    filtered_df = file_list_df[file_list_df['id'].isin(list_of_pmcids)]
    paths = filtered_df['File'].tolist()
    paths = ['/pub/pmc/' + path for path in paths]
    return paths


def delete_files(path_to_media: str) -> None:
    """
    Deletes specific file types (.pdf, .nxml, .doc, .xlsx) from the given directory.

    Parameters:
    - path_to_media (str): Path to the directory containing media files to be deleted.

    Returns:
    - None
    """
    # Use glob to find all .pdf, .nxml, .doc, and .xlsx files
    pdf_files = glob.glob(os.path.join(path_to_media, '*.pdf'))
    doc_files = glob.glob(os.path.join(path_to_media, '*.doc'))
    xlsx_files = glob.glob(os.path.join(path_to_media, '*.xlsx'))
    xls_files = glob.glob(os.path.join(path_to_media, '*.xls'))
    txt_files = glob.glob(os.path.join(path_to_media, '*.txt'))
    zip_files = glob.glob(os.path.join(path_to_media, '*.zip'))
    ps_files = glob.glob(os.path.join(path_to_media, '*.ps'))
    csv_files = glob.glob(os.path.join(path_to_media, '*.csv'))
    tiff_files = glob.glob(os.path.join(path_to_media, '*.tiff'))

    # Combine the lists of files
    files_to_delete = pdf_files + doc_files + xlsx_files + xls_files + txt_files + zip_files + ps_files + csv_files + tiff_files

    # Check if there are files to delete
    if not files_to_delete:
        print("No files to delete.")
        return

    # Delete the files
    for file in files_to_delete:
        try:
            if os.path.exists(file):
                os.remove(file)
                print(f"Deleted: {file}")
            else:
                print(f"File does not exist: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")


if __name__ == "__main__":

    # example to download media files for one article PMC176545
    # remote file can be found in file lists
    remote_root = "/pub/pmc/"
    REMOTE_FILE_PATH = remote_root + "oa_package/44/2a/PMC7450322.tar.gz" # "oa_package/e6/58/PMC176545.tar.gz"

    # set paths and create output directory
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(current_script_dir, '..', '..', 'data')
    data_dir = os.path.abspath(target_dir)
    out_dir = data_dir / Path("toy/media_files/")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)
    
    # establish ftp connection
    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login("anonymous", "")

    # downloading by article accession ids
    download_media(ftp=ftp,
                   remote_filepath = REMOTE_FILE_PATH,
                   out_dir = out_dir,
                   keep_compressed=False
                   )
    
    ftp.quit()
    print("Download complete. Check toy/media_files/PMC176545/")