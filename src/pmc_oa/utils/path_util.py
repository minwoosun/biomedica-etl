import os
import pandas as pd


def get_license_path_name(license):
    if license == "commercial":
        return "comm"
    elif license == "noncommercial":
        return "noncomm"
    else:
        return "other"
    
def accession_to_volume_license(accession_id, df): 

    result = df[df['AccessionID'] == accession_id]

    if not result.empty:
        # Extract the volume and license
        volume = result['volume'].values[0]
        license_type = result['license'].values[0]
        return volume, license_type
    else:
        return None, None


def build_directories(base_path):
    
    log_dir = base_path / 'logs/'
    data_dir = base_path / 'data/'
    file_list_dir = data_dir / "file_lists/"
    file_list_dir_comm = file_list_dir  / "comm/"
    file_list_dir_noncomm = file_list_dir  / "noncomm/"
    file_list_dir_other = file_list_dir  / "other/"
    media_dir = data_dir / "media_files/"
    media_dir_comm = file_list_dir  / "comm/"
    media_dir_noncomm = file_list_dir  / "noncomm/"
    media_dir_other = file_list_dir  / "other/"
    
    os.makedirs(base_path, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(media_dir, exist_ok=True)
    os.makedirs(file_list_dir, exist_ok=True)
    os.makedirs(file_list_dir_comm, exist_ok=True)
    os.makedirs(file_list_dir_noncomm, exist_ok=True)
    os.makedirs(file_list_dir_other, exist_ok=True)
    os.makedirs(media_dir_comm, exist_ok=True)
    os.makedirs(media_dir_noncomm, exist_ok=True)
    os.makedirs(media_dir_other, exist_ok=True)
