import os
import sys
from pmc_oa.utils.json_utils import load_json

def load_label_dict(subset_path:str,dict_filename:str="label_dict.json") -> dict[str,int]:
    """
    Load label dictionaries from JSON files in a specified directory.

    This function iterates through all directories within the given `subset_path`.
    For each directory, it checks if a JSON label file exists and loads its content
    into a dictionary. All labels are then aggregated into a single dictionary.

    Parameters
    ----------
    subset_path : str
        Path to the directory containing subdirectories with JSON label files.

    Returns
    -------
    dict[str,int]
        A dictionary with labels loaded from JSON files in the specified directory.
        Each entry in the dictionary corresponds to a label from a JSON file.

    Warns
    -----
    Warning
        If a JSON label file is missing in any subdirectory.

    Notes
    -----
    The function expects the presence of JSON label files in each subdirectory
    of `subset_path`. If any label file is missing, it will log a warning.
    """
    labels = {} 
    for dir_name in os.listdir(subset_path):
        dir_path = os.path.join(subset_path, dir_name)
        if os.path.isdir(dir_path):         # Check if it's a directory
            label_file = os.path.join(dir_path, dict_filename)
            if os.path.isfile(label_file):  # Check if the file exists
                labels.update(load_json(label_file))
            else:
                print(f"Warning: {label_file} does not exist.")
    
    size_in_bytes = sys.getsizeof(labels)
    size_in_gb = size_in_bytes / (1024**3)  # Convert bytes to gigabytes
    print(f"Label Dict with {len(labels):,} datapoints and size: {round(size_in_gb,3)} GB ")
    
    return labels