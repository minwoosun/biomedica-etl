import os
import json
import shutil
from typing import List, Dict, Any, Optional, Union, Set


def load_json(file_path: str) -> Optional[List[Dict[str, Any]]]:
    """
    Load a JSON file that contains a list of dictionaries.
    
    :param file_path: Path to the JSON file
    :return: List of dictionaries
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
       #          print(f"Loaded {len(data)} dictionaries.")
                return data
            else:
                raise ValueError("The JSON file does not contain a list of dictionaries.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except ValueError as ve:
        print(f"Value Error: {ve}")
    return []


def find_missing_json(original_path, entrez_path):
    
    # Get a list of files in each directory
    files_in_json_og = set(os.listdir(original_path))
    files_in_json_entrez = set(os.listdir(entrez_path))
    
    # Find files in json_og that are missing in json_entrez
    missing_files = files_in_json_og - files_in_json_entrez
    missing_files_list = list(missing_files)

    return missing_files_list


def make_copies(original_path, entrez_path, missing_files_list):
    """
    Copies files from original_path to entrez_path based on the missing_files_list.

    Args:
        original_path (str): Path to the directory containing the original JSON files.
        entrez_path (str): Path to the directory where the copies should be stored.
        missing_files_list (list): List of filenames to copy.
    """
    # Ensure the destination directory exists
    os.makedirs(entrez_path, exist_ok=True)
    
    for filename in missing_files_list:
        json_og_path = os.path.join(original_path, filename)
        json_copy_path = os.path.join(entrez_path, filename)
        
        try:
            shutil.copy(json_og_path, json_copy_path)
            print(f"Copied: {json_og_path} to {json_copy_path}")
        except FileNotFoundError:
            print(f"File not found: {json_og_path}")
        except Exception as e:
            print(f"Error copying {json_og_path} to {json_copy_path}: {e}")


if __name__ = "__main__":
    
    LICENSE = "commercial" # "noncommercial" "other"

    json_og_path = f"./data/json/{LICENSE}/"
    json_post_entrez_path = f"./data/json_final/{LICENSE}/"

    missing_files_list = find_missing_json(json_og_path, json_post_entrez_path)
    print("Total missing: " + str(len(missing_files_list)))