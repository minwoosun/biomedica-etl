import json
import os

def save_json(data, file_path):
    """
    Save a dictionary as a JSON file. Creates directories if they don't exist.
    
    Args:
        data (dict): The dictionary to save.
        file_path (str): The path where the JSON file will be saved.
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Save dictionary to JSON
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"JSON saved to {file_path}")

def load_json(file_path,verbose:bool=True):
    """
    Load a dictionary from a JSON file.
    
    Args:
        file_path (str): The path to the JSON file to load.
        
    Returns:
        dict: The loaded dictionary.
    """
    # Load dictionary from JSON
    with open(file_path, 'r') as f:
        data = json.load(f)
    if verbose:
        print(f"JSON loaded from {file_path}")
    return data