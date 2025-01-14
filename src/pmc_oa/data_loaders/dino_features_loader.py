import os
import numpy as np

def load_and_concatenate_subset(
    features_root: str, 
    subset: str,
    limit:int=None,
    samples_per_shard:int=None):
    """
    Load and concatenate feature, image, and key data from multiple directories.

    This function iterates over directories within a specified path, loading
    `.npy` files for `features`, `images`, and `keys` if they exist, and
    concatenates them into single arrays for each type.

    Parameters
    ----------
    features_root : str
        The root directory containing the data directories.
    subset : str
        The subset directory within `features_root` to target.

    Returns
    -------
    tuple of np.ndarray
        A tuple containing three concatenated numpy arrays:
        - features : np.ndarray
            The concatenated feature data.
        - images : np.ndarray
            The concatenated image data.
        - keys : np.ndarray
            The concatenated key data.

    Example
    -------
    >>> features_root = "/pasteur/u/ale9806/Repositories/pmc-oa/DinoFeatures"
    >>> subset = "other"
    >>> features, images, keys = load_and_concatenate_data(features_root, subset)
    """
    data_path = os.path.join(features_root, subset)
    
    # Initialize empty arrays for features, images, and keys
    features = np.array([])
    images = np.array([])
    keys = np.array([])

    # Iterate over directories in data_path and load .npy files
    iter_dirs = os.listdir(data_path)
    
    if limit:
        print("Limit enabled only loading:")
        iter_dirs= iter_dirs[:limit]
        print(iter_dirs)
        
    for dir_name in iter_dirs:
        dir_path = os.path.join(data_path, dir_name)
        if os.path.isdir(dir_path):  # Check if it's a directory
            features_path = os.path.join(dir_path, "features.npy")
            images_path   = os.path.join(dir_path, "images.npy")
            keys_path     = os.path.join(dir_path, "keys.npy")

            # Load and concatenate .npy files if they exist
            if os.path.exists(features_path):
                loaded_features = np.load(features_path, mmap_mode='r')
                if samples_per_shard:
                    random_integers = np.random.randint(0, 2048 ,samples_per_shard )
                    loaded_features = loaded_features[random_integers]
                features = np.concatenate((features, loaded_features), axis=0) if features.size else loaded_features

            if os.path.exists(images_path):
                loaded_images = np.load(images_path, mmap_mode='r')
                if samples_per_shard:
                    loaded_images = loaded_images[random_integers]
                images = np.concatenate((images, loaded_images), axis=0) if images.size else loaded_images

            if os.path.exists(keys_path):
                loaded_keys = np.load(keys_path, mmap_mode='r')
                if samples_per_shard:
                    loaded_keys = loaded_keys[random_integers]
                keys        = np.concatenate((keys, loaded_keys), axis=0) if keys.size else loaded_keys

            # Optionally delete loaded variables after concatenation
            #del loaded_features, loaded_images, loaded_keys

    return features, images, keys


def load_and_concatenate_subsets(
    features_root: str, 
    subsets:list[str] =["other","commercial","noncommercial"],
    limit:int=None,
    samples_per_shard:int=100):
    """
    Load and concatenate feature, image, and key data across multiple subsets.

    Parameters
    ----------
    features_root : str
        The root directory containing the subset directories.
    subsets : list of str
        A list of subset directory names within `features_root` to target.

    Returns
    -------
    tuple of np.ndarray
        A tuple containing three concatenated numpy arrays:
        - features : np.ndarray
            The concatenated feature data across all subsets.
        - images : np.ndarray
            The concatenated image data across all subsets.
        - keys : np.ndarray
            The concatenated key data across all subsets.
    """
    all_features = np.array([])
    all_images = np.array([])
    all_keys = np.array([])

    for subset in subsets:
        features, images, keys = load_and_concatenate_subset(
            features_root,
            subset,
            limit=limit,
            samples_per_shard=samples_per_shard)
        
        # Concatenate subset data with overall data
        all_features = np.concatenate((all_features, features), axis=0) if all_features.size else features
        all_images   = np.concatenate((all_images, images), axis=0) if all_images.size else images
        all_keys     = np.concatenate((all_keys, keys), axis=0) if all_keys.size else keys

    return {"features":all_features,"images":all_images,"keys":all_keys}