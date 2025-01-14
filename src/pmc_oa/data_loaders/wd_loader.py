import os 
import random
import torchvision.transforms as transforms

def collect_tar_files(root: str,
                      subsets: list[str] = ["commercial", "noncommercial", "other"],
                      random_seed:int=None) -> list[str]:
    """
    Collects all .tar files from specified subset directories.

    Parameters
    ----------
    root : str
        The root directory where the subset directories are located.
    subsets : list of str, optional
        The list of subset directories to search for .tar files. Defaults to
        ["commercial", "noncommercial", "other"].
    random_seed: int
        Random seed

    Returns
    -------
    list of str
        A list of file paths to the .tar files found within the specified subsets.
    
    Raises
    ------
    FileNotFoundError
        If a specified subset directory does not exist.
    
    Notes
    -----
    The order of the returned .tar files is randomized.

    Examples
    --------
    >>> collect_tar_files("/data", subsets=["commercial"])
    ['/data/commercial/file1.tar', '/data/commercial/file2.tar']
    """
    print("No random seed, is this intentional?")
    tar_files = []
    for subset in subsets:
        tar_directory = os.path.join(root, subset)
        
        # Check if the directory exists
        if not os.path.isdir(tar_directory):
            raise FileNotFoundError(f"Directory '{tar_directory}' does not exist")
        
        tar_files.extend([os.path.join(tar_directory, f) 
                          for f in os.listdir(tar_directory) if f.endswith('.tar')])

    if not tar_files:
        raise FileNotFoundError("No .tar files found in the specified directory.")
    
    if random_seed:
        random.seed(random_seed)
        random.shuffle(tar_files)
    else:
        tar_files.sort()

    return tar_files


def collect_tar_files_from_labels(root: str,
                      subsets: list[str] = ["commercial", "noncommercial", "other"],
                      random_seed: int = None) -> list[str]:
    """
    Collects all .tar files from specified subset directories, including one more level of subdirectories.

    Parameters
    ----------
    root : str
        The root directory where the subset directories are located.
    subsets : list of str, optional
        The list of subset directories to search for .tar files. Defaults to
        ["commercial", "noncommercial", "other"].
    random_seed: int, optional
        Random seed for shuffling the results.

    Returns
    -------
    list of str
        A list of file paths to the .tar files found within the specified subsets.
    
    Raises
    ------
    FileNotFoundError
        If a specified subset directory does not exist.
    
    Notes
    -----
    The order of the returned .tar files is randomized if a random seed is provided.
    """
    print("No random seed, is this intentional?")
    tar_files = []
    for subset in subsets:
        subset_directory = os.path.join(root, subset)
        
        # Check if the directory exists
        if not os.path.isdir(subset_directory):
            raise FileNotFoundError(f"Directory '{subset_directory}' does not exist")
        
        # Iterate over the first level of subdirectories within each subset
        for subdir in os.listdir(subset_directory):
            subdir_path = os.path.join(subset_directory, subdir)
            if os.path.isdir(subdir_path):
                tar_files.extend([os.path.join(subdir_path, f) 
                                  for f in os.listdir(subdir_path) if f.endswith('.tar')])

    if not tar_files:
        raise FileNotFoundError("No .tar files found in the specified directory.")
        
    # Shuffle or sort the results based on the random seed
    if random_seed is not None:
        random.seed(random_seed)
        random.shuffle(tar_files)
    else:
        tar_files.sort()

    return tar_files


def filter_no_caption_or_no_image(sample):
    """
    Filters out samples that do not have both a caption and an image.

    A sample is considered valid if it contains a 'txt' key for the caption
    and at least one of the following keys for the image: 'png', 'jpg', 'jpeg', 'webp'.

    Parameters
    ----------
    sample : dict
        A dictionary representing a sample which may contain image and caption data.

    Returns
    -------
    bool
        True if the sample contains both a caption ('txt' key) and an image ('png', 'jpg', 'jpeg', or 'webp' key), 
        otherwise False.
    """
    has_caption = ('txt' in sample)
    has_image = ('png' in sample or 'jpg' in sample or 'jpeg' in sample or 'webp' in sample)
    return has_caption and has_image


resize_transform = transforms.Compose([
    transforms.ToPILImage(),        # Convert NumPy array to PIL Image
    transforms.Resize((224, 224)),  # Resize to 224x224
    transforms.ToTensor(),          # Convert PIL Image to PyTorch Tensor
])


