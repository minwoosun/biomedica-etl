import os
import json
from pathlib import Path
from typing import List, Dict, Any, Tuple
import random
import matplotlib.pyplot as plt
from torchvision.transforms import ToTensor
from torch.utils.data import Dataset



def read_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """Read JSON data from a file.
    
    Parameters
    ----------
    file_path : Path
        The path to the JSON file.
    
    Returns
    -------
    List[Dict[str, Any]]
        The data contained in the JSON file as a list of dictionaries.
    
    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    JSONDecodeError
        If the file contents are not valid JSON.
    """
    with open(file_path, 'r') as file:
        return json.load(file)


def aggregate_publications_from_json(
        directory_path: str,
        drop_empty_figure_sets:bool=True,
        n:int= None,
        verbose:bool=True) -> List[Dict[str, str]]:
    """
    Aggregate publication data from all JSON files in the specified directory.
    
    Parameters
    ----------
    directory_path : str
        The path to the directory containing JSON files.
    
    Returns
    -------
    List[Dict[str, Any]]
        A list of all publications aggregated from the JSON files in the directory.
    
    Raises
    ------
    Exception
        If there is an error reading a JSON file.
    """
    
    root = Path(directory_path)/"data"/"json"
    all_publications = []
    droped_studies = 0
    
    # Iterate over each file in the JSON directory

    COUNT = 0
    for json_file in root.iterdir():
        COUNT+=1
        if n:
            if COUNT == n:
                break
        if json_file.is_file() and json_file.suffix == '.json':
            try:
                # Read and aggregate JSON data
                publication_data = read_json_file(json_file)
                if drop_empty_figure_sets: 
                    new_publication_data = [data  for data in publication_data if data['figureset'] != []]
                    droped_studies+= len(publication_data) - len(new_publication_data)
                    publication_data = new_publication_data
                
                    
                all_publications.extend(publication_data)
                
            except Exception as error:
                print(f"Failed to read {json_file.name}: {error}")
    if verbose:
        print(f"Dropped {droped_studies} empty studies")
    
    return all_publications


def extract_image_caption_pairs(publications: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """
    Extract image paths and captions from the list of publication dictionaries.

    Parameters
    ----------
    publications : List[Dict[str, Any]]
        List of dictionaries, where each dictionary represents a publication.

    Returns
    -------
    List[Tuple[str, str]]
        List of tuples where each tuple contains an image path and its corresponding caption.
    """
    image_caption_pairs = []
    for publication in publications:
        figureset:list[dict] = publication.get('figureset', [])
        accession_id:str = publication.get("accession_id", [])
        
        for figure in figureset:
            image_path:str = figure.get('image_path')
            caption:str    = figure.get('caption')
            image_id:str   = figure.get('image_id')
            if image_path and caption:
                image_caption_pairs.append(
                    {"image_path":image_path, 
                     "caption":caption,
                     "image_id":image_id,
                     "accession_id":accession_id})
    
    return image_caption_pairs


def plot_random_examples(dataset: Dataset, num_examples: int = 10):
    """
    Plot random examples from the dataset.

    Parameters
    ----------
    dataset : Dataset
        The PyTorch dataset from which to sample images and captions.
    num_examples : int
        The number of random examples to plot.
    """
    indices = random.sample(range(len(dataset)), num_examples)
    
    plt.figure(figsize=(15, num_examples * 3))  # Adjust the figure size for better visibility
    
    for i, idx in enumerate(indices):
        caption, image = dataset[idx]['caption'], dataset[idx]['image']
        
        # Convert image tensor back to PIL Image for displaying
        image = ToTensor()(image).permute(1, 2, 0).numpy()  # Convert tensor to numpy array for plotting
        
        plt.subplot(num_examples, 1, i + 1)
        plt.imshow(image)
        plt.axis('off')
        
        # Print caption below the image
        plt.text(0.5, -0.1, caption, ha='center', va='top', fontsize=10, wrap=True, bbox=dict(facecolor='white', alpha=0.8))

    plt.tight_layout()
    plt.show()
