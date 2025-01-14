import torch
from torch.utils.data import Dataset
from PIL import Image
from pathlib import Path
from typing import List, Dict, Any, Tuple

from pmc_oa.data_loaders.loader_utils import aggregate_publications_from_json,extract_image_caption_pairs 


class PMCOA(Dataset):
    def __init__(self, root, transform=None,n=None):
        """
        Custom Dataset for loading images and captions.
        
        Parameters
        ----------
        image_caption_pairs : List[Tuple[str, str]]
            List of tuples where each tuple contains an image path and its corresponding caption.
        transform : callable, optional
            A function/transform to apply to the images.
        """
        self.root = root
        self.publications:List[Dict] = aggregate_publications_from_json(root,n)

            
        self.image_caption_pairs:List[Dict]= extract_image_caption_pairs(self.publications)
        
        self.transform = transform
    
    def __len__(self) -> int:
        return len(self.image_caption_pairs)
    
    def __getitem__(self, idx: int) -> Dict:
        data_point:Dict = self.image_caption_pairs[idx]
        image_path = Path(self.root + "/" + data_point["image_path"].replace("../",""))
        data_point["image"] = Image.open(image_path).convert('RGB')  

        if self.transform:
            data_point["image"] = self.transform(data_point["image"])
        
        return data_point
    
    def __str__(self):
        return f"Dataset object with {len(self.publications)} publications and {len(self.image_caption_pairs)} image-caption pairs"
    
    def __repr__(self): 
        return f"Dataset object with {len(self.publications)} publications and {len(self.image_caption_pairs)} image-caption pairs"
        #return  self.filtered_adata.__repr__()

