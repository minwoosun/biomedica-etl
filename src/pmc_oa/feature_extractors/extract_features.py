import os

import torch 
import numpy as np
from tqdm import tqdm


def save_embeddings_and_images(
        save:str, 
        features:np.array, 
        images:np.array, 
        keys:np.array):
    # Create the directory if it doesn't exist
    #os.makedirs(save, exist_ok=True)
    
    # Save features, images, keys, and their shapes
    np.save(os.path.join(save, "features"),  np.array(features))   # Save features
    np.save(os.path.join(save, "images"),    np.array(images))     # Save images
    np.save(os.path.join(save, "keys"),      np.array(keys))       # Save keys
    
    # Save shapes
    shapes = {
        'features_shape': np.array(features).shape,
        'images_shape':   np.array(images).shape,
        'keys_shape':     np.array(keys).shape
    }
    np.save(os.path.join(save, "shapes"), shapes)  # Save shapes as a dictionary
    
  

def extract_features_from_dataloader(
    dataloader, 
    model, 
    device:str      = "cuda", 
    max_images:int  = None, 
    store_images    = 10,
    save: str       = None,
    save_every: int = 10):
    """
    Extracts features from images using a pre-trained model and returns both the features and 
    a subset of the images. Includes a progress bar using `tqdm`.

    Parameters
    ----------
    dataloader : torch.utils.data.DataLoader
        A DataLoader providing batches of images and labels.
    
    model : torch.nn.Module
        Pre-trained feature extraction model.
    
    device : str, optional
        Device to perform computation on ('cuda' or 'cpu'). Default is 'cuda'.
    
    max_images : int, optional
        Maximum number of batches of images to process. Default is 100.
    
    store_images : int, optional
        Number of images to store in the returned image subset. Default is 10.

    Returns
    -------
    tuple of (numpy.ndarray, list of torch.Tensor)
        - all_features: A numpy array of extracted features for the processed images.
        - images: A list containing `store_images` number of original image tensors.

    Notes
    -----
    The function moves images to the specified device for feature extraction and then
    moves the features back to the CPU before converting them to numpy arrays.
    
    The extraction process stops after processing `max_images` batches from the dataloader.

    Examples
    --------
    >>> features, img_subset = extract_features_from_dataloader(dataloader, dinov2_vits14_reg)
    >>> features.shape
    (num_images, feature_size)
    """
    all_features = []
    keys = []
    images = []

    # Ensure model is in evaluation mode and moved to the correct device
    model.to(device)
    model.eval()

    with torch.no_grad():
        # Adding tqdm progress bar for visual feedback
        for i, sample in enumerate(tqdm(dataloader, desc="Extracting Features")):
            image, _ ,key = sample
            img = image.to(device)  # Move image tensor to GPU
            features = model(img).squeeze().cpu().numpy()  # Move features to CPU and convert to numpy
            all_features.extend(features)
            keys.extend(key)
            
            # Store first `store_images` images for further inspection
            if i < store_images:
                images.extend(image)
            
            # Save every `save_interval` batches
            if save and (i + 1) % save_every == 0:
                os.makedirs(save, exist_ok=True)
                save_embeddings_and_images(
                    save     = save, 
                    features = all_features, 
                    images   = images,  
                    keys     = keys)

            # Stop after `max_images` batches are processed
            if max_images:
                if i == max_images:
                    break
                    
    os.makedirs(save, exist_ok=True)
    np.save(os.path.join(save, f"features"), np.array(all_features))  # Save features
    np.save(os.path.join(save, f"images"), np.array(images))          # Save images
    np.save(os.path.join(save, f"keys"), np.array(keys))      # Save keys
    print(f"Embeddings and images saved to {save} at batch {i + 1}")
        
    images = np.array(images)


    
    return all_features, images,keys