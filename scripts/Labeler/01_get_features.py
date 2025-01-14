import time
import psutil

import torch
import webdataset as wds
from torchvision import transforms
from torch.utils.data import DataLoader
import argparse

from pmc_oa.utils                  import get_memory_usage
from pmc_oa.data_loaders.wd_loader import collect_tar_files,filter_no_caption_or_no_image
from pmc_oa.feature_extractors     import extract_features_from_dataloader

def main(
    dataset_root:str, 
    device:str, 
    image_size:tuple, 
    batch_size:int, 
    model_name:str, 
    max_images:int, 
    store_images:int,
    subset: str,
    save_name:str,
    from_n_to_m:tuple[int] =None):

    get_memory_usage("Before Runing Labeler")
    start_time = time.time()
    ############################################
    ############# LOAD DATALOADER ##############
    ############################################

    resize_transform = transforms.Compose([
        transforms.ToPILImage(),        # Convert NumPy array to PIL Image
        transforms.Resize(image_size),  # Resize 
        transforms.ToTensor() ])        # Convert PIL Image to PyTorch Tensor


    if isinstance(subset, str):
        subset    = [subset]
        
    tar_files:list[str] = collect_tar_files(dataset_root,subsets=subset)
    len_tar:int = len(tar_files)
    
    if from_n_to_m:
        n,m       = from_n_to_m

        if m == -1:
            tar_files = tar_files[n:]
        elif m > len_tar:
            m = len_tar
            tar_files = tar_files[n:m]
        else:
            tar_files = tar_files[n:m]

        save_name:str = f"{save_name}/{subset[0]}/{subset[0]}_{n}_to_{m}"
        print(f"embeding from {n} to {m}")
        
    #import pdb;pdb.set_trace()
    # Create a WebDataset with on-the-fly transformations applied
    dataset = (wds.WebDataset(tar_files)
               .select(filter_no_caption_or_no_image)     # Filter Images
               .decode('rgb')                             # Decode to PIL
               .to_tuple('jpg',"txt","__key__")           # Assume each sample contains 'image' and 'label'
               .map_tuple(resize_transform, lambda x: x)) # Apply resize to image only, leave label unchanged
    
    dataloader = DataLoader(
        dataset     = dataset, 
        batch_size  = batch_size,
        num_workers = 6)
    
    ############################################
    ######### LOAD FEATURE EXTRACTOR ###########
    ############################################
    #dinov2_vits14_reg = torch.hub.load('facebookresearch/dinov2', 'dinov2_vits14_reg')
    dinov2_vits14_reg = torch.hub.load('facebookresearch/dinov2', model_name)
   
    ############################################
    ############## RUN EXTRACTOR ###############
    ############################################
    _, _, _ = extract_features_from_dataloader(
                                        dataloader   = dataloader, 
                                        model        = dinov2_vits14_reg , 
                                        device       = device, 
                                        max_images   = max_images, 
                                        store_images = store_images,
                                        save         = save_name)
    
    ############################################
    ############## Get Run Stats ###############
    ############################################
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
    get_memory_usage("After Runing Labeler")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run feature extraction on WebDataset images.')
    parser.add_argument('--dataset_root', type=str,   default='/pasteur2/u/ale9806/data/pmc-oa/full_panel_w_labels/', help='Dataset root directory.')
    parser.add_argument('--save_name',    type=str,   default='/pasteur/u/ale9806/Repositories/pmc-oa/DinoFeatures',  help='Filename to save extracted features.')
    parser.add_argument('--subset',       type=str,   default="other",             help='Subset to choose from ["commercial", "noncommercial", "other"')
    parser.add_argument('--image_size',   type=int,   default=(224, 224), nargs=2, help='Resize dimensions for images, e.g., 224 224.')
    parser.add_argument('--from_n_to_m',  type=int,   default=None,       nargs=2, help='Filename to save extracted features.')
    parser.add_argument('--batch_size',   type=int,   default=2048,                help='Batch size for DataLoader.')
    parser.add_argument('--model_name',   type=str,   default='dinov2_vitl14_reg', help='Name of the model to load from torch.hub.')
    parser.add_argument('--max_images',   type=int,   default=None,                help='Maximum number of images to process.')
    parser.add_argument('--store_images', type=int,   default=1,                   help='Number of images to store during extraction.')
    
    
    args = parser.parse_args()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    # Run the main function with parsed arguments
    main( 
        dataset_root = args.dataset_root,
        device       = device, 
        image_size   = tuple(args.image_size),
        batch_size   = args.batch_size,
        model_name   = args.model_name,
        max_images   = args.max_images ,
        store_images = args.store_images,
        subset       = args.subset,
        save_name    = args.save_name,
        from_n_to_m  = args.from_n_to_m)