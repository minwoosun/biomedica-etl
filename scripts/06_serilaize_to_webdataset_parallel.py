import io
import os
import json
import argparse
from pathlib import Path

from PIL import Image
from tqdm import tqdm
import webdataset as wds

from pmc_oa.utils.json_utils import load_json

#######################################################
################## HELPER FUNCTIONS ###################
#######################################################
def save_example_to_tar(writer, sample: dict) -> None:
    """
    Save an image and its caption to a tar file, with additional metadata.

    The function processes the input sample, separating the image (`jpg`) and caption (`txt`) 
    from other metadata fields, then writes these components to the tar file.

    Parameters
    ----------
    writer : tarfile.TarFile
        The tarfile writer object responsible for writing data.
    sample : dict
        A dictionary containing the fields:
        - "jpg": Image file (bytes or path).
        - "txt": Caption associated with the image (string).
        - Additional metadata fields (optional).
    
    Raises
    ------
    Exception
        If an error occurs during encoding or writing to the tar file, it will be raised.
    """
    tar_sample = {}
    tar_sample["__key__"] = sample["__key__"]  
    tar_sample["jpg"]     = encode_image(sample["jpg"])
    tar_sample["txt"]     = sample['txt'].encode('utf-8')

    metadata = {k: v for k, v in sample.items() if k not in {"jpg", "txt", "__key__"}}
    tar_sample["json"] = json.dumps(metadata)
    writer.write(tar_sample)
   
def encode_image(image_path): 
    with Image.open(image_path) as img:
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG")
        image_bytes = buffer.getvalue()
    return image_bytes 

#######################################################
################### MAIN FUNCTION #####################
#######################################################
def generate_webdataset(
        filepath:str, 
        output_dir:str, 
        subset:str,
        labeler_dict_path:str,
        annotiation_dict_path:str,
        examples_per_shard:int=10000,
        range_:tuple=(0,2000)
        ):
    """
    Generate a webdataset from a dataset of PMC-OA data.

    Parameters
    ----------
    filepath : str
        Path to the root directory containing the downloaded data.
    output_dir : str
        Directory where the serialized webdataset will be saved.
    subset : str
        Subset of the data to process (one of "noncommercial", "commercial", "other").
    labeler_dict_path : str
        Path to the directory containing label dictionaries. 
        The label dictionary contains cluster labels as keys and a list of all images belonging to that cluster.
    annotiation_dict_path : str
        Path to the annotation dictionary. The annotation dictionary maps cluster labels to additional metadata:
        including:["panel_type","supanel_type","primary_label","secondary_label"].
    examples_per_shard : int, optional
        Number of examples to include in each shard, by default 10000.
    range_ : tuple, optional
        Range of cluster labels to process (inclusive), by default (0, 2000).

    Returns
    -------
    None
        This function does not return a value. It saves the generated webdataset shards to the specified output directory.

    Notes
    -----
    - The function assumes the data is structured in a specific format, including directories for JSON metadata and image files.
    - Each subset is serialized independently and saved in a tar format.
    - Images and metadata are validated to ensure consistency before being serialized.

    Raises
    ------
    AssertionError
        If the queried article's PMID does not match the expected value.

    Examples
    --------
    >>> generate_webdataset(
    ...     filepath="/data/pmc_oa",
    ...     output_dir="/output/webdataset",
    ...     subset="train",
    ...     labeler_dict_path="/data/label_dicts",
    ...     annotiation_dict_path="/data/annotations.json",
    ...     examples_per_shard=5000,
    ...     range_=(0, 1000)
    ... )
    """
    # Set Name and webdataset pattern 
    shard_index, count  = 0,0                   # Start counters
    root      = Path(filepath)                  # Path were downlad data is stored
    json_root = root/ "data"/ "json_entrez"     # Path to json 
    out_path  = os.path.join(output_dir,subset) # Path to store the seralized dataset. Remeber that each subset is seralized indepnedlty, thus saved data in subsey     
    os.makedirs(out_path, exist_ok=True)        # make output path if it does not exist already
    shard_pattern = str(Path(out_path) / f"{range_[0]}_{range_[1]}%06d.tar") # Start dataset pattern
    
    writer        = wds.TarWriter(shard_pattern % shard_index)                  # Start dataset writer
    label_dict    = load_json(os.path.join(labeler_dict_path,f"{subset}.json")) # Load label dict (maps image to label)
    taxonomy_dict = load_json(annotiation_dict_path)                            # Load annoation dict (maps a labele e.g. 0 to annoations)
   
    for cluster_label in tqdm(range(range_[0],range_[1]), desc="Processing clusters"):
        cluster_label = str(cluster_label)
        figure_list = label_dict[cluster_label] 
        for file in figure_list:
            # 1) Get arguments from filename
            args:str     = file.split("-")
            batch:str    = args[0] 
            pmid:str     = args[1]
            position:str = args[2]
            soft_id:str  = '-'.join(args[3:]) 
            # 2) Load File Batch and look for given figure set
            batch_path:str        = os.path.join(json_root,f"{batch}.json")   # Path to a batch (a collection of manuscripts stored as a list of dicts)
            bath_list:list[dict]  = load_json(batch_path,verbose=False)       # Load list of dicts 
            lookup:dict[str,dict] = {a['accession_id']: a for a in bath_list} # Create a lookup for the given batch
            query:dict[str,str]   = lookup.get(pmid)                          # Get article using the exctracted PMID
            assert query['accession_id'] ==  pmid                             # Ensure we retrived the correct article 
            figure:dict[str,str]  = query["figure_set"][int(position)]        # Get figure set from article 
            # 3) Populate data point 
            # 3.a) Set image properties
            datapoint:dict = dict()
            datapoint['image_file_name']:str             = str(figure['image_file_name'])
            datapoint['image_cluster_id']:str            = str(figure['image_id'])
            datapoint['image_hash']:str                  = str(figure['hash'])
            datapoint["jpg"]:str                         = str(root / figure["image_path"].replace("../", ""))
            datapoint["txt"]:str                         = str(figure.get('caption', ""))
            datapoint["image_label_id"]:str              = str(cluster_label)
            datapoint['image_panel_type']:str            = str(taxonomy_dict[cluster_label]['panel_type'])
            datapoint['image_panel_subtype']:str         = str(taxonomy_dict[cluster_label]['panel_subtype'])
            datapoint['image_primary_label']:list[str]   = taxonomy_dict[cluster_label]['global_class']
            datapoint['image_secondary_label']:list[str] = taxonomy_dict[cluster_label]['local_class']
            # Ensure Image File exists:
            if Path(datapoint["jpg"]).is_file():
                try:
                    image_size:tuple = Image.open(datapoint["jpg"]).size
                    datapoint["image_size"]:list  = [image_size[0], image_size[1]]
                except:
                    datapoint["image_size"]:list = [0, 0]
                    print("*"*10)
                    print("*"*10)
                    print(f"Could Not get image size for {figure['image_file_name']}")
                    print("*"*10)
                    print("*"*10)
            else:
                datapoint["image_size"]:list = [0, 0] 
        
            datapoint['image_set']:dict               = query.get('figure_set',{})
            datapoint['image_context']:dict           = query.get('context',{})
            # 3.b) Set additionaal details
            datapoint["article_accession_id"]         =  str(query.get('accession_id',""))
            datapoint["article_title"]:str            =  str(query.get("title",""))
            datapoint["article_journal"]:str          =  str(query.get("journal",""))
            datapoint["article_date"]:str             =  str(query.get("date",""))
            datapoint['article_abstract']:str         =  str(query.get('abstract',""))
            datapoint['article_mesh_terms']:list[str] =  query.get('mesh',[""])
            datapoint["article_keywords"]:list[str]   =  query.get('keyword',[""])
            datapoint['article_reference_ids']:list         = query.get('reference_ids',[""])
            datapoint['article_reference_count']:str        = query.get('reference_count',[""])
            datapoint['article_reference_list']:list[dict]  = query.get('reference_list',[""])
            
            try: 
                article_subject = query['article_categories'].get('subj-group',['']) 
                if isinstance(article_subject, dict):
                    datapoint["article_subject"]:str = [article_subject.get('subject','')]
                elif isinstance(article_subject, list):
                    datapoint["article_subject"]:str = [s.get('subject','') for s in article_subject]
                else:
                    datapoint["article_subject"]:str = ['']
            except:
                datapoint["article_subject"]:str = [''] 
                print("*"*10);print("*"*10);print(f"Could Not get artile subject for {figure['image_file_name']}");print("*"*10);print("*"*10)
                
            datapoint['article_citation']:str  = str(query.get('citation',""))
            datapoint["article_license"]:str   = str(query.get("license",""))
            datapoint['article_text']:str      = str(query.get('nxml',[""]))
            # 3.c) Set unique instance key
            datapoint["__key__"]  = file
            if datapoint["txt"]  and  isinstance(datapoint["txt"] , str) and datapoint["txt"]  != 'No caption found' and datapoint["jpg"] and  int(image_size[0]) > 20 and int(image_size[1]) > 20:
                try:
                    save_example_to_tar(writer,sample=datapoint) 
                    count += 1
                except:
                    print("*"*10);print("*"*10)
                    print(f"Could not be serialized {figure['image_file_name']}")
                    print("*"*10);print("*"*10)
                    
            #If samples per shard surpassess specifed number, create a new shard
            if count >= examples_per_shard:
                count = 0
                shard_index += 1
                writer.close()
                writer = wds.TarWriter(shard_pattern % shard_index)
                print("Created new Tar")
    
    # If we reach the final datapoint to serailizae, but not the number of samples per shard
    # This two last lines make sure that writer is close correclty
    if count > 0:
        writer.close()

#######################################################
################ ARGUMENT PARSING #####################
####################################################### 
def main():
    parser = argparse.ArgumentParser(description='Generate a WebDataset from images and captions.')
    parser.add_argument('input_dir',               type=str, help='The input directory containing the dataset (JSON and images).')
    parser.add_argument('output_dir',              type=str, help='The output directory where the resulting tar files will be saved.')
    parser.add_argument('--subset',                type=str, help='THe subset to serialzie')
    parser.add_argument('--label_dict_path'       ,type=str, help='This path contains a path to file contianing a dict that maps cluster id to image file paths')
    parser.add_argument('--annotiation__dict_path',type=str, help='This path contains a path to file contianing a dict that maps cluster id to annotations')
    parser.add_argument('--examples_per_shard',    type=int, default=10000, help='Number of examples per shard (default: 1000).')
    parser.add_argument('--range',                 type=int, nargs=2, metavar=('INIT', 'FINISH'), help='Two numbers specifying the initial and final range')
    args = parser.parse_args()

    # Format Range to tupple before running code
    init, finish = args.range if args.range else (0, 2000)
    range_:tuple =(init, finish) 
    print("*"*20);print(f"Running Serialization from  labels {range_[0]} to {range_[1]}");print("*"*20)
    
    # Run Serialzaition
    generate_webdataset(
        filepath              = args.input_dir, 
        output_dir            = args.output_dir,
        subset                = args.subset,
        labeler_dict_path     = args.label_dict_path,
        annotiation_dict_path = args.annotiation__dict_path,
        examples_per_shard    = args.examples_per_shard,
        range_                = range_)
 
if __name__ == '__main__':
    main()