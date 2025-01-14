import os
import sys
import time
import joblib
import random
import argparse

import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt

from pmc_oa.utils            import get_memory_usage
from pmc_oa.utils.json_utils import save_json


def group_list(l, group_size):
    """
    :param l:           list
    :param group_size:  size of each group
    :return:            Yields successive group-sized lists from l.
    """
    for i in range(0, len(l), group_size):
        yield l[i:i+group_size]


def main(args):
    ####################################################### 
    ###################### Load Data ######################
    #######################################################
    if  args.subset != "None":
        data_path = os.path.join(args.features_root, args.subset)
    else:
        data_path = os.path.join(args.features_root)

    features_path = os.path.join(data_path, "features.npy")
    features      = np.load(features_path, mmap_mode='r')

    key_path = os.path.join(data_path, "keys.npy")
    keys     = np.load(key_path, mmap_mode='r')

    assert len(keys) == len(features)
    
    feature_batches = group_list(features, args.batch_size)
    keys_batches    = group_list(keys, args.batch_size)
    
    ####################################################### 
    ###################### Load Models ####################
    ####################################################### 
    pca    = joblib.load(os.path.join(args.mode_root, args.pca_model))
    kmeans = joblib.load(os.path.join(args.mode_root, args.kmeans_model))

    ####################################################### 
    ###################### Get Labels #####################
    ####################################################### 
    total_dict = {}
    for feature_batch, keys_batch in tqdm(zip(feature_batches, keys_batches)):
        reduced_features = pca.transform(feature_batch)       # Apply PCA
        labels = kmeans.predict(reduced_features)             # Apply KMeans
        total_dict.update({key: int(label) for key, label in zip(keys_batch, labels)})

    label_path = os.path.join(data_path, "label_dict.json")
    save_json(total_dict,label_path)

    ####################################################### 
    ################ Get Memory Usage #####################
    ####################################################### 
    get_memory_usage("Total Memory Usage")
    dict_size = sys.getsizeof(total_dict) / (1024 ** 3)
    print(f"Label Dict Usage: {dict_size} GB")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process features with PCA and KMeans")
    parser.add_argument("--kmeans_model",  type=str, default="kmeans_1000.joblib", help="Path to KMeans model file")
    parser.add_argument("--pca_model",     type=str, default="pca_1000.joblib",    help="Path to PCA model file")
    parser.add_argument("--mode_root",     type=str, default="/pasteur/u/ale9806/Repositories/pmc-oa/scripts/Labeler/models", help="Model")
    parser.add_argument("--features_root", type=str, default="/pasteur/u/ale9806/Repositories/pmc-oa/DinoFeatures", help="Features root")
    parser.add_argument("--subset",        type=str, default="commercial", help="Subset directory name")
    parser.add_argument("--batch_size",    type=int, default=262144,       help="Batch size for processing features")
    args = parser.parse_args()
    main(args)