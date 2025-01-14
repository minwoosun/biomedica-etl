import os
import time

import joblib
import numpy as np
from tqdm import tqdm

from pmc_oa.utils                  import get_memory_usage
from pmc_oa.data_loaders.wd_loader import collect_tar_files,filter_no_caption_or_no_image
from pmc_oa.feature_extractors     import extract_features_from_dataloader

from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.cluster import DBSCAN





def get_pca(
    features: np.array,
    from_file:str = None,
    save_to: str  = None,
    n_components: int = 45) -> np.array:
    """
    Performs Principal Component Analysis (PCA) on the input features and reduces
    their dimensionality to a specified number of components.

    Parameters
    ----------
    features : np.array
        The input array of features to be reduced, where rows represent samples 
        and columns represent features.
    save_to : str, optional
        Path to save the PCA model object using joblib. If False, the model is 
        not saved.
    n_components : int, default=45
        The number of principal components to keep.

    Returns
    -------
    np.array
        An array of features reduced to the specified number of principal components.
    
    Notes
    -----
    - Prints the elapsed time for the PCA computation.
    - Uses sklearn's PCA with `n_components` specified by the user.
    
    Examples
    --------
    >>> import numpy as np
    >>> features = np.random.rand(100, 50)
    >>> reduced_features = get_pca(features, save_to="pca_model.joblib", n_components=30)
    """
    start_time = time.time()

    if from_file:
        pca:np.array = joblib.load(from_file)
        rfs:np.array = pca.transform(features) 
    else:
        pca:np.array = PCA(n_components=n_components)
        rfs:np.array = pca.fit_transform(features)  

    if save_to:
        joblib.dump(pca, save_to)
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"PCA Elapsed time: {elapsed_time:.2f} seconds")

    return rfs


def get_kmeans(
    features: np.array,
    num_clusters: int = 1000,
    save_to: str = False,
    from_file: str = False) -> np.array:
    """
    Applies K-Means clustering on the input features and returns cluster labels.
    If a saved model is specified, it loads the model and applies it to the features.

    Parameters
    ----------
    features : np.array
        The input array of features to cluster, where rows represent samples.
    num_clusters : int, default=1000
        The number of clusters to form.
    save_to : str, optional
        Path to save the K-Means model object using joblib. If False, the model 
        is not saved.
    from_file : str, optional
        Path to a saved K-Means model file. If provided, the model is loaded 
        and used to predict cluster labels, bypassing the training step.

    Returns
    -------
    np.array
        An array of cluster labels for each sample.
    
    Notes
    -----
    - Prints the elapsed time for the K-Means clustering computation.
    - If `from_file` is provided, the function will skip training and load the 
      model from the file path.

    Examples
    --------
    >>> import numpy as np
    >>> features = np.random.rand(1000, 50)
    >>> labels = get_kmeans(features, num_clusters=10, save_to="kmeans_model.joblib")
    >>> # Using a saved model
    >>> labels = get_kmeans(features, from_file="kmeans_model.joblib")
    """
    start_time = time.time()
    
    if from_file:
        # Load the model if from_file is provided
        kmeans = joblib.load(from_file)
        labels = kmeans.predict(features)
    else:
        # Fit a new model if from_file is not provided
        kmeans = KMeans(n_clusters=num_clusters)
        kmeans.fit(features)
        labels = kmeans.labels_

        # Save the model if save_to path is provided
        if save_to:
            joblib.dump(kmeans, save_to)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"KMEANS Elapsed time: {elapsed_time:.2f} seconds")
    return labels

def get_dbscan(
    features: np.array,
    eps: float = 0.5,
    min_samples: int = 1000,
    save_to: str = False,
    from_file: str = False) -> np.array:
    """
    Applies DBSCAN clustering on the input features and returns cluster labels.
    If a saved model is specified, it loads the model and applies it to the features.

    Parameters
    ----------
    features : np.array
        The input array of features to cluster, where rows represent samples.
    eps : float, default=0.5
        The maximum distance between two samples for them to be considered as in the same neighborhood.
    min_samples : int, default=5
        The number of samples in a neighborhood for a point to be considered as a core point.
    save_to : str, optional
        Path to save the DBSCAN model object using joblib. If False, the model is not saved.
    from_file : str, optional
        Path to a saved DBSCAN model file. If provided, the model is loaded and used to predict
        cluster labels, bypassing the training step.

    Returns
    -------
    np.array
        An array of cluster labels for each sample.
    
    Notes
    -----
    - Prints the elapsed time for the DBSCAN clustering computation.
    - If `from_file` is provided, the function will skip training and load the model 
      from the file path.

    Examples
    --------
    >>> import numpy as np
    >>> features = np.random.rand(1000, 50)
    >>> labels = get_dbscan(features, eps=0.3, min_samples=10, save_to="dbscan_model.joblib")
    >>> # Using a saved model
    >>> labels = get_dbscan(features, from_file="dbscan_model.joblib")
    """
    start_time = time.time()
    
    if from_file:
        # Load the model if from_file is provided
        dbscan = joblib.load(from_file)
        labels = dbscan.fit_predict(features)
    else:
        # Fit a new model if from_file is not provided
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
        labels = dbscan.fit_predict(features)

        # Save the model if save_to path is provided
        if save_to:
            joblib.dump(dbscan, save_to)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"DBSCAN Elapsed time: {elapsed_time:.2f} seconds")
    return labels
    
num_clusters:int = 2000

get_memory_usage("Before Runing Labeler")
features = np.load("/pasteur/u/ale9806/Repositories/pmc-oa/DinoFeatures/all/features.npy", mmap_mode='r')
#images   = np.load("/pasteur/u/ale9806/Repositories/pmc-oa/notebooks/DinoFeatures/images_3000.npy")
get_memory_usage("Before Runing Labeler")



#reduced_features = get_pca(
#    features  = features ,
#    from_file ='../pca_model.joblib')


reduced_features = get_pca(
    features  = features,
     save_to  = f"../pca_{num_clusters}")

_ = get_kmeans(
    features = reduced_features,
    num_clusters = num_clusters,
    save_to= f"../kmeans_{num_clusters}")
