import time
import joblib
import numpy as np
from tqdm import tqdm

from sklearn.cluster import KMeans
from sklearn.cluster import DBSCAN
from sklearn.decomposition import PCA


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