# How to Add Labels to PMC-OA?

To add labels to your dataset, follow these steps:

1. **Prepare Data**  
   Ensure all subsets have been serialized to a WebDataset using the provided serialization scripts.

2. **Run Scripts in Sequence**  
   - Run `01_get_features.py` to extract DinoV2 features for each image in every subset (requires GPU).
   - Run `02_train_labelers.py` to train a labeler.
   - Run `03_get_label_dict.py` to generate a label dictionary for each data point.
   - Run `06_serialize` to serialize the dataset with label propagation enabled.

[Optional] If your cluster uses SLURM, feel free to use the job submission files in `sbatch` for running the scripts.
 
3. **Label Verification (Optional)**  
   If you are training a new labeler from scratch, it’s recommended to verify the labels with clinicians and scientists.  
   If you are using the provided labeler, please refer to the [TODO section] for further instructions.
