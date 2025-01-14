#!/bin/bash

# Set the root path and subset path
data_path="/pasteur/u/ale9806/Repositories/pmc-oa/DinoFeatures"  # Replace with your actual root path
subset="noncommercial"
subset_path="${data_path}/${subset}"

# Iterate over all directories in the subset path
for dir_name in "$subset_path"/*; do
    if [ -d "$dir_name" ]; then  # Check if it's a directory
        features_path="${dir_name}/features.npy"
        images_path="${dir_name}/images.npy"
        last_folder_name=$(basename "$dir_name")  # Get the last folder name

        # Optional: check if these files exist
        if [ -f "$features_path" ] && [ -f "$images_path" ]; then
            echo "Found features and images files in: $dir_name"
            echo "Submitting job"
    
            sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=${last_folder_name}_labeler
#SBATCH --output=slurm_logs_labeler/%x.out
#SBATCH --error=slurm_logs_labeler/%x.err
#SBATCH --mem=10G
#SBATCH -c 5
#SBATCH -p pasteur
#SBATCH -A pasteur
#SBATCH --time=48:00:00

# Commands to run for each job
python ../03_get_label_dict.py \
    --mode_root "/pasteur/u/ale9806/Repositories/pmc-oa/scripts/Labeler/models" \
    --features_root ${dir_name} \
    --kmeans_model "kmeans_2000.joblib" \
    --pca_model "pca_2000.joblib" \
    --subset "None" \
    --batch_size 262144   
EOF
        fi
    fi
done
