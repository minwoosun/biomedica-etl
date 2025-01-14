#!/bin/bash

##################
#   Parameters   #
##################

# Initialize batch index and batch directory
LICENSE="commercial"
BATCH_COUNT=3 # Total number of batches
BATCHES_PER_NODE=3 
BATCH_DIR="./data/file_lists/${LICENSE}/"

# Cluster parameters
nodes=("pasteur3")

# # license type
# if [[ $# -gt 0 ]]; then
#   LICENSE="$1"
# fi

# echo ${LICENSE}

# LICENSE="noncommercial" # "noncommercial", "other"

###################
#   Run program   #
###################

# Create the slurm_logs directory if it does not exist
mkdir -p slurm_logs
    
# submit 3 jobs for a node
for (( node_idx=0; node_idx<${#nodes[@]}; node_idx++ )); do
  node=${nodes[$node_idx]}

  # Calculate the starting and ending batch indices for this node
  start_batch_idx=$((node_idx * BATCHES_PER_NODE))
  end_batch_idx=$((start_batch_idx + BATCHES_PER_NODE - 1))

  # Submit 3 jobs to the current node
  for (( batch_idx=$start_batch_idx; batch_idx<=$end_batch_idx; batch_idx++ )); do

    sbatch --nodelist=$node <<< \
"#!/bin/bash
#SBATCH --job-name=batch-${batch_idx}
#SBATCH --output=slurm_logs/batch-${batch_idx}-%j-out.txt
#SBATCH --error=slurm_logs/batch-${batch_idx}-%j-err.txt
#SBATCH --mem=16gb
#SBATCH -c 1  # Number of CPU cores
#SBATCH -p pasteur
#SBATCH -A pasteur
#SBATCH --time=48:00:00
#SBATCH --ntasks=1

# Construct the command for this specific batch
cmd=\"python scripts/03_download_batch_ftp.py --data_dir ./data/ --license \"${LICENSE}\" --filelist_batch_dir ${BATCH_DIR} --batch_idx $batch_idx\"

# Print and run the command
echo \"Constructed Command:\n\$cmd\"
eval \"\$cmd\"
"
  done

done