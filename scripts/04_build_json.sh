#!/bin/bash

##################
#   Parameters   #
##################

# Initialize batch index and batch directory
LICENSE="noncommercial"
BATCH_COUNT=6 # 160 Total number of batches (in this case 8 nodes * 20 jobs per node = 160 batches)
BATCHES_PER_NODE=2  # 20 batches per node
BATCH_DIR="./data/file_lists/${LICENSE}/"

# List of nodes (make sure these are the correct node names on your cluster)
nodes=(
  "pasteur1"
  "pasteur2"
  "pasteur3"
  # "pasteur4"
  # "pasteur5"
  # "pasteur6"
  # "pasteur7"
  # "pasteur8"
)

# Cluster parameters
partition="pasteur"
account="pasteur"

###################
#   Run program   #
###################

# Create the slurm_logs directory if it does not exist
mkdir -p slurm_logs_json
    
# Loop over each node and submit 8 jobs for each node
for (( node_idx=0; node_idx<${#nodes[@]}; node_idx++ )); do
  node=${nodes[$node_idx]}

  # Calculate the starting and ending batch indices for this node
  start_batch_idx=$((node_idx * BATCHES_PER_NODE))
  end_batch_idx=$((start_batch_idx + BATCHES_PER_NODE - 1))

  # Submit 8 jobs to the current node
  for (( batch_idx=$start_batch_idx; batch_idx<=$end_batch_idx; batch_idx++ )); do

    sbatch --nodelist=$node <<< \
"#!/bin/bash
#SBATCH --job-name=json-${batch_idx}
#SBATCH --output=slurm_logs_json/batch-${batch_idx}-%j-out.txt
#SBATCH --error=slurm_logs_json/batch-${batch_idx}-%j-err.txt
#SBATCH --mem=1gb
#SBATCH -c 1 
#SBATCH -p $partition
#SBATCH -A $account
#SBATCH --time=24:00:00
#SBATCH --ntasks=1

# Construct the command for this specific batch
cmd=\"python scripts/04_build_json.py --data_dir ./data/ --license ${LICENSE} --filelist_batch_dir ${BATCH_DIR} --batch_idx $batch_idx\"

# Print and run the command
echo \"Constructed Command:\n\$cmd\"
eval \"\$cmd\"
"
  done

done