#!/bin/bash

#######################
### 1) Load dot env ###
#######################
if [ -f .env ]; then
    source .env
fi

###############

mkdir -p $wb_HF_upload_slurm_log
echo "Submitting Web Dataset to HuggingFace"

sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=upload2hf
#SBATCH --output=$wb_HF_upload_slurm_log/%x.out
#SBATCH --error=$wb_HF_upload_slurm_log/%x.err
#SBATCH --mem=3G
#SBATCH -c $n_cpu_upload
#SBATCH -p $node
#SBATCH -A $node
#SBATCH --time=50:00:00

python 7_upload_wd_dataset.py --token $HF_API_TOKEN --repo_id $repo_id --folder_path $serialization_root --num_workers $num_workers
EOF