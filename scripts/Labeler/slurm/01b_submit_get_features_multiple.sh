#!/bin/bash

# Set the root path and calculate the end based on the number of items in that path
data_path='/pasteur2/u/ale9806/data/pmc-oa/full_panel_w_labels/' # Replace with your actual root path
subset='commercial'
subset_path="${data_path}${subset}"

# 1000-1100
start=1100
end=$(ls "$subset_path" | wc -l)
step=100  # Adjust this step to define each job's range increment

# Loop through the range and submit a job for each segment
for ((i=$start; i<$end; i+=$step)); do
    j=$((i + step))
    echo "Submitting job for range $i to $j"
    sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=${subset}_${i}_${j}
#SBATCH --output=slurm_logs_features/%x.out
#SBATCH --error=slurm_logs_features/%x.err
#SBATCH --mem=60G
#SBATCH -c 5
#SBATCH -p pasteur
#SBATCH -A pasteur
#SBATCH --gres=gpu:a6000
#SBATCH --time=48:00:00
#SBATCH --ntasks=1

python ../01_get_features.py --subset $subset --from_n_to_m $i $j
EOF
done
 
