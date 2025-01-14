#!/bin/bash
#SBATCH --job-name=01_nonc
#SBATCH --output=slurm_logs/%x.out
#SBATCH --error=slurm_logs/%x.err
#SBATCH --mem=300gb
#SBATCH -c 5
#SBATCH -p pasteur
#SBATCH -A pasteur
#SBATCH--gres=gpu:a6000
#SBATCH --time=48:00:00
#SBATCH --ntasks=1

python ../01_get_features.py --subset other --from_n_to_m [0,10]
