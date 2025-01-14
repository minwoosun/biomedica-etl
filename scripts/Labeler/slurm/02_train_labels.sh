#!/bin/bash
#SBATCH --job-name=labeler
#SBATCH --output=slurm_logs/%x.out
#SBATCH --error=slurm_logs/%x.err
#SBATCH --mem=500gb
#SBATCH -c 5
#SBATCH -p pasteur
#SBATCH -A pasteur
#SBATCH --time=48:00:00
#SBATCH --ntasks=1

python ../02_train_labelers.py