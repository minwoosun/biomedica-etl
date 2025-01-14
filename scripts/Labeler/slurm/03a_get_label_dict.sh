#!/bin/bash
#SBATCH --job-name=get_ldict
#SBATCH --output=slurm_logs/%x.out
#SBATCH --error=slurm_logs/%x.err
#SBATCH --mem=40gb
#SBATCH -c 5
#SBATCH -p pasteur
#SBATCH -A pasteur
#SBATCH --time=48:00:00
#SBATCH --ntasks=1

python ../03_get_label_dict.py \
    --mode_root "/pasteur/u/ale9806/Repositories/pmc-oa/scripts/Labeler/models" \
    --features_root "/pasteur/u/ale9806/Repositories/pmc-oa/DinoFeatures" \
    --kmeans_model "kmeans_2000.joblib" \
    --pca_model "pca_2000.joblib" \
    --subset "other" \
    --batch_size 262144   


