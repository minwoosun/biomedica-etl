#!/bin/bash

#SBATCH --job-name=entrez
#SBATCH --output=slurm_logs/%j-out.txt
#SBATCH --error=slurm_logs/%j-err.txt
#SBATCH --mem=2gb
#SBATCH -c 1
#SBATCH -p pasteur
#SBATCH -A pasteur
#SBATCH --time=48:00:00
#SBATCH --ntasks=1

EMAIL="..."
API="..."
LICENSE="commercial"

python scripts/05_json_to_entrez.py \
    --csv-path "./data/entrez/${LICENSE}_json_paths.csv" \
    --out-dir-path "./data/json_entrez/${LICENSE}/" \
    --completed-log-file-path "./data/entrez/logs/completed_${LICENSE}.csv" \
    --error-log-file-path "./data/entrez/logs/error_${LICENSE}.csv" \
    --email "$EMAIL" \
    --api "$API"