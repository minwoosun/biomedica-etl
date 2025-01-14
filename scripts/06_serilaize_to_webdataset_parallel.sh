#!/bin/bash

##########################################
###    1) Select subset to serialize   ###
# THIS STEP REQUIERES HUMAN INTERVENTION #
##########################################
# 
# Select one from ["other","commercial","noncommercial"]:
subset="noncommercial"
#subset="other"
#subset="commercial"

#####################################
###          2) Select step       ###
#####################################
# I recommend:
# 10 for commercial 
# 40 for noncommercial
# 200 for other
step=40

#####################################
###         3) Load dot env       ###
#####################################
if [ -f .env ]; then
    source .env
fi

#####################################
###  4) Submission SLURM Script   ###
#####################################
mkdir -p $serialization_slurm_log
echo "Submitting jobs for subset ${subset}"

for ((start=0; start<$NUM_CLUSTERS; start+=step)); do
    end=$((start+step))
    echo "Submitting job for range ${start},${end}"
    sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=${subset}_${start}_${end}
#SBATCH --output=$serialization_slurm_log/%x.out
#SBATCH --error=$serialization_slurm_log/%x.err
#SBATCH --mem=5G
#SBATCH -c 1
#SBATCH -p $node
#SBATCH -A $node
#SBATCH --nodelist=pasteur2,pasteur3,pasteur4
#SBATCH --time=72:00:00

python 06_serilaize_to_webdataset_parallel.sh \
${json_dump_root}${subset}/pmc-oa/ \
$serialization_root \
--subset ${subset} \
--range ${start} ${end} \ 
--labeler_dict_path $labeler_dict_path \
--annotiation_dict_path $annotiation_dict_path \ 
EOF
done