#BSUB -n 1
#BSUB -q training
#BSUB -J Main[1-9]
#BSUB -o Main.%J.out
#BSUB -e Main.%J.err

module load anaconda/3
source /apps/anaconda/anaconda3/etc/profile.d/conda.sh
conda deactivate
conda activate ../env

python main.py $LSB_JOBINDEX 9 100000
