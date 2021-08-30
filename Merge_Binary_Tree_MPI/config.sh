#BSUB -n 5
#BSUB -q training
#BSUB -J Main
#BSUB -o Main.%J.out
#BSUB -e Main.%J.err

module load anaconda/3
source /apps/anaconda/anaconda3/etc/profile.d/conda.sh
conda deactivate
conda activate ../env

mpirun -np 5 python main.py 200000
