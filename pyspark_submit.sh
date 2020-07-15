#!/bin/bash
#SBATCH --mail-user=P_WADHWA@encs.concordia.ca
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL
#SBATCH --mail-type=REQUEUE
#SBATCH --mail-type=ALL
#SBATCH --account=def-glatard
#SBATCH --time=00:50:00
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --mem=100G
#SBATCH --cpus-per-task=32
#SBATCH --ntasks-per-node=1


module load python/3.7
module load spark/2.4.4

source ~/ENV/bin/activate



export PYSPARK_PYTHON="/home/pulkit18/ENV/bin/python"
export PYSPARK_DRIVER_PYTHON="/home/pulkit18/ENV/bin/python"

export PYTHONPATH=${PYTHONPATH}:${PWD}

export MKL_NUM_THREADS=1
export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR
export SLURM_SPARK_MEM=$(printf "%.0f" $((${SLURM_MEM_PER_NODE} *95/100)))


#start master
start-master.sh
sleep 20

MASTER_URL_STRING=$(grep -Po '(?=spark://).*' $SPARK_LOG_DIR/spark-${SPARK_IDENT_STRING}-org.apache.spark.deploy.master*.out)

IFS=' '
read -ra MASTER_URL <<< "$MASTER_URL_STRING"
echo "master url :" ${MASTER_URL}

NWORKERS=$((SLURM_NTASKS - 1))

echo "----->" ${NWORKERS}
echo "----->" ${SPARK_LOG_DIR}
echo "----->" ${SLURM_CPUS_PER_TASK}
echo "----->" ${MASTER_URL}
echo "----->" ${SLURM_SPARK_MEM}

SPARK_NO_DAEMONIZE=1 srun -n ${NWORKERS} -N ${NWORKERS} --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m ${SLURM_SPARK_MEM}M -c ${SLURM_CPUS_PER_TASK} ${MASTER_URL} &
slaves_pid=$!

acc=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/accidents_montreal.py
eval=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/evaluate.py
exprtres=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/export_results.py
preprocess=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/preprocess.py
random_forest=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/random_forest.py
random_undersampler=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/random_undersampler.py
rnid=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/road_network_nids.py
road=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/road_network.py
solar_features=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/solar_features.py
utils=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/utils.py
weather=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/weather.py
wrkdir=/home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/workdir.py



srun -n 1 -N 1 spark-submit --master ${MASTER_URL} --executor-memory ${SLURM_SPARK_MEM}M /home/pulkit18/projects/def-glatard/pulkit18/accident-prediction-montreal/main_train_urf.py --py-files ${acc} ${eval} ${exprtres} ${preprocess} ${random_forest} ${random_undersampler} ${rnid} ${road} ${solar_features} ${utils} ${weather} ${wrkdir}

kill $slaves_pid
stop-master.sh
