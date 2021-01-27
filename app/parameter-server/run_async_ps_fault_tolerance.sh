#!/bin/bash
export RAY_BACKEND_LOG_LEVEL=info

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null
sudo fuser -k 20210/tcp -s &> /dev/null

sleep 1

ROOT_DIR=$(dirname $(realpath -s $0))/../../
source $ROOT_DIR/load_cluster_env.sh

n_nodes=8
model=resnet50

echo "==========" async-ps-$n_nodes-$model-hoplite "=========="
python hoplite_asgd_fault_tolerance.py -n $(($n_nodes - 1)) -a $((($n_nodes - 1) / 2)) -m $model
sleep 1

echo "==========" async-ps-$n_nodes-$model-ray "=========="
python ray_asgd_fault_tolerance.py -n $(($n_nodes - 1)) -a $((($n_nodes - 1) / 2)) -m $model
sleep 1
