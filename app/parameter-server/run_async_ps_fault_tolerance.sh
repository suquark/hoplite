#!/bin/bash
export RAY_BACKEND_LOG_LEVEL=info

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null
sudo fuser -k 20210/tcp -s &> /dev/null
sleep 1

n_nodes=7
model=resnet50

echo "==========" async-ps-$n_nodes-$model-hoplite "=========="
python hoplite_asgd_fault_tolerance.py -n $(($n_nodes - 1)) -a $((($n_nodes - 1) / 2)) -m $model --iterations 100
sleep 1

echo "==========" async-ps-$n_nodes-$model-ray "=========="
python ray_asgd_fault_tolerance.py -n $(($n_nodes - 1)) -a $((($n_nodes - 1) / 2)) -m $model --iterations 100
