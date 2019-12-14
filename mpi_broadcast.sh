#!/bin/bash
if [ -z "$1" ]; then echo "ERROR: input size required"; exit; fi

worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)

master=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')

slaves=()
for s in $worker_pubips; do slaves+=($(ssh $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done

echo $master
echo ${slaves[@]}

all_nodes=(${slaves[@]})
all_nodes+=($master)
echo ${all_nodes[@]}, ${#all_nodes[@]}

all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

mpirun --map-by ppr:1:node -hosts $all_hosts /home/ubuntu/efs/mpitutorial/tutorials/mpi-broadcast-and-collective-communication/code/compare_bcast $[$1/4] 10 
