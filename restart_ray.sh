#!/bin/bash

my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
# get cluster info
worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
slaves=()
for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
all_nodes=($my_address ${slaves[@]})

ray stop
for node in ${slaves[@]}; do
 echo "=> $node"
 ssh -o StrictHostKeyChecking=no $node PATH=$PATH:/home/ubuntu/anaconda3/bin:/home/ubuntu/anaconda3/condabin, ray stop &
done
wait

ray start --head --redis-port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --resources='{"node":1}' --object-store-memory=34359738368
sleep 5
for node in ${slaves[@]}; do
 echo "=> $node"
 ssh -o StrictHostKeyChecking=no $node PATH=$PATH:/home/ubuntu/anaconda3/bin:/home/ubuntu/anaconda3/condabin, ray start --redis-address=$my_address:6379 --object-manager-port=8076 --resources=\'{\"node\":1}\' --object-store-memory=34359738368 &
done
wait
