#!/bin/bash

ROOT_DIR=$(dirname $(realpath -s $0))
source $ROOT_DIR/load_cluster_env.sh

./fornode ray stop

ray start --head --redis-port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --resources='{"node":1}' --object-store-memory=34359738368
sleep 5
for node in ${OTHERS_IPADDR[@]}; do
 echo "=> $node"
 ssh -o StrictHostKeyChecking=no $node PATH=$PATH:/home/ubuntu/anaconda3/bin:/home/ubuntu/anaconda3/condabin, ray start --redis-address=$MY_IPADDR:6379 --object-manager-port=8076 --resources=\'{\"node\":1}\' --object-store-memory=34359738368 &
done
wait
