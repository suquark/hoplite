#!/bin/bash

# This script can run commands on all nodes on the cluster: ./fornode <commands>

root_dir=$HOME/efs/object_store/
source $root_dir/load_cluster_env.sh

PIDS=()

redis-server --port 7777 --protected-mode no &

REDIS_PID=$!

i=0
echo $MY_IPADDR
for node in ${ALL_IPADDR[@]}; do
  echo "=> $node $i"
  ssh -o StrictHostKeyChecking=no $node PATH=$PATH:/home/ubuntu/anaconda3/bin:/home/ubuntu/anaconda3/condabin, \
  /home/ubuntu/efs/gloo/build/gloo/benchmark/benchmark \
  --size 4 \
  --rank $i \
  --redis-host $MY_IPADDR \
  --redis-port 7777 \
  --prefix initial-benchmark \
  --transport tcp \
  --elements -1 \
  --iteration-time 1s \
  allreduce_ring_chunked &
  PIDS+=($!)
  i=$(($i + 1))
done
wait ${PIDS[@]}
kill $REDIS_PID



