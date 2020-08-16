#!/bin/bash
if [ -z "$1" ]; then echo "ERROR: input size required"; exit; fi

make send_recv > /dev/null

source ../load_cluster_env.sh
all_nodes=(${ALL_IPADDR[@]:0:2})  # only pick 2 nodes
all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

echo data size: $1
echo Nodes: ${all_nodes[@]}

../mpirun_pernode.sh $all_hosts $(realpath -s send_recv) $[$1/4]
