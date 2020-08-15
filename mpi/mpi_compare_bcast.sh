#!/bin/bash
if [ -z "$2" ]; then echo "ERROR: number of nodes & input size required"; exit; fi

make compare_bcast > /dev/null

source ../load_cluster_env.sh
all_nodes=(${ALL_IPADDR[@]:0:$1})
all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

echo Number of nodes: $1 "(actually ${#all_nodes[@]})", data size: $2
echo Nodes: ${all_nodes[@]}

../mpirun_pernode.sh $all_hosts $(realpath -s compare_bcast) $[$2/4] 1
