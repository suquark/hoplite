#!/bin/bash

if [ -z "$3" ]; then echo "ERROR: test name, node number and input size required"; exit; fi

ROOT_DIR=$(dirname $(realpath -s $0))/../
source $ROOT_DIR/load_cluster_env.sh

test_name=$1 # can be allgather/allreduce/gather/multicast/reduce
test_executable=$test_name
test_executable_abspath=$(realpath -s $test_executable)
world_size=$2
object_size=$3

make $test_name > /dev/null

all_nodes=(${ALL_IPADDR[@]:0:$world_size})
all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

echo Number of nodes: $world_size "(actually ${#all_nodes[@]})", data size: $object_size
echo Nodes: ${all_nodes[@]} "("${#all_nodes[@]}")"

$ROOT_DIR/mpirun_pernode.sh $all_hosts $test_executable_abspath $[$object_size/4]
