#!/bin/bash

if [ -z "$3" ]; then echo "ERROR: test name, node number and input size required"; exit; fi

test_name=$1 # can be allgather/allreduce/gather/multicast/reduce
root_dir=$(dirname $(realpath -s $0))/../
test_executable=$test_name
test_executable_abspath=$(realpath -s $test_executable)
world_size=$2
object_size=$3

make $test_name > /dev/null

worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)

my_address=$($root_dir/get_ip_address.sh)

slaves=()
for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s $root_dir/get_ip_address.sh)); done
slaves=(${slaves[@]:0:$(($world_size-1))})

all_nodes=($master ${slaves[@]})

echo Number of nodes: $world_size "(actually ${#all_nodes[@]})", data size: $object_size
echo Nodes: ${all_nodes[@]} "("${#all_nodes[@]}")"

all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

mpirun --map-by ppr:1:node -hosts $all_hosts $test_executable_abspath $[$object_size/4]
