#!/bin/bash

if [ -z "$3" ]; then echo "ERROR: test name, node number and input size required"; exit; fi

SCRIPT_DIR=$(dirname $(realpath -s $0))
TEST_UNILS_DIR=$(realpath -s $SCRIPT_DIR/../../test_utils)
source $TEST_UNILS_DIR/load_cluster_env.sh

test_name=$1 # can be allgather/allreduce/gather/multicast/reduce
make $test_name > /dev/null

test_executable=$test_name
test_executable_abspath=$(realpath -s $test_executable)
world_size=$2
object_size=$3

# create logging dir
log_dir=$SCRIPT_DIR/log/$(date +"%Y%m%d-%H%M%S")-$test_name-$world_size-$object_size
mkdir -p $log_dir
ln -sfn $log_dir/ $SCRIPT_DIR/log/latest

all_nodes=(${ALL_IPADDR[@]:0:$world_size})
all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

echo Number of nodes: $world_size "(actually ${#all_nodes[@]})", data size: $object_size
echo Nodes: ${all_nodes[@]} "("${#all_nodes[@]}")"

$TEST_UNILS_DIR/mpirun_pernode.sh $all_hosts \
    -x MPI_LOGGING_DIR="$log_dir" \
    test_wrapper.sh $test_executable_abspath $[$object_size/4]
