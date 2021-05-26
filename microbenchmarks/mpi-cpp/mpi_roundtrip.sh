#!/bin/bash
if [ -z "$1" ]; then echo "ERROR: input size required"; exit; fi

make roundtrip > /dev/null

SCRIPT_DIR=$(dirname $(realpath -s $0))
TEST_UNILS_DIR=$(realpath -s $SCRIPT_DIR/../../test_utils)
source $TEST_UNILS_DIR/load_cluster_env.sh

all_nodes=(${ALL_IPADDR[@]:0:2})  # only pick 2 nodes
all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

echo data size: $1
echo Nodes: ${all_nodes[@]}

$TEST_UNILS_DIR/mpirun_pernode.sh $all_hosts $(realpath -s roundtrip) $[$1/4]
