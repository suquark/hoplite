#!/bin/bash

if [ "$#" -lt 3 ]; then
  echo "$(tput setaf 1)[ERROR]$(tput sgr 0) test name, number of nodes & input size required"
  echo "test name: allreduce_ring, allreduce_ring_chunked, allreduce_halving_doubling, "
  echo "           allreduce_bcube, barrier_all_to_all, broadcast_one_to_all, pairwise_exchange"
  exit -1
fi

if [ "$#" -gt 3 ]; then
  echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"
  exit -1
fi

# trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

test_name=$1
world_size=$2
object_size=$3

SCRIPT_DIR=$(dirname $(realpath -s $0))
TEST_UNILS_DIR=$(realpath -s $SCRIPT_DIR/../../test_utils)
GLOO_DIR=$SCRIPT_DIR/gloo/

source $TEST_UNILS_DIR/load_cluster_env.sh

# prepare logging directory
log_dir=$SCRIPT_DIR/log/$(date +"%Y%m%d-%H%M%S.%N")-$test_name-$world_size-$object_size
mkdir -p $log_dir
ln -sfn $log_dir/ $SCRIPT_DIR/log/latest

# gloo benchmarks requires Redis
redis-server --port 7799 --protected-mode no &> /dev/null &
REDIS_PID=$!
sleep 1
echo "IP address of this node: $MY_IPADDR"

all_nodes=(${ALL_IPADDR[@]:0:$world_size})
all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')
$TEST_UNILS_DIR/mpirun_pernode.sh $all_hosts \
    -x GLOO_DIR="$GLOO_DIR" \
    -x GLOO_LOGGING_DIR="$log_dir" \
    -x REDIS_HOST="$MY_IPADDR" \
    -x test_name="$test_name" \
    -x object_size="$object_size" \
    test_wrapper.sh

kill $REDIS_PID
