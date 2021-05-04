#!/bin/bash
if [ "$#" -lt 4 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) test name, number of nodes, input size & n_trials required"; exit -1; fi
if [ "$#" -gt 4 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"; exit -1; fi

## setup
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT
SCRIPT_DIR=$(dirname $(realpath -s $0))
TEST_UNILS_DIR=$(realpath -s $SCRIPT_DIR/../../test_utils)
BINARIES_DIR=$(realpath -s $SCRIPT_DIR/../../build)

## cleanup procs
sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null
sudo fuser -k 20210/tcp -s &> /dev/null

test_name=$1
world_size=$2
object_size=$3
n_trials=$4

# get cluster info
source $TEST_UNILS_DIR/load_cluster_env.sh
OTHERS_IPADDR=(${OTHERS_IPADDR[@]:0:$(($world_size-1))})
echo "$(tput setaf 2)[INFO]$(tput sgr 0) head_node: $MY_IPADDR; other_nodes: ${OTHERS_IPADDR[@]}"

# prompt test info
echo "$(tput setaf 2)[INFO]$(tput sgr 0) Running test $(tput setaf 3)$(tput bold)$test_name$(tput sgr 0)"

# create logging dir
log_dir=$SCRIPT_DIR/log/$(date +"%Y%m%d-%H%M%S")-$test_name-$world_size-$object_size
mkdir -p $log_dir
ln -sfn $log_dir/ $SCRIPT_DIR/log/latest

export RAY_BACKEND_LOG_LEVEL=info

# pkill notification
# sleep 0.5
# ($BINARIES_DIR/notification 2>&1 | tee $log_dir/$MY_IPADDR.notification.log) &
# sleep 0.5

all_nodes=(${ALL_IPADDR[@]:0:$world_size})
all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

$TEST_UNILS_DIR/mpirun_pernode.sh $all_hosts \
    -x HOPLITE_LOGGING_DIR=$log_dir \
    -x RAY_BACKEND_LOG_LEVEL=$RAY_BACKEND_LOG_LEVEL \
    test_wrapper.sh python hoplite_microbenchmarks.py $test_name -s $object_size

sleep 1
