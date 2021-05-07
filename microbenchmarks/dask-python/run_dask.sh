#!/bin/bash
if [ "$#" -lt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) number of nodes"; exit -1; fi
if [ "$#" -gt 3 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"; exit -1; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

SCRIPT_DIR=$(dirname $(realpath -s $0))
TEST_UNILS_DIR=$(realpath -s $SCRIPT_DIR/../../test_utils)
world_size=$1

if [ "$#" -eq 1 ]; then
    source $TEST_UNILS_DIR/load_cluster_env.sh
    OTHERS_IPADDR=(${OTHERS_IPADDR[@]:0:$(($world_size-1))})
    
    dask-scheduler &
    sleep 1

    for index in ${!OTHERS_IPADDR[@]}
    do
        rank=$((index+1))
        ssh -t -t ${OTHERS_IPADDR[$index]} "$(realpath -s $0) $MY_IPADDR $rank" &
    done

    dask-worker $MY_IPADDR:18786 --name Dask-0
else
    master=$1
    index=$2
    dask-worker $master:18786 --name Dask-$index
fi
