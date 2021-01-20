#!/bin/bash
if [ "$#" -lt 1 ]; then 
    echo "kill the current rank";
    kill $(cat /tmp/hoplite_test.pid)
fi
if [ "$#" -gt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"; exit -1; fi

## setup
ROOT_DIR=$(dirname $(realpath -s $0))

# get cluster info
source $ROOT_DIR/load_cluster_env.sh

echo "kill rank $1"
ssh ${ALL_IPADDR[$1]} $(realpath -s $0)
