#!/bin/bash
if [ "$#" -lt 3 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) test name, number of nodes & input size required"; exit -1; fi
if [ "$#" -gt 6 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"; exit -1; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null

## setup
root_dir=$(dirname $(realpath -s $0))

test_name=$1
test_executable=$test_name
test_executable_abspath=$(realpath -s $test_executable)
world_size=$2
object_size=$3

if [ ! -f $test_executable_abspath ]; then
    echo "$(tput setaf 1)[ERROR]$(tput sgr 0) test executable not found: $test_executable_abspath"
    exit -2
fi

if [ "$#" -eq 3 ]; then
    # get cluster info
    source load_cluster_env.sh
    echo "$(tput setaf 2)[INFO]$(tput sgr 0) master: $MY_IPADDR; others: ${OTHERS_IPADDR[@]}"

    # prompt test info
    echo "$(tput setaf 2)[INFO]$(tput sgr 0) Running test $(tput setaf 3)$(tput bold)$test_name$(tput sgr 0)"

    # create logging dir
    log_dir=$root_dir/log/$(date +"%Y%m%d-%H%M%S")-$test_name-$world_size-$object_size
    mkdir -p $log_dir
    ln -sfn $log_dir/ $root_dir/log/latest

    pkill notification
    sleep 2
    (./notification $MY_IPADDR 2>&1 | tee $log_dir/$MY_IPADDR.notification.log) &
    sleep 2

    # execute remote commands
    for index in ${!OTHERS_IPADDR[@]}
    do
        rank=((index+1))
        ssh -t -t ${OTHERS_IPADDR[$index]} "$(realpath -s $0) $test_name $world_size $object_size $MY_IPADDR $rank $log_dir" &
    done
    # start local process (rank=0)
    $test_executable_abspath $MY_IPADDR $MY_IPADDR $world_size 0 $object_size 2>&1 | tee $log_dir/$MY_IPADDR.server.log
else
    sleep 5
    my_address=$(hostname -i)

    redis_address=$4
    rank=$5
    log_dir=$6
    echo "$(tput setaf 2)[INFO]$(tput sgr 0) Node($my_address) redis_address: $redis_address world_size: $world_size rank: $5 object_size: $object_size"
    $test_executable_abspath $redis_address $my_address $world_size $rank $object_size 2>&1 | tee $log_dir/$my_address.client.log
fi
