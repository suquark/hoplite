#!/bin/bash
if [ "$#" -lt 3 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) test name, number of nodes & input size required$"; exit -1; fi
if [ "$#" -gt 6 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"; exit -1; fi


trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null

## setup
my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
# plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &> /dev/null &
# sleep 2

test_name=$1
test_executable=$1_test
working_dir=$(dirname $(realpath -s $0))
test_executable_abspath=$working_dir/$test_executable
world_size=$2
object_size=$3

if [ ! -f $test_executable_abspath ]; then
    echo "$(tput setaf 1)[ERROR]$(tput sgr 0) test executable not found: $test_executable_abspath"
    exit -2
fi

if [ "$#" -eq 3 ]; then
    # prompt test info
    echo "$(tput setaf 2) Running test $(tput bold)$test_name ...$(tput sgr 0)"

    pkill notification
    sleep 2
    ./notification $my_address &
    sleep 2

    # get cluster info
    worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
    slaves=()
    for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
    slaves=(${slaves[@]:0:$(($world_size-1))})
    echo "$(tput setaf 2)[INFO]$(tput sgr 0) master: $my_address; slaves: ${slaves[@]}"

    # create logging dir
    log_dir=$working_dir/log/$(date +"%Y%m%d-%H%M%S")-$test_name-$world_size-$object_size
    mkdir -p $log_dir

    # execute remote commands
    for index in ${!slaves[@]}
    do
        ssh -t -t ${slaves[$index]} "$(realpath -s $0) $test_name $world_size $object_size $my_address $((index+1)) $log_dir" &
    done

    # start local process (rank=0)
    $test_executable_abspath $my_address $my_address $world_size 0 $object_size 2>&1 | tee $log_dir/$my_address.server.log
else
    sleep 5

    redis_address=$4
    rank=$5
    log_dir=$6
    echo "Node($my_address) redis_address: $redis_address world_size: $world_size rank: $5 object_size: $object_size"
    $test_executable_abspath $redis_address $my_address $world_size $rank $object_size 2>&1 | tee $log_dir/$my_address.client.log
fi
