#!/bin/bash
if [ "$#" -lt 2 ]; then echo "ERROR: number of nodes & input size required"; exit; fi
if [ "$#" -gt 5 ]; then echo "ERROR: too many arguments: $#"; exit; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null

## setup
my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &> /dev/null &
sleep 2

working_dir=$(dirname $(realpath -s $0))

if [ "$#" -eq 2 ]; then
    pkill notification
    sleep 2
    redis-server redis.conf &> /dev/null &  # port = 6380
    ./notification $my_address &
    sleep 2
    worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
    slaves=()
    for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
    slaves=(${slaves[@]:0:$(($1-1))})
    echo "[Multicast] master: $my_address; slaves: ${slaves[@]}"

    log_dir=$working_dir/log/$(date +"%Y%m%d-%H%M%S")-multicast-$1-$2
    mkdir -p $log_dir

    ($working_dir/multicast_test $my_address $my_address $1 0 $2 2>&1 | tee $log_dir/$my_address.server.log) &
    sleep 2

    for index in ${!slaves[@]}
    do
        ssh -t -t ${slaves[$index]} "$(realpath -s $0) $my_address $1 $((index+1)) $2 $log_dir" &
    done
    sleep 30
else
    # sudo fuser -km /tmp/multicast_plasma
    echo "[Putting Object] redis_address: $1 world_size: $2 rank: $3 object_size: $4 my_address: $my_address"
    $working_dir/multicast_test $1 $my_address $2 $3 $4 2>&1 | tee $5/$my_address.client.log
fi
