#!/bin/bash
if [ "$#" -lt 2 ]; then echo "ERROR: number of nodes & input size required"; exit; fi
if [ "$#" -gt 5 ]; then echo "ERROR: too many arguments: $#"; exit; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null
# sudo apt install valgrind

## setup
my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &> /dev/null &
# export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin

working_dir=$(dirname $(realpath -s $0))

if [ "$#" -eq 2 ]; then
	redis-server redis.conf &> /dev/null &  # port = 6380
	sleep 2

	worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
	slaves=()
	for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
    slaves=(${slaves[@]:0:$(($1-1))})
	echo "[Reducing Object] master: $my_address; slaves: ${slaves[@]}"

	log_dir=$working_dir/log/$(date +"%Y%m%d-%H%M%S")-allreduce-$1-$2
	mkdir -p $log_dir

	($working_dir/reduce_test $my_address $my_address $1 0 $2 2>&1 | tee $log_dir/$my_address.server.log) &
	# sleep 2

	for index in ${!slaves[@]}
	do
		ssh -t -t ${slaves[$index]} "$(realpath -s $0) $1 $index $my_address $2 $log_dir" &
	done
else
	# sudo fuser -km /tmp/multicast_plasma
	echo "[Putting Object] world_size: $1 rank: $2 redis_address: $3 object_size: $4 my_address: $my_address"
	$working_dir/reduce_test $3 $my_address $1 $2 $4 2>&1 | tee $5/$my_address.client.log
fi

sleep 30
