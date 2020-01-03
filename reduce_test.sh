#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: input size required"; exit; fi
if [ "$#" -gt 4 ]; then echo "ERROR: too many arguments: $#"; exit; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null
# sudo apt install valgrind

## setup
my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &> /dev/null &
# export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin

working_dir=$(dirname $(realpath -s $0))

if [ "$#" -eq 1 ]; then
	redis-server redis.conf &> /dev/null &  # port = 6380
	sleep 2

	worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
	slaves=()
	for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
	# slaves=(${slaves[@]:0:1})
	echo "[Reducing Object] master: $my_address; slaves: ${slaves[@]}"

	object_ids=()
	for oid in $(seq -f "%040g" 1 ${#slaves[@]}); do object_ids+=($oid); done
	log_dir=$working_dir/log/$(date +"%Y%m%d-%H%M%S")-reduce
	mkdir -p $log_dir

	($working_dir/reduce_test $my_address $my_address s $1 ${object_ids[@]} 2>&1 | tee $log_dir/$my_address.server.log) &
	# sleep 2

	for index in ${!slaves[@]}
	do
		ssh -t -t ${slaves[$index]} "$(realpath -s $0) $my_address $1 ${object_ids[$index]} $log_dir" &
	done
else
	# sudo fuser -km /tmp/multicast_plasma
	echo "[Putting Object] redis_address: $1 object_size: $2 objectid: $3 my_address: $my_address"
	$working_dir/reduce_test $1 $my_address c $2 $3 2>&1 | tee $4/$my_address.client.log
fi

sleep 36
