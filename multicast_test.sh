#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: number of nodes & input size required"; exit; fi
if [ "$#" -gt 4 ]; then echo "ERROR: too many arguments: $#"; exit; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null
## setup
my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &> /dev/null &
# export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin
sleep 2

working_dir=$(dirname $(realpath -s $0))

if [ "$#" -eq 2 ]; then
	redis-server redis.conf &> /dev/null &  # port = 6380
	sleep 2
	worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
	slaves=()
	for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
	slaves=(${slaves[@]:0:$(($1-1))})
	echo "[Putting Object] master: $my_address; slaves: ${slaves[@]}"
	log_dir=$working_dir/log/$(date +"%Y%m%d-%H%M%S")-multicast
	mkdir -p $log_dir

	## multicast
	($working_dir/multicast_test $my_address $my_address s $2 2>&1 | tee $log_dir/$my_address.server.log) &
	sleep 2

	for slave in ${slaves[@]}
	do
		ssh -t -t $slave "$(realpath -s $0) $1 $my_address 8c97b7d89adb8bd86c9fa562704ce40ef645627a $log_dir" &
	done
else
	# sudo fuser -km /tmp/multicast_plasma
	echo "[Getting Object] redis_address: $2 object_id: $3 my_address: $my_address"
	## multicast
	($working_dir/multicast_test $2 $my_address c $3 2>&1 | tee $4/$my_address.client.log) &
fi

sleep 36
