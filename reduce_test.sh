#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: input size required"; exit; fi
if [ "$#" -gt 3 ]; then echo "ERROR: too many arguments: $#"; exit; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

## setup
my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &> /dev/null &
# export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin
sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null
sleep 2


if [ "$#" -eq 1 ]; then
	redis-server redis.conf &> /dev/null &  # port = 6380
	redis-server redis_notification.conf &> /dev/null &  # port = 6381
	sleep 2

	worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
	slaves=()
	for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
	echo "[Putting Object] master: $my_address; slaves: ${slaves[@]}"

	object_ids=()
	for oid in $(seq -f "%040g" 1 ${#slaves[@]}); do object_ids+=($oid); done

	/home/ubuntu/efs/object_store/reduce_test $my_address $my_address s $1 ${object_ids[@]} &
	sleep 2

	for index in ${!slaves[@]}
	do
		ssh -t -t ${slaves[$index]} "$(realpath -s $0) $my_address $1 ${object_ids[$index]}" &
	done
else
	echo "[Getting Object] redis_address: $1 object_size: $2 objectid: $3 my_address: $my_address"
	/home/ubuntu/efs/object_store/reduce_test $1 $my_address c $2 $3
fi

sleep 360000
