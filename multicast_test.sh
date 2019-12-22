#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: input size required"; exit; fi
if [ "$#" -gt 2 ]; then echo "ERROR: too many arguments: $#"; exit; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

## setup
my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin
sudo fuser -k 6666/tcp
sudo fuser -k 50055/tcp
sleep 4

if [ "$#" -eq 1 ]; then
	redis-server redis.conf &> /dev/null &  # port = 6380
	redis-server redis_notification.conf &> /dev/null &  # port = 6381
	sleep 2
	worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
	slaves=()
	for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
	echo "master: $my_address; slaves: ${slaves[@]}"

	## multicast
	./multicast_test $my_address $my_address s $1 &
	sleep 2

	for slave in ${slaves[@]}
	do
		ssh -t -t $slave "$(realpath -s $0) $my_address 8c97b7d89adb8bd86c9fa562704ce40ef645627a" &
	done
else
	echo "master: $1 object_id: $2"
	## multicast
	/home/ubuntu/efs/object_store/object_store $1 $my_address c $2
fi

sleep 360000
