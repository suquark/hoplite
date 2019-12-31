#!/bin/bash
if [ -z "$1" ]; then echo "ERROR: input size required"; exit; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT
redis-server redis.conf &> /dev/null &  # port = 6380
redis-server redis_notification.conf &> /dev/null &  # port = 6381

sleep 1

worker_pubips=$(ray get_worker_ips ~/ray_bootstrap_config.yaml)

# master=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')

master=$(hostname -I | awk '{print $1}')

slaves=()
for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s hostname -I | awk '{print $1}')); done

echo master: $master
echo slaves: ${slaves[@]}

obj_handle=8c97b7d89adb8bd86c9fa562704ce40ef645627a

## setup
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin

sleep 5
## multicast
./object_store $master $master s $1 &

for slave in ${slaves[@]}
do
	ssh -t -t $slave "/home/ubuntu/efs/object_store/start_slave.sh $master $obj_handle" &
done

sleep 360000
