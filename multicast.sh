#!/bin/bash
if [ -z "$1" ]; then echo "ERROR: input size required"; exit; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT
redis-server redis.conf &
sleep 1

worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)

master=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')

slaves=()
for s in $worker_pubips; do slaves+=($(ssh $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done

echo master: $master
echo slaves: ${slaves[@]}

obj_handle=8c97b7d89adb8bd86c9fa562704ce40ef645627a

## setup
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin

sleep 5
## multicast
protoc --version
./object_store $master $master s $1 &

sleep 10

for slave in ${slaves[@]}
do
	ssh -tt $slave "/home/ubuntu/efs/object_store/start_slave.sh $master $obj_handle" &
done

sleep 360000
