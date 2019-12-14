#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
redis-server redis.conf&
sleep 5

master=$(ray get-head-ip ~/ray_bootstrap_config.yaml)
slaves=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)

echo master: $master
echo slaves: $slaves

obj_handle=8c97b7d89adb8bd86c9fa562704ce40ef645627a

## clean up cluster
pkill object_store
pkill plasma

for slave in $slaves
do
	ssh $slave "pkill object_store"
	ssh $slave "pkill plasma"
done

sleep 10

## setup
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma &

for slave in $slaves
do
	ssh $slave "plasma-store-server -m 4000000000 -s /tmp/multicast_plasma" &
done

sleep 10

## multicast
./object_store $master $master s $1 &

sleep 10

for slave in $slaves
do
	ssh $slave "/home/ubuntu/efs/object_store/object_store $master $slave c $obj_handle" &
done

sleep 360000

