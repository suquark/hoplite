#!/bin/bash
master=172.31.29.249
slaves="172.31.29.128 172.31.26.179 172.31.25.36"

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
plasma-store-server -m 4000000000 -s /tmp/plasma &

for slave in $slaves
do
	ssh $slave "plasma-store-server -m 4000000000 -s /tmp/plasma" &
done

sleep 10

## multicast
./object_store $master $master s & 

sleep 10

for slave in $slaves
do
	ssh $slave "./object_store/object_store $master $slave c $obj_handle" &
done


