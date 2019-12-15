#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT
current_ip=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')

echo master: $1
echo object_id: $2

pkill object_store

## setup
plasma-store-server -m 4000000000 -s /tmp/multicast_plasma & &> /dev/null

sleep 5
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin

## multicast
/home/ubuntu/efs/object_store/object_store $1 $current_ip c $2
