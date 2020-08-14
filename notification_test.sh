#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null

## setup
root_dir=$(dirname $(realpath -s $0))
my_address=$($root_dir/get_ip_address.sh)

pkill '^notification$'
pkill '^notification_server_test$'
sleep 2
./notification $my_address $my_address &
sleep 2
./notification_server_test $my_address $my_address &
sleep 40
