#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null

## setup
my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
working_dir=$(dirname $(realpath -s $0))

pkill '^notification$'
pkill '^notification_server_test$'
sleep 2
./notification $my_address &
sleep 2
./notification_server_test $my_address &
sleep 40
