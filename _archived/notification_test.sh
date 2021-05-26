#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

sudo fuser -k 6666/tcp -s &> /dev/null
sudo fuser -k 50055/tcp -s &> /dev/null

## setup
ROOT_DIR=$(dirname $(realpath -s $0))
source $ROOT_DIR/load_cluster_env.sh

pkill '^notification$'
pkill '^notification_server_test$'
sleep 2
./notification $MY_IPADDR $MY_IPADDR &
sleep 2
./notification_server_test $MY_IPADDR $MY_IPADDR &
sleep 40
