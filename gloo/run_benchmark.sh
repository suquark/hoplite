#!/bin/bash

if [ "$#" -lt 3 ]; then
  echo "$(tput setaf 1)[ERROR]$(tput sgr 0) test name, number of nodes & input size required"
  echo "test name: allreduce_ring, allreduce_ring_chunked, allreduce_halving_doubling, "
  echo "           allreduce_bcube, barrier_all_to_all, broadcast_one_to_all, pairwise_exchange"
  exit -1
fi

if [ "$#" -gt 6 ]; then
  echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"
  exit -1
fi

# trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

test_name=$1
world_size=$2
object_size=$3

ROOT_DIR=$HOME/efs/object_store/
source $ROOT_DIR/load_cluster_env.sh
ALL_IPADDR=(${ALL_IPADDR[@]:0:$world_size})
PIDS=()

GLOO_DIR=$HOME/efs/gloo/

log_dir=$ROOT_DIR/gloo_log/$(date +"%Y%m%d-%H%M%S")-$test_name-$world_size-$object_size
mkdir -p $log_dir
ln -sfn $log_dir/ $ROOT_DIR/gloo_log/latest

redis-server --port 7777 --protected-mode no &> /dev/null &
REDIS_PID=$!
sleep 1

i=0
echo $MY_IPADDR
for node in ${ALL_IPADDR[@]}; do
  echo "=> $node $i"
  ssh -o StrictHostKeyChecking=no $node \
    $GLOO_DIR/build/gloo/benchmark/benchmark \
    --size $world_size \
    --rank $i \
    --redis-host $MY_IPADDR \
    --redis-port 7777 \
    --prefix benchmark-$test_name-$world_size-$object_size \
    --transport tcp \
    --elements $(($object_size / 4)) \
    --iteration-count 1 \
    $test_name \
    2>&1 | tee $log_dir/$i.log &
  PIDS+=($!)
  i=$(($i + 1))
done
wait ${PIDS[@]}
kill $REDIS_PID
