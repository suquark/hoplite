#!/bin/bash

worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)

root_dir=$(dirname $(realpath -s $0))
my_address=$($root_dir/get_ip_address.sh)

slaves=()
for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s $root_dir/get_ip_address.sh)); done

all_nodes=(${slaves[@]})
all_nodes+=($master)
echo ${all_nodes[@]}, ${#all_nodes[@]}

# start iperf server
for s in ${all_nodes[@]}
do
	ssh -o StrictHostKeyChecking=no $s pkill iperf
	ssh $s iperf -s &> /dev/null &
done

for s in ${all_nodes[@]}
do
	for t in ${all_nodes[@]}
	do
		if [ "$s" == "$t" ]
		then continue
		fi
		echo $s, $t
		ssh $s iperf -c $t -t 5 | grep GBytes
	done
	break
done

# shutdown iperf server
for s in ${all_nodes[@]}
do
        ssh $s pkill iperf &> /dev/null &
done
