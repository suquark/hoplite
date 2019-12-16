#!/bin/bash

worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)

master=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')

slaves=()
for s in $worker_pubips; do slaves+=($(ssh $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done

all_nodes=(${slaves[@]})
all_nodes+=($master)
echo ${all_nodes[@]}, ${#all_nodes[@]}

# start iperf server
for s in ${all_nodes[@]}
do
	ssh $s pkill iperf
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
