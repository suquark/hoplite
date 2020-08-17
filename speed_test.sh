#!/bin/bash
ROOT_DIR=$(dirname $(realpath -s $0))
source $ROOT_DIR/load_cluster_env.sh

echo ${ALL_IPADDR[@]}, ${#ALL_IPADDR[@]}

# start iperf server
for s in ${ALL_IPADDR[@]}
do
	ssh -o StrictHostKeyChecking=no $s pkill iperf
	ssh $s iperf -s &> /dev/null &
done

for s in ${ALL_IPADDR[@]}
do
	for t in ${ALL_IPADDR[@]}
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

for s in ${ALL_IPADDR[@]}
do
    ssh $s pkill iperf &> /dev/null &
done
