#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: number of nodes"; exit; fi
if [ "$#" -gt 2 ]; then echo "ERROR: too many arguments: $#"; exit; fi

num_tests=10

# multicast
for i in `seq 6 10`;
do
	for test_index in `seq 1 $num_tests`;
	do
		obj_size=$((8**$i))
		./multicast_test.sh $1 $obj_size
	done
done

# reduce
for i in `seq 6 10`;
do
	for test_index in `seq 1 $num_tests`;
	do
		obj_size=$((8**$i))
		./reduce_test.sh $1 $obj_size
	done
done



