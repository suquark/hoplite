#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: number of nodes & number of tests"; exit; fi
if [ "$#" -gt 3 ]; then echo "ERROR: too many arguments: $#"; exit; fi

# multicast
for i in `seq 6 10`;
do
	for test_index in `seq 1 $2`;
	do
		obj_size=$((8**$i))
		./multicast_test.sh $1 $obj_size
	done
done

# reduce
for i in `seq 6 10`;
do
	for test_index in `seq 1 $2`;
	do
		obj_size=$((8**$i))
		./reduce_test.sh $1 $obj_size
	done
done
