#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: number of tests"; exit; fi
if [ "$#" -gt 2 ]; then echo "ERROR: too many arguments: $#"; exit; fi

for num_nodes in 2 4 6 8 10 12 14 16
do
# multicast
for i in `seq 20 30`;
do
	for test_index in `seq 1 $1`;
	do
		obj_size=$((2**$i))
		./multicast_test.sh $num_nodes $obj_size
		pushd mpi
		./mpi_broadcast.sh $num_nodes $obj_size > ../mpi_log/multicast-$num_nodes-$obj_size-$test_index
		popd
	done
done

# reduce
for i in `seq 20 30`;
do
	for test_index in `seq 1 $1`;
	do
		obj_size=$((2**$i))
		./reduce_test.sh $num_nodes $obj_size
		pushd mpi
		./mpi_reduce.sh $num_nodes $obj_size > ../mpi_log/reduce-$num_nodes-$obj_size-$test_index
		popd
	done
done

# allreduce
for i in `seq 20 30`;
do
	for test_index in `seq 1 $1`;
	do
		obj_size=$((2**$i))
		./allreduce_test.sh $num_nodes $obj_size
		pushd mpi
		./mpi_allreduce.sh $num_nodes $obj_size > ../mpi_log/allreduce-$num_nodes-$obj_size-$test_index
		popd
	done
done
done
