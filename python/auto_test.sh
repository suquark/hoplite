#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: number of tests"; exit; fi
if [ "$#" -gt 2 ]; then echo "ERROR: too many arguments: $#"; exit; fi

mkdir -p log

for num_nodes in 8
do

for test_name in multicast reduce allreduce gather allgather; do
  for i in `seq 20 30`; do
    for test_index in `seq 1 $1`; do
      obj_size=$((2**$i))
      filename=log/$test_name-$num_nodes-$obj_size.log
      if [ ! -f $filename ]; then
         ./launch_test.py -t ray-${test_name} -n $num_nodes -s $obj_size 2>&1 | tee $filename
      fi
    done
  done
done

done
