#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: number of tests"; exit; fi
if [ "$#" -gt 2 ]; then echo "ERROR: too many arguments: $#"; exit; fi

mkdir -p log

for test_name in ray-gather; do
for num_nodes in 4 8 12 16; do
  for i in 10 15 20 25 30; do
    for test_index in `seq 1 $1`; do
      obj_size=$((2**$i))
      filename=log/$test_name-$num_nodes-$obj_size-$test_index
      if [ ! -f $filename ]; then
         ./launch_test.py -t ${test_name} -n $num_nodes -s $obj_size 2>&1 | tee $filename
      sleep 5
      fi
    done
  done
done

done
