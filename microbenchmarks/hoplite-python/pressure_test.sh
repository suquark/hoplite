#!/bin/bash
if [ "$#" -lt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) number of nodes required"; exit -1; fi
if [ "$#" -gt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"; exit -1; fi

trap "exit" INT

for i in `seq 1000`; do
./run_test.sh multicast 8 $[2**17]
./run_test.sh reduce 8 $[2**17]
./run_test.sh allreduce 8 $[2**17]
./run_test.sh gather 8 $[2**17] 
done

for i in `seq 5`; do
./run_test.sh multicast 8 $[2**17]
./run_test.sh reduce 8 $[2**17]
./run_test.sh allreduce 8 $[2**17]
./run_test.sh gather 8 $[2**17] 
done
