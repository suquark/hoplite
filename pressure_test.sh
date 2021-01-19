#!/bin/bash
if [ "$#" -lt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) number of nodes required"; exit -1; fi
if [ "$#" -gt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"; exit -1; fi

./run_test.sh multicast_test 8 $[2**17] 1000
./run_test.sh multicast_test 8 $[2**30] 10

./run_test.sh reduce_test 8 $[2**17] 1000
./run_test.sh reduce_test 8 $[2**30] 10

./run_test.sh allreduce_test 8 $[2**17] 1000
./run_test.sh allreduce_test 8 $[2**30] 10

./run_test.sh gather_test 8 $[2**17] 1000
./run_test.sh gather_test 8 $[2**30] 10
