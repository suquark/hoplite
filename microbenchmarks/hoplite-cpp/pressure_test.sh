#!/bin/bash
if [ "$#" -lt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) number of nodes required"; exit -1; fi
if [ "$#" -gt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"; exit -1; fi

trap "exit" INT

./run_test.sh multicast $1 $[2**17] 1000
./run_test.sh multicast $1 $[2**30] 5

./run_test.sh reduce $1 $[2**17] 1000
./run_test.sh reduce $1 $[2**30] 5

./run_test.sh allreduce $1 $[2**17] 1000
./run_test.sh allreduce $1 $[2**30] 5

./run_test.sh gather $1 $[2**17] 1000
./run_test.sh gather $1 $[2**30] 5
