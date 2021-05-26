#!/bin/bash
all_hosts=$1
shift
# This syntax is for OpenMPI
/opt/amazon/openmpi/bin/mpirun --mca btl_tcp_if_exclude lo,docker0 --map-by ppr:1:node -H $all_hosts $@
