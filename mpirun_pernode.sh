#!/bin/bash
all_hosts=$1
shift
# This syntax is for OpenMPI
mpirun --mca btl_tcp_if_include ens3 --map-by ppr:1:node -H $all_hosts $@
