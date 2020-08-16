#!/bin/bash
mkdir -p ps-log-cmp/

source ../../load_cluster_env.sh

for n_nodes in 8; do

    echo "==========" sync-$n_nodes-hoplite "=========="
    pkill notification
    ../../restart_all_workers.sh
    python parameter_server.py -n $(($n_nodes - 1)) --no-test | tee ps-log-cmp/sync-$n_nodes-hoplite.log

    echo "==========" sync-$n_nodes-mpi "=========="
    
    all_nodes=(${ALL_IPADDR[@]:0:$n_nodes})
    all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

    echo Nodes: ${all_nodes[@]} "("${#all_nodes[@]}")"

    pkill notification
    ../../restart_all_workers.sh
    ../../mpirun_pernode.sh $all_hosts python $(realpath -s mpi_parameter_server.py) --no-test | tee ps-log-cmp/sync-$n_nodes-mpi.log

done
