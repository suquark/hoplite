#!/bin/bash
if [ -z "$2" ]; then echo "ERROR: number of nodes & input size required"; exit; fi

make compare_bcast > /dev/null

worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)

root_dir=$(dirname $(realpath -s $0))/../

master=$($root_dir/get_ip_address.sh)

slaves=()
for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s $root_dir/get_ip_address.sh)); done
slaves=(${slaves[@]:0:$(($1-1))})

all_nodes=($master ${slaves[@]})

echo Number of nodes: $1 "(actually ${#all_nodes[@]})", data size: $2
echo Nodes: ${all_nodes[@]}

all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

mpirun --mca btl_tcp_if_include ens5 --map-by ppr:1:node -H $all_hosts $(realpath -s compare_bcast) $[$2/4] 1