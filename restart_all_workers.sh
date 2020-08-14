#!/bin/bash

# This script is only used when necessary to reboot the ray workers.
# Workers may not be available until next task execution, so some errors could still occur.
if [ "$#" -eq 0 ]; then
    root_dir=$(dirname $(realpath -s $0))
    my_address=$($root_dir/get_ip_address.sh)

    # get cluster info
    worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
    slaves=()
    for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s $root_dir/get_ip_address.sh)); done
    all_nodes=($my_address ${slaves[@]})
    for node in ${all_nodes[@]}; do
        echo "=> $node"
        ssh $node $(realpath -s $0) restart &
    done
    wait
else
    for pid in $(ps aux | grep 'default_worker.py' | grep -v 'object_manager_port' | grep -v grep | awk '{print $2}'); do
        kill -9 $pid
    done
fi
