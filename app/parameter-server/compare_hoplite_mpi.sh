mkdir -p ps-log-cmp/

root_dir=$(dirname $(realpath -s $0))/../../

for n_nodes in 8; do
    echo "==========" sync-$n_nodes-hoplite "=========="
    pkill notification
    $root_dir/restart_all_workers.sh
    python parameter_server.py -n $(($n_nodes - 1)) --no-test | tee ps-log-cmp/sync-$n_nodes-hoplite.log
    echo "==========" sync-$n_nodes-mpi "=========="
    worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
    master=$($root_dir/get_ip_address.sh)
    slaves=()
    for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s $root_dir/get_ip_address.sh)); done
    slaves=(${slaves[@]:0:$(($n_nodes-1))})

    all_nodes=($master ${slaves[@]})

    echo Nodes: ${all_nodes[@]} "("${#all_nodes[@]}")"

    all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

    pkill notification
    $root_dir/restart_all_workers.sh
    mpirun --map-by ppr:1:node -hosts $all_hosts python mpi_parameter_server.py --no-test | tee ps-log-cmp/sync-$n_nodes-mpi.log

done
