mkdir -p ps-log-cmp/

for n_nodes in 8; do
    echo "==========" sync-$n_nodes-hoplite "=========="
    pkill notification
    /home/ubuntu/efs/zhuohan/object_store/restart_all_workers.sh 
    python parameter_server.py -n $(($n_nodes - 1)) --no-test | tee ps-log-cmp/sync-$n_nodes-hoplite.log
    echo "==========" sync-$n_nodes-mpi "=========="
    worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
    master=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')
    slaves=()
    for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
    slaves=(${slaves[@]:0:$(($n_nodes-1))})

    all_nodes=($master ${slaves[@]})

    echo Nodes: ${all_nodes[@]} "("${#all_nodes[@]}")"

    all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')
    
    pkill notification
    /home/ubuntu/efs/zhuohan/object_store/restart_all_workers.sh 
    mpirun --map-by ppr:1:node -hosts $all_hosts python mpi_parameter_server.py --no-test | tee ps-log-cmp/sync-$n_nodes-mpi.log
    
done
