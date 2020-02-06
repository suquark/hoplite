slaves=()
for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
slaves=(${slaves[@]:0:$(($1-1))})

all_nodes=($master ${slaves[@]})

echo Number of nodes: $1, data size: $2
echo Nodes: ${all_nodes[@]} "("${#all_nodes[@]}")"

all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')

mpirun --map-by ppr:1:node -hosts $all_hosts "python mpi_parameter_server.py -n 15 --no-test"

# mkdir -p ps-log/

# pkill notification
# /home/ubuntu/efs/zhuohan/object_store/restart_all_workers.sh 
# python parameter_server.py -n 15 --no-test | tee ps-log/sync-16-hoplite.log

# pkill notification
# /home/ubuntu/efs/zhuohan/object_store/restart_all_workers.sh 
# python ray_parameter_server_baseline.py -n 15 --no-test | tee ps-log/sync-16-ray.log

# pkill notification
# /home/ubuntu/efs/zhuohan/object_store/restart_all_workers.sh 
# python mpi_parameter_server.py -n 15 --no-test | tee ps-log/sync-16-mpi.log
