rm hostfile

my_address=$(ifconfig | grep 'inet.*broadcast' | awk '{print $2}')

# get cluster info
worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
slaves=()
for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s ifconfig | grep 'inet.*broadcast' | awk '{print $2}')); done
all_nodes=($my_address ${slaves[@]})
for node in ${all_nodes[@]}; do
    echo $node "slots=1" >> hostfile
done
echo "================ hostfile ================"
cat hostfile
echo "============== hostfile end =============="

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
