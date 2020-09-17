export RAY_BACKEND_LOG_LEVEL=info
mkdir -p ps-log/

ROOT_DIR=$(dirname $(realpath -s $0))/../../
source $ROOT_DIR/load_cluster_env.sh

for n_nodes in 16; do
  for model in alexnet vgg16 resnet50; do
    echo "==========" allreduce-$n_nodes-$model-mpi "=========="
    all_nodes=(${ALL_IPADDR[@]:0:$n_nodes})
    all_hosts=$(echo ${all_nodes[@]} | sed 's/ /,/g')
    echo Nodes: ${all_nodes[@]} "("${#all_nodes[@]}")"

    pkill notification
    # $ROOT_DIR/restart_all_workers.sh
    $ROOT_DIR/mpirun_pernode.sh $all_hosts python $(realpath -s mpi_all_reduce.py) -m $model \
      | tee ps-log/allreduce-$n_nodes-$model-mpi.log
    sleep 0.5

    echo "==========" allreduce-$n_nodes-$model-gloo "=========="
    i=0
    for node in ${ALL_IPADDR[@]:0:$n_nodes}; do
      echo "=> $node"
      ssh -o StrictHostKeyChecking=no $node PATH=$PATH:/home/ubuntu/anaconda3/bin:/home/ubuntu/anaconda3/condabin, \
        python $ROOT_DIR/app/parameter-server/gloo_all_reduce.py \
          --master_ip $MY_IPADDR \
          --rank $i \
          --size $n_nodes \
          -m $model \
          | tee ps-log/allreduce-$n_nodes-$model-gloo.$i.log &
      i=$((i+1))
    done
    wait
    sleep 0.5

    echo "==========" allreduce-$n_nodes-$model-hoplite "=========="
    python hoplite_all_reduce.py -n $n_nodes -m $model | tee ps-log/allreduce-$n_nodes-$model-hoplite.log
    sleep 0.5

    echo "==========" allreduce-$n_nodes-$model-ray "=========="
    python ray_parameter_server_baseline.py -n $(($n_nodes - 1)) -m $model | tee ps-log/allreduce-$n_nodes-$model-ray.log
    sleep 0.5
  done
done
