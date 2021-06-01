export RAY_BACKEND_LOG_LEVEL=info
mkdir -p ps-log/

for n_nodes in 8 16; do
  for model in alexnet vgg16 resnet50; do
    echo "==========" async-ps-$n_nodes-$model-hoplite "=========="
    python parameter_server.py -n $(($n_nodes - 1)) -a $((($n_nodes - 1) / 2)) -m $model | tee ps-log/async-ps-$n_nodes-$model-hoplite.log
    sleep 0.5

    echo "==========" async-ps-$n_nodes-$model-ray "=========="
    python ray_parameter_server_baseline.py -n $(($n_nodes - 1)) -a $((($n_nodes - 1) / 2)) -m $model | tee ps-log/async-ps-$n_nodes-$model-ray.log
    sleep 0.5
  done
done
