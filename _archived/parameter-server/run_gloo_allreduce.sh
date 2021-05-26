MODEL=alexnet

mkdir -p ps-log/

ROOT_DIR=$(dirname $(realpath -s $0))/../../
source $ROOT_DIR/load_cluster_env.sh

for n_nodes in 8; do
    i=0
    for node in ${ALL_IPADDR[@]:0:$n_nodes}; do
        echo "=> $node"
        ssh -o StrictHostKeyChecking=no $node PATH=$PATH:/home/ubuntu/anaconda3/bin:/home/ubuntu/anaconda3/condabin, \
            python $ROOT_DIR/app/parameter-server/gloo_all_reduce.py \
                --master_ip $MY_IPADDR \
                --rank $i \
                --size $n_nodes \
                -m $MODEL &
        i=$((i+1))
    done
    wait
done
