# This file should only be sourced.

MY_IPADDR=$(hostname -i)
# OTHERS_IPADDR=()
# for s in $(ray get-worker-ips ~/ray_bootstrap_config.yaml); do
#     OTHERS_IPADDR+=($(ssh -o StrictHostKeyChecking=no $s hostname -i));
# done
SCRIPT_CURRENT_DIR=$(dirname $(realpath -s ${BASH_SOURCE[0]}))

OTHERS_IPADDR=($(python $SCRIPT_CURRENT_DIR/get_worker_ips.py 2>/dev/null))
ALL_IPADDR=($MY_IPADDR ${OTHERS_IPADDR[@]})
unset SCRIPT_CURRENT_DIR
