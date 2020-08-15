# This file should only be sourced.

MY_IPADDR=$(hostname -i)
OTHERS_IPADDR=()
for s in $(ray get-worker-ips ~/ray_bootstrap_config.yaml); do
    OTHERS_IPADDR+=($(ssh -o StrictHostKeyChecking=no $s hostname -i));
done
ALL_IPADDR=($MY_IPADDR ${OTHERS_IPADDR[@]})
