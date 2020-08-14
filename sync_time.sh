#!/bin/bash

sudo apt install -y chrony
sudo sed -i 's/^# information about usuable directives./server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4\n/g' /etc/chrony/chrony.conf
sudo /etc/init.d/chrony restart

root_dir=$(dirname $(realpath -s $0))

if [ "$#" -eq 0 ]; then
    worker_pubips=$(ray get-worker-ips ~/ray_bootstrap_config.yaml)
    slaves=()
    for s in $worker_pubips; do slaves+=($(ssh -o StrictHostKeyChecking=no $s $root_dir/get_ip_address.sh)); done

    for index in ${!slaves[@]}
    do
        ssh -t -t ${slaves[$index]} "$(realpath -s $0) 0"
    done
fi
