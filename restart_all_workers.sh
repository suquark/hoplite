#!/bin/bash

# This script is only used when necessary to reboot the ray workers.
# Workers may not be available until next task execution, so some errors could still occur.
if [ "$#" -eq 0 ]; then
    ./fornode $(realpath -s $0) restart
else
    for pid in $(ps aux | grep 'default_worker.py' | grep -v 'object_manager_port' | grep -v grep | awk '{print $2}'); do
        kill -9 $pid
    done
fi
