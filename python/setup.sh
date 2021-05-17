#!/bin/bash

script_dir=$(dirname "${BASH_SOURCE[0]}")
pip install -e $script_dir
cp $script_dir/_hoplite_client.*.so $script_dir/hoplite/
cp $script_dir/../build/notification $script_dir/hoplite/
