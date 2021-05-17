#!/bin/bash

# make hoplite importable
script_dir=$(dirname "${BASH_SOURCE[0]}")
site_packages=$(python -c 'import site; print(site.getsitepackages()[0])')
echo $(realpath $script_dir) > $site_packages/easy-install.pth
echo $(realpath $script_dir) > $site_packages/hoplite.egg-link
