#!/bin/bash

# See https://github.com/facebookincubator/gloo

sudo apt-get install -y libhiredis-dev redis-server

if [ ! -d gloo ]; then
    git clone git@github.com:facebookincubator/gloo.git
fi

cd gloo
# Pin gloo version to commit 881f7f0dcf06f7e49e134a45d3284860fb244fa9
git checkout 881f7f0dcf06f7e49e134a45d3284860fb244fa9
rm -rf build
mkdir build
cd build
# Redis is required for the benchmark.
cmake ../ -DBUILD_BENCHMARK=1 -DUSE_REDIS=ON
make -j8
