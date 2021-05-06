#!/bin/bash

# See https://github.com/facebookincubator/gloo

sudo apt-get install -y libhiredis-dev redis-server

if [ ! -d gloo ]; then
    git clone git@github.com:facebookincubator/gloo.git
fi

cd gloo
# Pin gloo version to commit ca528e32fea9ca8f2b16053cff17160290fc84ce
git checkout ca528e32fea9ca8f2b16053cff17160290fc84ce
rm -rf build
mkdir build
cd build
# Redis is required for the benchmark.
cmake ../ -DBUILD_BENCHMARK=1 -DUSE_REDIS=ON
make -j8
