#!/bin/bash

# See https://github.com/facebookincubator/gloo

sudo apt-get install -y libhiredis-dev redis-server

cd ..

if [ ! -d gloo ]; then
    git clone git@github.com:facebookincubator/gloo.git
fi

cd gloo
rm -rf build
mkdir build
cd build
# Redis is required for the benchmark.
cmake ../ -DBUILD_BENCHMARK=1 -DUSE_REDIS=ON
make -j8
