#!/bin/bash
sudo apt-get install -y libhiredis-dev redis-server

cd ..

if [ ! -d gloo ]; then
    git clone git@github.com:facebookincubator/gloo.git
fi

cd gloo
rm -rf build
mkdir build
cd build
cmake ../ -DBUILD_BENCHMARK=1
make -j8
