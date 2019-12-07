#!/bin/bash

## build Arrow
if [ ! -d arrow ]; then
     git clone git@github.com:apache/arrow.git

     sudo apt-get install \
          build-essential \
          cmake \
          libboost-filesystem-dev \
          libboost-regex-dev \
          libboost-system-dev

     mkdir arrow/cpp/build
     pushd arrow/cpp/build

     cmake ..
     make -j 32 && sudo make install

     cmake -D ARROW_PLASMA=on -D ARROW_BUILD_TESTS=on ..
     make -j 32 && sudo make install
     popd
fi

## build hiredis (redis client)
if [ ! -d hiredis ]; then
     git clone git@github.com:redis/hiredis.git
     pushd hiredis
     make USE_SSL=1
     popd
fi

## build redis (redis server & cli)
REDIS_VER=redis-5.0.7
if [ ! -d $REDIS_VER ]; then
     wget http://download.redis.io/releases/$REDIS_VER.tar.gz
     tar xzf $REDIS_VER.tar.gz && rm $REDIS_VER.tar.gz
     pushd $REDIS_VER
     make -j && sudo make install
     popd
fi

