#!/bin/bash

## build protobuf
if [ ! -d protoc]; then
    git clone https://github.com/protocolbuffers/protobuf.git

    sudo apt-get install \
         autoconf \
	 automake \
	 libtool \
	 curl \
	 make \
	 g++ \
	 unzip

    pushd protobuf
    git submodule update --init --recursive
    ./autogen.sh
    ./configure
    make -j 32 && sudo make install
    sudo ldconfig
    popd
fi

## build grpc
if [ ! -d grpc]; then
     git clone https://github.com/grpc/grpc.git
     pushd grpc
     git submodule update --init

     make -j 32 && sudo make install
     popd
fi

## build Arrow
if [ ! -d arrow ]; then
     git clone https://github.com/apache/arrow.git

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
     sudo apt-get install \
          libssl-dev
     git clone https://github.com/redis/hiredis.git
     pushd hiredis
     make USE_SSL=1 && sudo make install
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

