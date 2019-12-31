#!/bin/bash

## build grpc
if [ ! -d grpc ]; then
     git clone -b $(curl -L https://grpc.io/release) https://github.com/grpc/grpc.git

     sudo apt-get install -y \
          build-essential \
	  autoconf \
	  libtool \
	  pkg-config \
	  libgflags-dev \
	  libgtest-dev \
	  clang-5.0 \
	  libc++-dev

     pushd grpc
     git submodule update --init --recursive

     make && sudo make install
     popd

     pushd grpc/third_party/protobuf
     ./autogen.sh
     ./configure
     make && sudo make install
     popd
fi

## build Arrow
if [ ! -d arrow ]; then
     git clone https://github.com/apache/arrow.git

     sudo apt-get install -y \
          build-essential \
          cmake \
          libboost-filesystem-dev \
          libboost-regex-dev \
          libboost-system-dev

     mkdir arrow/cpp/build
     pushd arrow/cpp/build

     # cmake ..
     # make -j && sudo make install

     cmake -D ARROW_PLASMA=on ..
     make && sudo make install
     popd
fi

## build hiredis (redis client)
if [ ! -d hiredis ]; then
     sudo apt-get install -y \
          libssl-dev
     git clone https://github.com/redis/hiredis.git
     pushd hiredis
     make USE_SSL=1 && sudo make install
     sudo ldconfig
     popd
fi

## build redis (redis server & cli)
REDIS_VER=redis-5.0.7
if [ ! -d $REDIS_VER ]; then
     wget http://download.redis.io/releases/$REDIS_VER.tar.gz
     tar xzf $REDIS_VER.tar.gz && rm $REDIS_VER.tar.gz
     pushd $REDIS_VER
     make && sudo make install
     popd
fi

if [ ! -d efs-utils ]; then
     sudo apt-get -y install binutils
     git clone https://github.com/aws/efs-utils.git
     pushd efs-utils
     ./build-deb.sh
     sudo apt-get -y install ./build/amazon-efs-utils*deb
     popd
     mkdir efs
fi
