#!/bin/bash

cd $HOME

sudo apt update

## build grpc
if [ ! -d grpc ]; then
     
     git clone -b $(curl -L https://grpc.io/release) https://github.com/grpc/grpc.git

     sudo apt-get install \
       build-essential \
	  autoconf \
	  libtool \
	  pkg-config \
	  libgflags-dev \
	  libgtest-dev \
	  clang-5.0 \
	  libc++-dev

     pushd grpc
     # pin gRPC version to 1.31.0
     git checkout tags/v1.31.0
     git submodule update --init --recursive

     mkdir build && cd build
     cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local
     make -j8 && sudo make install
     popd

     pushd grpc/third_party/protobuf
     ./autogen.sh
     ./configure
     make -j8 && sudo make install
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
