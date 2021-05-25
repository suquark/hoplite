#!/bin/bash

cd $HOME

sudo apt update

## build grpc
if [ ! -d grpc ]; then

     sudo apt-get install -y \
       build-essential \
	  autoconf \
	  libtool \
	  pkg-config \
	  libgflags-dev \
	  libgtest-dev \
	  clang-5.0 \
	  libc++-dev
     
     git clone https://github.com/grpc/grpc.git

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
