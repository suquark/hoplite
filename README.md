# object_store

```bash
sudo apt-get install \
     build-essential \
     cmake \
     libboost-filesystem-dev \
     libboost-regex-dev \
     libboost-system-dev

mkdir (Arrow)/cpp/build
cd (Arrow)/cpp/build

cmake ..
make -j 32
sudo make install

cmake -D ARROW_PLASMA=on -D ARROW_BUILD_TESTS=on ..
make -j 32
sudo make install
```


