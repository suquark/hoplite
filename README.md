# Hoplite: Efficient Collective Communication for Task-Based Distributed Systems

## TODOs

- [ ] Cleanup python code.

- [ ] Refactor `src/reduce_dependency.cc` to switch from chain, binary tree, and star.

- [ ] Fix fault-tolerance for reduce. (figure out why sometimes it fails)

- [ ] Improve documentation coverage.


## Install dependencies

```bash
# C++ dependencies
./install_dependencies.sh
# python dependencies
pip install -r requirements.txt
```

## Build Hoplite

First, build Hoplite C++ binaries 

```bash
# requires CMake >= 3.13, gcc/clang with C++14 support
mkdir build
pushd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
popd
```

Binaries and shared libraries are under `build` after successful compilation.

Then build Hoplite Python library

```bash
pip install -e python
cp build/notification python/hoplite/
./fornode $(realpath python/setup.sh) 
```

To validate you have installed the python library correctly,

```bash
./fornode python -c \"import hoplite\"
```

## ML Model Serving Experiments (Section 5.4)

See [app/ray_serve](app/ray_serve). It also includes the fault tolerance experiments related to model serving in section 5.5.


## Microbenchmarks

`${microbenchmark_name}` includes `multicast`, `reduce`, `gather`, `allreduce`, `allgather`.

### Automatic benchmarking

**Automatic benchmarking for MPI and Hoplite**:

`./auto_test.sh`

`python script/parse_result.py log/`

`python script/parse_mpi_result.py mpi_log/`

**Automatic benchmarking for Gloo**:

`cd gloo`

`./auto_test_gloo.sh ${NUM_TRIALS}`

`python parse_gloo_result.py ../gloo_log/`

## Benchmark manually

**Hoplite C++ interface** See [microbenchmarks/hoplite-cpp](microbenchmarks/hoplite-cpp)

**Hoplite Python interface** See [microbenchmarks/hoplite-python](microbenchmarks/hoplite-python)

**MPI (baseline)** See [microbenchmarks/mpi-cpp](microbenchmarks/mpi-cpp)

**Gloo (baseline)** See [microbenchmarks/gloo-cpp](microbenchmarks/gloo-cpp)

## Lint

`./format.sh`
