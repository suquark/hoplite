# Hoplite: Efficient Collective Communication for Task-Based Distributed Systems

## TODOs

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


## Microbenchmarks (Figure 7 at Section 5.1, Figure 13 at Appendix A)

First, run the microbenchmarks and collect data _(About 30 min)_

### OpenMPI _(about 30 min)_

```bash
pushd microbenchmarks/mpi-cpp
./auto_test.sh
python parse_result.py --verbose
popd
```

### Hoplite _(about 15 min)_

```bash
pushd microbenchmarks/hoplite-cpp
./auto_test.sh
python parse_result.py --verbose
popd
```

### Gloo _(about 20 min)_

```bash
pushd microbenchmarks/gloo-cpp
./install_gloo.sh
./auto_test.sh
python parse_result.py --verbose
popd
```

### Ray _(about ? min)_

TODO: fix the problem that Ray stuck in the middle.

### Dask _(about ? min)_

TODO: run dask.

## Lint

`./format.sh`
