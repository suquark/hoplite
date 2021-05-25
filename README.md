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

## Reinforcement Learning (Section 5.3)

See [RLLib experiments](rllib).

## ML Model Serving Experiments (Section 5.4)

See [app/ray_serve](app/ray_serve). It also includes the fault tolerance experiments related to model serving in section 5.5.


## Microbenchmarks (Section 5.1)

* Figure 6 at Section 5.1 - See ??????.

* Figure 7 at Section 5.1, Figure 13 at Appendix A - See [Hoplite Microbenchmarks](microbenchmarks).


## Lint

`./format.sh`
