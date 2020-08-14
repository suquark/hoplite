# Hoplite: Efficient Collective Communication for Task-Based Distributed Systems

## Install dependencies

```bash
./install_dependencies.sh
```

## Microbenchmarks

`${test_name}` includes `multicast`, `reduce`, `gather`, `allreduce`, `allgather`.

### Hoplite

**C++**

`./run_tests.sh ${test_name}_test ${total_number_of_nodes} ${input_size_in_bytes}`

**Python**

```
cd python
./launch_test.py -t ray-${test_name} -n ${total_number_of_nodes} -s ${input_size_in_bytes}
```

**Round-trip**

```
cd python
./launch_test.py -t sendrecv -n 2 -s ${input_size_in_bytes}
```

### MPI

`cd mpi && ./mpi_{test_name}.sh ${total_number_of_nodes} ${input_size_in_bytes}`

**Round trip**

`cd mpi && ./mpi_sendrecv.sh ${input_size_in_bytes}`

## Lint

`./format.sh`
