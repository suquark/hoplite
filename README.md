# Hoplite: Efficient Collective Communication for Task-Based Distributed Systems

## Install dependencies

```bash
./install_dependencies.sh
```

## Microbenchmarks

`${test_name}` includes `multicast`, `reduce`, `gather`, `allreduce`, `allgather`.

### Automatic benchmarking

**Automatic benchmarking for MPI and Hoplite**:

`./auto_test.sh`

`python script/parse_result.py log/`

`python script/parse_mpi_result.py mpi_log/`

**Automatic benchmarking for Gloo**:

`cd gloo`

`./auto_test_gloo.sh ${NUM_TRIALS}`

`python parse_gloo_result.py ../gloo_log/`

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

### Gloo

`cd gloo && ./run_benchmark.sh ${gloo_test_name} ${total_number_of_nodes} ${input_size_in_bytes}`

## Lint

`./format.sh`
