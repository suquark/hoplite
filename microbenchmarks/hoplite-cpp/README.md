## Hoplite C++ interface benchmarks

Hoplite collective communication benchmarks with C++ binaries.

```bash
./run_tests.sh ${microbenchmark_name} ${total_number_of_nodes} ${input_size_in_bytes}
```

`${microbenchmark_name}` includes 5 most common collective communication operations: `multicast`, `reduce`, `gather`, `allreduce`, `allgather`.

### Pressure test (optional)

Intensive benchmarks that exploits corner cases. This test proves reliablility of our system. It could take serveral minutes to complete with high speed networks.

Usage: `./pressure_test.sh`

### Subset reduction test (optional)

This test shows Hoplite is able to reduce only a subset of objects. For example, we have 8 candidate objects to reduce, but we want to reduce 4 objects that are created first.

Usage:

```bash
./run_tests.sh subset_reduce ${total_number_of_nodes} ${input_size_in_bytes}
```

A proper setup uses `total_number_of_nodes=8`.
