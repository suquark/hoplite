## Hoplite Python interface benchmarks

Hoplite collective communication benchmarks with Python.

```bash
./run_tests.sh ${microbenchmark_name} ${total_number_of_nodes} ${input_size_in_bytes}
```

`${microbenchmark_name}` includes 5 most common collective communication operations: `multicast`, `reduce`, `gather`, `allreduce`, `allgather`.

### Pressure test (optional)

Intensive benchmarks that exploits corner cases. This test proves reliablility of our system. It could take serveral minutes to complete with high speed networks.

Usage: `./pressure_test.sh`

### Round-trip test (optional)

This test shows when transfer data from the object store, Hoplite is able to overlap object copy with object transfer to gain higher performance.

Usage:

```bash
./run_tests.sh roundtrip 2 ${input_size_in_bytes}
```
