## MPI collective communication benchmarks (baseline)

Usage:

```bash
./mpi_${microbenchmark_name}.sh ${total_number_of_nodes} ${input_size_in_bytes}
```

`${microbenchmark_name}` includes 5 most common collective communication operations: `multicast`, `reduce`, `gather`, `allreduce`, `allgather`.

### Roundtrip test

Usage:

```bash
./mpi_sendrecv.sh ${input_size_in_bytes}
```
