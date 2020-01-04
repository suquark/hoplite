# object_store

## Install dependencies

```bash
./install_dependencies.sh
```

## Multicast

### MPI (baseline)

`cd mpi && ./mpi_broadcast.sh $node_number $data_size`

### Our library

`./multicast_test.sh $data_size`

## Reduce

### MPI (baseline)

`cd mpi && ./mpi_reduce.sh $node_number $data_size`

### Our library

`./reduce_test.sh $data_size`

## Lint

`./format.sh`
