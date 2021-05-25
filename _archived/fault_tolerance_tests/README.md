# Fault tolerance tests (optional)

To enable fault tolerance test, first apply the patch that introduces failures:

```bash
patch -p1 --directory .. < enable_failure.patch
```

Then recompile the C++ project.

## Multicast fault tolerance test

Intensive benchmarks that exploits corner cases. This test proves reliablility of our system. It could take serveral minutes to complete with high speed networks.

Usage:


```bash
./run_test_fault_tolerance.sh subset_reduce ${total_number_of_nodes} ${input_size_in_bytes}
```

### Subset reduction fault tolerance test

This test shows Hoplite is able to reduce only a subset of objects. For example, we have 8 candidate objects to reduce, but we want to reduce 4 objects that are created first.

Usage:

```bash
./run_test_fault_tolerance.sh subset_reduce ${total_number_of_nodes} ${input_size_in_bytes}
```

We suggest `total_number_of_nodes>=4`.
