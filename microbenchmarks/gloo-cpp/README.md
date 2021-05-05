## Gloo collective communication benchmarks (baseline)

Usage:

```bash
./run_benchmark.sh ${gloo_microbenchmark_name} ${total_number_of_nodes} ${input_size_in_bytes}`
```

`${gloo_microbenchmark_name}` includes allreduce_ring, allreduce_ring_chunked, allreduce_halving_doubling, allreduce_bcube, barrier_all_to_all, broadcast_one_to_all, pairwise_exchange.

Note: Sometimes Gloo would be flaky and you might see error messages like

```
terminate called after throwing an instance of 'gloo::IoException'
  what():  [**/hoplite/microbenchmarks/gloo-cpp/gloo/gloo/transport/tcp/pair.cc:572] Connection closed by peer [172.31.48.113]:44461
```

when you use large payloads. We have taken that into consideration when writing our result parsing scripts, and you will get informed during parsing. You can manually rerun these tests if you want to increase the accuracy of the statistics.
