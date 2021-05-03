# Hoplite: Efficient Collective Communication for Task-Based Distributed Systems


## TODOs

- [x] Move outdated scripts into `_archived`

- [x] Rename `python/auto_ray_benchmarks.py` to `python/launch_ray_microbenchmarks.py`.

- [x] Move Ray roundtrip test into `python/auto_ray_benchmarks.py`

- [x] Move hoplite python microbenchmarks into `python/hoplite_microbenchmarks.py`

- [x] Move microbenchmarks to `microbenchmarks`.

- [x] Change build system to CMake.

- [x] Use constants for ports etc in `src/distributed_object_store.cc`.

- [ ] `StrictHostKeyChecking=no` in `~/.ssh/config`

- [ ] Implement barrier inside hoplite.

- [ ] Reorganize automatic testing for python hoplite and C++ hoplite.

- [ ] Cleanup python/cython code.

- [ ] Test `python/launch_ray_microbenchmarks.py`

- [ ] Test `python/hoplite_microbenchmarks.py`

- [ ] Refactor `src/reduce_dependency.cc` to switch from chain, binary tree, and star.

- [ ] Test refactored `src/reduce_dependency.cc`.

- [ ] Fix fault-tolerance for reduce. (figure out why sometimes it fails)

- [ ] Improve documentation coverage.

## Build Hoplite

```bash
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
```

Binaries and shared libraries are under `build` after successful compilation.

## Install dependencies

```bash
./install_dependencies.sh
```

## Microbenchmarks

`${microbenchmark_name}` includes `multicast`, `reduce`, `gather`, `allreduce`, `allgather`.

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

```
cd microbenchmarks/hoplite-cpp
`./run_tests.sh ${microbenchmark_name}_test ${total_number_of_nodes} ${input_size_in_bytes}`
```

**Python**

```
cd microbenchmarks/hoplite-python
./launch_test.py -t ray-${microbenchmark_name} -n ${total_number_of_nodes} -s ${input_size_in_bytes}
```

**Round-trip**

```
cd python
./launch_test.py -t sendrecv -n 2 -s ${input_size_in_bytes}
```

### MPI

`cd mpi && ./mpi_{microbenchmark_name}.sh ${total_number_of_nodes} ${input_size_in_bytes}`

**Round trip**

`cd mpi && ./mpi_sendrecv.sh ${input_size_in_bytes}`

### Gloo

```
cd microbenchmarks/gloo-cpp
./run_benchmark.sh ${gloo_microbenchmark_name} ${total_number_of_nodes} ${input_size_in_bytes}`
```

## Lint

`./format.sh`

### Fault tolerance

`git apply` this patch to simulate failure in microbenchmarks:

```
diff --git a/src/object_sender.cc b/src/object_sender.cc
index 5569061..f11863e 100644
--- a/src/object_sender.cc
+++ b/src/object_sender.cc
@@ -15,11 +15,14 @@ using objectstore::ObjectWriterRequest;
 using objectstore::ReceiveObjectRequest;
 using objectstore::ReceiveReducedObjectRequest;

+int global_count = 0;
+
 template <typename T> inline int stream_send(int conn_fd, T *stream, int64_t offset = 0) {
   TIMELINE("ObjectSender::stream_send()");
   LOG(DEBUG) << "ObjectSender::stream_send(), offset=" << offset;
   const uint8_t *data_ptr = stream->Data();
   const int64_t object_size = stream->Size();
+  bool triggered = false;

   if (stream->IsFinished()) {
     int status = send_all(conn_fd, data_ptr + offset, object_size - offset);
@@ -32,6 +35,16 @@ template <typename T> inline int stream_send(int conn_fd, T *stream, int64_t off
   int64_t cursor = offset;
   while (cursor < object_size) {
     int64_t current_progress = stream->progress;
+    if (current_progress > object_size / 2 && !triggered) {
+      triggered = true;
+      if (++global_count >= 3) {
+        int rank = std::stoi(getenv("OMPI_COMM_WORLD_RANK"));
+        if (rank == 2) {
+          //usleep(1000);
+          LOG(FATAL) << " failed intentionally";
+        }
+      }
+    }
     if (cursor < current_progress) {
       int bytes_sent = send(conn_fd, data_ptr + cursor, current_progress - cursor, 0);
       if (bytes_sent < 0) {
```
