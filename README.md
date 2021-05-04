# Hoplite: Efficient Collective Communication for Task-Based Distributed Systems

## TODOs

- [ ] `StrictHostKeyChecking=no` in `~/.ssh/config`

- [ ] Reorganize automatic testing for python hoplite and C++ hoplite.

- [ ] Cleanup python code.

- [ ] Test `python/launch_ray_microbenchmarks.py`

- [ ] Refactor `src/reduce_dependency.cc` to switch from chain, binary tree, and star.

- [ ] Test refactored `src/reduce_dependency.cc`.

- [ ] Fix fault-tolerance for reduce. (figure out why sometimes it fails)

- [ ] Improve documentation coverage.


## Install dependencies

```bash
# C++ dependencies
./install_dependencies.sh
# python dependencies
pip install requirements.txt
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
pushd python
python setup.py build_ext --inplace
popd
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

## Benchmark manually

**Hoplite C++ interface** See [microbenchmarks/hoplite-cpp](microbenchmarks/hoplite-cpp)

**Hoplite Python interface** See [microbenchmarks/hoplite-python](microbenchmarks/hoplite-python)

**MPI (baseline)** See [microbenchmarks/mpi-cpp](microbenchmarks/mpi-cpp)

**Gloo (baseline)** See [microbenchmarks/gloo-cpp](microbenchmarks/gloo-cpp)

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
