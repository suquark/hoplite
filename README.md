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
