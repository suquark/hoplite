#ifndef _HOPLITE_COMMON_CONFIG_H_
#define _HOPLITE_COMMON_CONFIG_H_

// Enable non-blocking for the socket that receiving objects.
#define HOPLITE_ENABLE_NONBLOCKING_SOCKET_RECV

// Enable ACK for sending/receiving buffers. Usually used for debugging.
// #define HOPLITE_ENABLE_ACK

// The constant for RPC latency (in seconds)
#define HOPLITE_RPC_LATENCY (750 * 1e-6)

// The constanf for bandwidth (in bytes/second)
#define HOPLITE_BANDWIDTH (9.68 * (1 << 30) / 8);

// Use atomic type for buffer progress.
// #define HOPLITE_ENABLE_ATOMIC_BUFFER_PROGRESS

// Maximum inflow concurrency for a node
#define HOPLITE_MAX_INFLOW_CONCURRENCY 2

// The thread pool size for the distributed store to launch
// RPCs like `InvokeReduceTo` and `InvokeRedirectReduce`.
#define HOPLITE_THREADPOOL_SIZE_FOR_RPC 10

// NOTE: SO_ZEROCOPY & TCP_NODELAY is not working.

#endif  // _HOPLITE_COMMON_CONFIG_H_
