// Enable non-blocking for the socket that receiving objects.
#define HOPLITE_ENABLE_NONBLOCKING_SOCKET_RECV

// Enable ACK for TCP connections.
// #define HOPLITE_ENABLE_ACK

// The constant for RPC latency (in seconds)
#define HOPLITE_RPC_LATENCY (750 * 1e-6)

// The constanf for bandwidth (in bytes/second)
#define HOPLITE_BANDWIDTH (9.68 * (1 << 30) / 8);

// Use atomic type for buffer progress.
// #define HOPLITE_ENABLE_ATOMIC_BUFFER_PROGRESS

// NOTE: SO_ZEROCOPY & TCP_NODELAY is not working.
