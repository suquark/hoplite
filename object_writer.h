#ifndef OBJECT_WRITER_H
#define OBJECT_WRITER_H

#include <iostream>
#include <plasma/client.h>
#include "global_control_store.h"

extern void *pending_write;
extern long pending_size;
extern std::atomic_long progress;

void RunTCPServer(GlobalControlStoreClient& gcs_client, plasma::PlasmaClient& plasma_client, const std::string& ip, int port);

#endif // OBJECT_WRITER_H
