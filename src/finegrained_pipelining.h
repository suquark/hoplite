// This file should only be included in source files.

#pragma once

#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h> // for non-blocking socket
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <thread>

#include "common/config.h"
#include "logging.h"
#include "socket_utils.h"
