# cython: language_level=3
# distutils: language = c++

from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from _client cimport CDistributedObjectStore, CBuffer, CObjectID

from enum import Enum

cdef class Buffer:
    cdef CBuffer *buf

    def __cinit__(self, data_ptr, int64_t size):
        cdef uint8_t *_data_ptr

        _data_ptr = <uint8_t *>data_ptr
        self.buf = new CBuffer(_data_ptr, size)

    def data_ptr(self):
        return self.buf.data()

    def size(self):
        return self.buf.size()

    def __dealloc__(self):
        del self.buf


cdef class ObjectID:
    cdef CObjectID data

    def __cinit__(self, const c_string& binary):
        self.data = CObjectID.from_binary(binary)


class ReduceOp(Enum):
     MAX = 1
     MIN = 2
     SUM = 3
     PROD = 4


cdef class DistributedObjectStore:
    cdef DistributedObjectStore store

    def __cinit__(self, const c_string &redis_address, int redis_port,
                  int notification_port, int notification_listening_port,
                  const c_string &plasma_socket,
                  const c_string &my_address, int object_writer_port,
                  int grpc_port):
        self.store = DistributedObjectStore(redis_address, redis_port,
            notification_port, notification_listening_port, plasma_socket,
            my_address, object_writer_port, grpc_port)

    def get(self, object_id, reduce_op=None):
        pass

    def put(self, Buffer buf, object_id=None):
        pass
