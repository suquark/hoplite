# cython: language_level = 3

from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector


cdef extern from "<plasma/common.h>" namespace "plasma" nogil:
    cdef cppclass CObjectID "plasma::ObjectID":
        @staticmethod
        CObjectID from_binary(const c_string& binary)
        c_string binary() const


cdef extern from "<arrow/buffer.h>" namespace "arrow" nogil:
    cdef cppclass CBuffer "arrow::Buffer":
        CBuffer(uint8_t* data, int64_t size)
        const uint8_t* data()
        int64_t size()


cdef extern from "../src/distributed_object_store.h" namespace "" nogil:
    cdef cppclass CDistributedObjectStore "DistributedObjectStore":
        CDistributedObjectStore(const c_string &redis_address, int redis_port,
                                int notification_port, int notification_listening_port,
                                const c_string &plasma_socket,
                                const c_string &my_address, int object_writer_port,
                                int grpc_port)

        void Put(const void *data, size_t size, const CObjectID &object_id)

        CObjectID Put(const void *data, size_t size)

        void Get(const c_vector[CObjectID] &object_ids,
                size_t _expected_size, CObjectID *created_reduction_id,
                shared_ptr[CBuffer] *result)

        void Get(const c_vector[CObjectID] &object_ids,
                size_t _expected_size, const CObjectID &reduction_id,
                shared_ptr[CBuffer] *result)

        void Get(const CObjectID &object_id,
                shared_ptr[CBuffer] *result)
