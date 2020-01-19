# cython: language_level = 3

from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t, uint32_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

cdef extern from "util/logging.h" namespace "ray" nogil:
    cdef cppclass CRayLogLevel "ray::RayLogLevel":
        pass

    cdef cppclass CRayLog  "ray::RayLog":
        @staticmethod
        void StartRayLog(const c_string &my_address, CRayLogLevel log_level)


cdef extern from "util/logging.h" namespace "ray::RayLogLevel" nogil:
    cdef CRayLogLevel CRayLogDEBUG "ray::RayLogLevel::DEBUG"


cdef extern from "common/id.h" namespace "" nogil:
    cdef cppclass CObjectID "ObjectID":
        @staticmethod
        CObjectID FromBinary(const c_string& binary)
        @staticmethod
        CObjectID FromHex(const c_string& binary)
        c_string Binary() const


cdef extern from "common/buffer.h" namespace "" nogil:
    cdef cppclass CBuffer "Buffer":
        CBuffer(int64_t size)
        CBuffer(uint8_t* data, int64_t size)
        const uint8_t* Data()
        uint8_t* MutableData()
        int64_t Size()
        uint32_t CRC32() const


cdef extern from "../src/distributed_object_store.h" namespace "" nogil:
    cdef cppclass CDistributedObjectStore "DistributedObjectStore":
        CDistributedObjectStore(const c_string &redis_address, int redis_port,
                                int notification_port, int notification_listening_port,
                                const c_string &plasma_socket,
                                const c_string &my_address, int object_writer_port,
                                int grpc_port)

        void Put(const shared_ptr[CBuffer] &buffer, const CObjectID &object_id)

        CObjectID Put(const shared_ptr[CBuffer] &buffer)

        void Reduce(const c_vector[CObjectID] &object_ids,
                    size_t _expected_size, CObjectID *created_reduction_id)

        void Reduce(const c_vector[CObjectID] &object_ids,
                    size_t _expected_size, const CObjectID &reduction_id)

        void Get(const CObjectID &object_id,
                 shared_ptr[CBuffer] *result)
