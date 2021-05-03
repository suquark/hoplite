# cython: language_level = 3

from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t, uint32_t
from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set

from libcpp.vector cimport vector as c_vector

cdef extern from "util/logging.h" namespace "ray" nogil:
    cdef cppclass CRayLogLevel "hoplite::RayLogLevel":
        pass

    cdef cppclass CRayLog  "hoplite::RayLog":
        @staticmethod
        void StartRayLog(const c_string &my_address, CRayLogLevel log_level)


cdef extern from "util/logging.h" namespace "hoplite::RayLogLevel" nogil:
    cdef CRayLogLevel CRayLogDEBUG "hoplite::RayLogLevel::DEBUG"
    cdef CRayLogLevel CRayLogINFO "hoplite::RayLogLevel::INFO"
    cdef CRayLogLevel CRayLogWARNING "hoplite::RayLogLevel::WARNING"
    cdef CRayLogLevel CRayLogERROR "hoplite::RayLogLevel::ERROR"
    cdef CRayLogLevel CRayLogFATAL "hoplite::RayLogLevel::FATAL"


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


cdef extern from "client/distributed_object_store.h" namespace "" nogil:
    cdef cppclass CDistributedObjectStore "DistributedObjectStore":
        CDistributedObjectStore(const c_string &object_directory_address)

        void Put(const shared_ptr[CBuffer] &buffer, const CObjectID &object_id)

        CObjectID Put(const shared_ptr[CBuffer] &buffer)

        void Reduce(const c_vector[CObjectID] &object_ids,
                    CObjectID *created_reduction_id)

        void Reduce(const c_vector[CObjectID] &object_ids,
                    const CObjectID &reduction_id)

        void Reduce(const c_vector[CObjectID] &object_ids, 
                    CObjectID *created_reduction_id, 
                    ssize_t num_reduce_objects)

        void Reduce(const c_vector[CObjectID] &object_ids,
                    const CObjectID &reduction_id, 
                    ssize_t num_reduce_objects)

        unordered_set[CObjectID] GetReducedObjects(const CObjectID &reduction_id)

        void Get(const CObjectID &object_id,
                 shared_ptr[CBuffer] *result)
