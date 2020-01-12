# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from _client cimport CDistributedObjectStore, CBuffer, CObjectID
from cpython cimport Py_buffer

from enum import Enum

cdef class Buffer:
    cdef shared_ptr[CBuffer] buf

    def __cinit__(self, data_ptr, int64_t size):
        cdef uint8_t *_data_ptr

        _data_ptr = <uint8_t *>data_ptr
        self.buf.reset(new CBuffer(_data_ptr, size))

    @staticmethod
    cdef from_native(const shared_ptr[CBuffer] &buf):
        _self = <Buffer>Buffer.__new__(Buffer)
        _self.buf = buf
        return _self

    @classmethod
    def from_numpy(cls, obj):
        interface = obj.__array_interface__
        data_ptr, readonly = interface['data']
        nbytes = obj.nbytes
        return cls(data_ptr, nbytes)

    def data_ptr(self):
        return self.buf.get().Data()

    def size(self):
        return self.buf.get().Size()

    def __dealloc__(self):
        self.buf.reset()


cdef class ObjectID:
    cdef CObjectID data

    def __cinit__(self, const c_string& binary):
        self.data = CObjectID.FromBinary(binary)


class ReduceOp(Enum):
     MAX = 1
     MIN = 2
     SUM = 3
     PROD = 4


cdef class DistributedObjectStore:
    cdef unique_ptr[CDistributedObjectStore] store

    def __cinit__(self, bytes redis_address, int redis_port,
                  int notification_port, int notification_listening_port,
                  bytes plasma_socket,
                  bytes my_address, int object_writer_port,
                  int grpc_port):
        self.store.reset(new CDistributedObjectStore(redis_address, redis_port,
            notification_port, notification_listening_port, plasma_socket,
            my_address, object_writer_port, grpc_port))

    def get(self, ObjectID object_id, expected_size=None, reduce_op=None, reduction_id=None):
        cdef:
            shared_ptr[CBuffer] buf
            CObjectID _created_reduction_id
            c_vector[CObjectID] raw_object_ids

        if reduce_op is None:
            self.store.get().Get(object_id.data, &buf)
        elif reduce_op == ReduceOp.SUM:
            assert isinstance(expected_size, int) and expected_size > 0
            for oid in object_id:
                raw_object_ids.push_back((<ObjectID>oid).data)
            if reduction_id is None:
                self.store.get().Get(
                    raw_object_ids, <int64_t>expected_size, (<ObjectID>reduction_id).data, &buf)
            else:
                self.store.get().Get(
                    raw_object_ids, <int64_t>expected_size, &_created_reduction_id, &buf)
        else:
            raise NotImplementedError("Unsupported reduce_op")
        return Buffer.from_native(buf)

    def put(self, Buffer buf, object_id=None):
        cdef CObjectID created_object_id
        if object_id is None:
            created_object_id = self.store.get().Put(buf.buf)
            return ObjectID(created_object_id.Binary())
        else:
            self.store.get().Put(buf.buf, (<ObjectID>object_id).data)
            return object_id

    def __dealloc__(self):
        self.store.reset()
