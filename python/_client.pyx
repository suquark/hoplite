# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set
from libcpp.vector cimport vector as c_vector

from _client cimport CDistributedObjectStore, CBuffer, CObjectID, CRayLog, CRayLogDEBUG, CRayLogINFO, CRayLogERROR
from cpython cimport Py_buffer, PyObject
from cpython.buffer cimport PyBUF_SIMPLE, PyObject_CheckBuffer, PyBuffer_Release, PyObject_GetBuffer, PyBuffer_FillInfo

from enum import Enum
import socket

from cython.operator cimport dereference, preincrement


cdef class Buffer:
    cdef:
        shared_ptr[CBuffer] buf
        Py_buffer py_buf
        c_vector[Py_ssize_t] shape
        c_vector[Py_ssize_t] strides

    def __cinit__(self, *args, **kwargs):
        # Note: we should check self.py_buf.obj for uninitialized buffer,
        # but unfortuantely I haven't figured out how to do that because
        # Py_buffer is specially treated in Cython, and we cannot get rid of
        # reference count when cleaning up 'py_buf.obj'. Here we just use
        # 'None' as a workaround.
        PyBuffer_FillInfo(&self.py_buf, None, NULL, 0, 0, PyBUF_SIMPLE)
        self.shape.push_back(0)
        self.strides.push_back(1)

    def __init__(self):
        raise ValueError("This object cannot be created from __init__")

    cdef update_buffer_from_pointer(self, uint8_t *data, int64_t size):
        cdef CBuffer* new_buf
        new_buf = new CBuffer(data, size)
        self.buf.reset(new_buf)
        self.shape[0] = size

    @staticmethod
    cdef from_native(const shared_ptr[CBuffer] &buf):
        _self = <Buffer>Buffer.__new__(Buffer)
        _self.buf = buf
        _self.shape[0] = buf.get().Size()
        return _self

    @classmethod
    def from_buffer(cls, obj):
        cdef:
            Buffer new_buf
            Py_buffer* py_buf
        if not PyObject_CheckBuffer(obj):
            raise ValueError("Python object hasn't implemented the buffer interface")
        new_buf = Buffer.__new__(Buffer)
        py_buf = &new_buf.py_buf
        status = PyObject_GetBuffer(obj, py_buf, PyBUF_SIMPLE)
        if status < 0:
            raise ValueError("Failed to convert python object into buffer")
        new_buf.update_buffer_from_pointer(<uint8_t *>py_buf.buf, py_buf.len)
        return new_buf

    def __getbuffer__(self, Py_buffer* buffer, int flags):
        # get a bytes buffer
        buffer.readonly = 0
        buffer.buf = self.buf.get().MutableData()
        buffer.format = 'b'
        buffer.internal = NULL
        buffer.itemsize = 1
        buffer.len = self.buf.get().Size()
        buffer.ndim = 1
        buffer.obj = self
        buffer.shape = self.shape.data()
        buffer.strides = self.strides.data()
        buffer.suboffsets = NULL

    def data_ptr(self):
        return <int64_t>self.buf.get().Data()

    def size(self):
        return self.buf.get().Size()

    def __hash__(self):
        return self.buf.get().Hash()

    def __dealloc__(self):
        if self.py_buf.obj is not None:
            # This is a workaround. Even if it is now an empty pointer,
            # it should still work because the python implementation will
            # just ignore that. Also it cannot be a null pointer because
            # we should be the only owner of this buffer.
            PyBuffer_Release(&self.py_buf)
        self.buf.reset()


cdef class ObjectID:
    cdef CObjectID data

    def __cinit__(self, const c_string& binary):
        self.data = CObjectID.FromBinary(binary)

    def __reduce__(self):
        return type(self), (self.data.Binary(),)

    def __hash__(self):
        return hash(self.data.Binary())

    def __eq__(self, other):
        return self.data.Binary() == (<ObjectID>other).data.Binary()

    def __str__(self):
        return "ObjectID(" + self.data.Binary().hex() + ")"

    def __repr__(self):
        return self.__str__()


class ReduceOp(Enum):
     MAX = 1
     MIN = 2
     SUM = 3
     PROD = 4


cdef class DistributedObjectStore:
    cdef unique_ptr[CDistributedObjectStore] store

    def __cinit__(self, str object_directory_address):
        my_address = socket.gethostbyname(socket.gethostname()).encode()
        CRayLog.StartRayLog(my_address, CRayLogDEBUG)
        self.store.reset(new CDistributedObjectStore(object_directory_address.encode()))

    def get(self, ObjectID object_id):
        cdef:
            shared_ptr[CBuffer] buf
        self.store.get().Get(object_id.data, &buf)
        return Buffer.from_native(buf)

    def reduce_async(self, object_ids, reduce_op, reduction_id=None, num_reduce_objects=-1):
        cdef:
            ObjectID _created_reduction_id = ObjectID(b'\0' * 20)
            c_vector[CObjectID] raw_object_ids

        if reduce_op == ReduceOp.SUM:
            for oid in object_ids:
                raw_object_ids.push_back((<ObjectID>oid).data)
            if reduction_id is not None:
                if num_reduce_objects > 0:
                    self.store.get().Reduce(
                        raw_object_ids, (<ObjectID>reduction_id).data, <ssize_t>num_reduce_objects)
                else:
                    self.store.get().Reduce(
                        raw_object_ids, (<ObjectID>reduction_id).data)
                return reduction_id
            else:
                if num_reduce_objects > 0:
                    self.store.get().Reduce(
                        raw_object_ids, &_created_reduction_id.data, <ssize_t>num_reduce_objects)
                else:
                    self.store.get().Reduce(
                        raw_object_ids, &_created_reduction_id.data)
                return _created_reduction_id
        else:
            raise NotImplementedError("Unsupported reduce_op")

    def get_reduced_objects(self, ObjectID reduction_id):
        cdef:
            unordered_set[CObjectID] object_ids_
            CObjectID oid
        object_ids_ = self.store.get().GetReducedObjects(reduction_id.data)
        cdef unordered_set[CObjectID].iterator it = object_ids_.begin()
        object_ids = set()
        while it != object_ids_.end():
            oid = dereference(it)
            object_ids.add(ObjectID(oid.Binary()))
            preincrement(it)
        return object_ids

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
