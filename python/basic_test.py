import gc

import numpy as np
import py_distributed_object_store as store_lib

from py_distributed_object_store import Buffer
arr = np.random.rand(2,3,4)
buf = Buffer.from_buffer(arr)
print(buf.size(), arr.nbytes, hash(buf))
del arr
gc.collect()
print(buf.size(), hash(buf))

