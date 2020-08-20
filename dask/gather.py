from dask.distributed import Client
import numpy as np
import time
import sys
import os

def create_object(index, object_size):
    a = np.empty(object_size//4, dtype=np.float32)
    return a

def gather_object(o):
    return True

def main(pp, object_size):
    client = Client("127.0.0.1:8786")
    senders = []
    b = time.time()
    for i in range(0, pp):   
        senders.append(client.submit(create_object, i, object_size, workers=['Dask-' + str(i)]))
   
    receiver = client.submit(gather_object, senders, workers=['Dask-0'])

    before = time.time()
    print(receiver.result())
    after = time.time()

    print (after-before)


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))
