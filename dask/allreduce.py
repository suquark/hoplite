from dask.distributed import Client
import numpy as np
import time
import sys

def create_object(index, object_size):
    return np.empty(object_size//4, dtype=np.float32)

def reduce_object(obj_list, object_size):
    a = np.zeros(object_size//4, dtype=np.float32)
    for obj in obj_list:
        a += obj
    return a

def get_object(index, o):
    return True

def main(np, object_size):
    client = Client("127.0.0.1:8786")
    senders = []
    for i in range(0, np):   
        senders.append(client.submit(create_object, i, object_size, workers=['Dask-' + str(i)]))
    
    receiver = client.submit(reduce_object, senders, object_size, workers=['Dask-0'])

    other_receivers = []
    for i in range(1, np):   
        other_receivers.append(client.submit(get_object, i, receiver, workers=['Dask-' + str(i)]))
 
    before = time.time()
    for r in other_receivers:
        r.result()
    after = time.time()

    print (after-before)


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))
