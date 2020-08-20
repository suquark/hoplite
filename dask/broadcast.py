from dask.distributed import Client
import numpy as np
import time
import sys

def create_object(object_size):
    return np.empty(object_size//4, dtype=np.float32)

def get_object(o):
    return True

def main(np, object_size):
    client = Client("127.0.0.1:8786")
    sender = client.submit(create_object, object_size, workers=['Dask-0'])
    
    receivers = []
    for i in range(1, np):
        receivers.append(client.submit(get_object, sender, workers=['Dask-' + str(i)]))
    
    before = time.time()
    for receiver in receivers:
        receiver.result()
    after = time.time()

    print (after-before)


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))
