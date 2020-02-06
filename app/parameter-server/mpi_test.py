import numpy as np
from mpi4py import MPI
import time

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    data = np.ones(10, dtype=np.float32)
    data_sum = np.empty(10, dtype=np.float32)
else:
    time.sleep(rank * 3)
    data = np.ones(10, dtype=np.float32)
    data_sum = np.empty(10, dtype=np.float32)
comm.Bcast(data, root=0)
# comm.Reduce(data, data_sum, op=MPI.SUM, root=0)
print(rank, data, data_sum)