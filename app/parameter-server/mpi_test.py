import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    data = np.ones(10, dtype=np.float32)
else:
    data = np.empty(10, dtype=np.float32)

comm.Bcast(data, root=0)
print(rank, data)