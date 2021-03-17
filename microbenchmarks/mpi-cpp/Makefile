EXECS=multicast reduce allreduce gather allgather send_recv
MPICC?=mpicc

all: ${EXECS}

multicast: multicast.c
	${MPICC} -O2 -o multicast multicast.c

reduce: reduce.c
	${MPICC} -O2 -o reduce reduce.c

allreduce: allreduce.c
	${MPICC} -O2 -o allreduce allreduce.c

gather: gather.c
	${MPICC} -O2 -o gather gather.c

allgather: allgather.c
	${MPICC} -O2 -o allgather allgather.c

send_recv: send_recv.c
	${MPICC} -O2 -o send_recv send_recv.c

clean:
	rm -f ${EXECS}
