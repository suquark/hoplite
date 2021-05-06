EXECS=multicast reduce allreduce gather allgather roundtrip
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

roundtrip: roundtrip.c
	${MPICC} -O2 -o roundtrip roundtrip.c

clean:
	rm -f ${EXECS}
