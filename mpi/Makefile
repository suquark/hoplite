EXECS=compare_bcast reduce allreduce gather
MPICC?=mpicc

all: ${EXECS}

compare_bcast: compare_bcast.c
	${MPICC} -O2 -o compare_bcast compare_bcast.c

reduce: reduce.c
	${MPICC} -O2 -o reduce reduce.c

allreduce: allreduce.c
	${MPICC} -O2 -o allreduce allreduce.c

gather: gather.c
	${MPICC} -O2 -o gather gather.c

clean:
	rm -f ${EXECS}
