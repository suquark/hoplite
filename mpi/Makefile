EXECS=compare_bcast reduce
MPICC?=mpicc

all: ${EXECS}

compare_bcast: compare_bcast.c
	${MPICC} -O2 -o compare_bcast compare_bcast.c

reduce: reduce.c
	${MPICC} -O2 -o reduce reduce.c

clean:
	rm -f ${EXECS}
