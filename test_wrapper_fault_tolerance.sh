#!/bin/bash
trap 'echo delaying MPI shutdown...' INT TERM
logging_file=$HOPLITE_LOGGING_DIR/rank_$OMPI_COMM_WORLD_RANK.log
$@ 2>&1 | tee $logging_file
sleep 10
