#!/bin/bash
logging_file=$HOPLITE_LOGGING_DIR/rank_$OMPI_COMM_WORLD_RANK.log
( $@ 2>&1 & echo $! > /tmp/hoplite_test.pid ) | tee $logging_file &
wait
sleep 10  # wait for fault-tolerance
