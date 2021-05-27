#!/bin/bash
logging_file=$HOPLITE_LOGGING_DIR/rank_$OMPI_COMM_WORLD_RANK.log
source ~/anaconda3/etc/profile.d/conda.sh
conda activate
python hoplite_microbenchmarks.py $@ 2>&1 | tee $logging_file
