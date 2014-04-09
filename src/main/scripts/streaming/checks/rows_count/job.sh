#!/bin/bash

JOB_NAME=rows_count
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

hadoop jar $HADOOP_STREAMING \
-D mapred.reduce.tasks=1  \
-D mapred.job.name="$JOB_NAME" \
-D stream.non.zero.exit.is.failure=false \
-input $1 \
-output ${JOB_NAME}_$2 \
-mapper "grep -c '$2'" \
-reducer "reducer.sh" -file "$DIR/reducer.sh"

rm -f reducer.sh
