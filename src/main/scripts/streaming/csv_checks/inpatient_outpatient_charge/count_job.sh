#!/bin/bash

JOB_NAME=${3}_line_count
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


hadoop jar $HADOOP_STREAMING \
-D mapred.reduce.tasks=1  \
-D mapred.job.name="$JOB_NAME" \
-D stream.non.zero.exit.is.failure=false \
-input $1 \
-input $2 \
-output $JOB_NAME \
-mapper "count_mapper.sh" -file "$DIR/count_mapper.sh" \
-combiner "count_reducer.sh" \
-reducer "count_reducer.sh" -file "$DIR/count_reducer.sh"

rm -f count_reducer.sh
rm -f count_mapper.sh