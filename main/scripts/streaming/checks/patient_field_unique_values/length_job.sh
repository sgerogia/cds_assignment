#!/bin/bash

JOB_NAME=patient_field_unique_values
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

hadoop jar $HADOOP_STREAMING \
-D mapred.reduce.tasks=1  \
-D mapred.job.name=${JOB_NAME}_$2 \
-D stream.non.zero.exit.is.failure=false \
-input $1 \
-output ${JOB_NAME}_$2 \
-mapper "length_mapper.sh $3" -file "$DIR/length_mapper.sh" \
-reducer "uniq -c"

rm -f length_mapper.sh
rm -f reducer.sh
