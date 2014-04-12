#!/bin/bash

JOB_NAME=patient_field_id_names
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


hadoop jar $HADOOP_STREAMING \
-D mapred.reduce.tasks=1  \
-D mapred.job.name="$JOB_NAME" \
-D stream.non.zero.exit.is.failure=false \
-input $1 \
-output $JOB_NAME \
-mapper "mapper.sh" -file "$DIR/mapper.sh" \
-reducer "uniq -c"

rm -f mapper.sh