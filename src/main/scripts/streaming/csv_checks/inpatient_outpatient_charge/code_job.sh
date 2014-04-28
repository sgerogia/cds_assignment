#!/bin/bash

JOB_NAME=${3}_codes
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


hadoop jar $HADOOP_STREAMING \
-D mapred.reduce.tasks=1  \
-D mapred.job.name="$JOB_NAME" \
-D stream.non.zero.exit.is.failure=false \
-input $1 \
-input $2 \
-output $JOB_NAME \
-mapper "code_mapper.sh" -file "$DIR/code_mapper.sh" \
-reducer "uniq -c" 

rm -f code_mapper.sh
