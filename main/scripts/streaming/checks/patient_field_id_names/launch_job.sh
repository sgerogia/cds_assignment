#!/bin/bash
hadoop jar $HADOOP_STREAMING \
-D mapred.reduce.tasks=1  \
-D mapred.job.name="patient_field_id_names" \
-D stream.non.zero.exit.is.failure=false \
-input $1 \
-output patient_field_id_names \
-mapper "mapper.sh" -file "./main/scripts/streaming/checks/patient_field_id_names/mapper.sh" \
-reducer "reducer.sh" -file "./main/scripts/streaming/checks/patient_field_id_names/reducer.sh"

rm -f mapper.sh
rm -f reducer.sh