#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../inpatient_outpatient_charge" && pwd )"

JOB_PREFIX=inpatient_outpatient_summary_by_code_state

$DIR/code_job.sh $1 $2 $JOB_PREFIX
$DIR/count_job.sh $1 $2 $JOB_PREFIX
