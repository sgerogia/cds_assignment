#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

JOB_PREFIX=inpatient_outpatient_charge

$DIR/code_job.sh $1 $2 $JOB_PREFIX
$DIR/count_job.sh $1 $2 $JOB_PREFIX
