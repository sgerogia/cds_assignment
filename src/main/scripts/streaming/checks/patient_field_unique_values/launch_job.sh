#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$DIR/job.sh $1 age age
$DIR/job.sh $1 gender gndr
$DIR/job.sh $1 income inc
$DIR/length_job.sh $1 id id

