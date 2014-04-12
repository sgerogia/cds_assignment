#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$DIR/job.sh $1 "<rows>"
$DIR/job.sh $1 "</rows>"
