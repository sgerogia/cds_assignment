#!/bin/bash
grep "field" | grep "$1" | awk -F "[<>]" '{print length($3)}'