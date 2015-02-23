#!/bin/bash

PROC_NAME="generator-out/Q[0-9]*.out"

echo "Started"
while true
do
    echo "Checking..."
    # Get all PIDs for process name
    procs=(`ps aux | grep  $PROC_NAME | awk '{print $2}'`)

    # for each PID in PIDs array
    for pid in $procs; do
        # get elapsed time in form mm:ss and remove ":" character
        # to make it easier to parse time 
        time=(`ps -o etime $pid | sed -e 's/[:-]/ /g'`)
        # get minutes from time
        min=${time[1]}
        # if proces runs 8 minutes then kill it
        if [ "$min" -gt "8" ]; then
            eval "ps aux | awk '(\$2 == $pid){print \"Killed \" \$1 \" \" \$2 \" \" \$3 \" \" \$4 \" \" \$5 \" \" \$6 \" \" \$7 \" \" \$8 \" \" \$9 \" \" \$10 \" \" \$11 \" \" \$12}' >> ~/KilledProcs.txt"
            eval "ps aux | awk '(\$2 == $pid){print \"Killed \" \$1 \" \" \$2 \" \" \$3 \" \" \$4 \" \" \$5 \" \" \$6 \" \" \$7 \" \" \$8 \" \" \$9 \" \" \$10 \" \" \$11 \" \" \$12}'"
    		procs=(`ps aux | grep  $PROC_NAME`)
            kill -9 $pid
        fi
    done
    sleep 3m
done
