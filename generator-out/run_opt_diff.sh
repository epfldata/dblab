#!/bin/bash

# This script is meant to generate code for different optimization combinations
# for a specific query, and will generate the statistics in CSV format

#Hard-coded parameters
DATA_FOLDER="/Users/dashti/Desktop"

if [ $# -ne 3 ]; then
    echo "Incorrect number of command line arguments provided (you gave $# and 3 are needed)."
    echo "Usage ./run_opt_diff.sh <TPCH_QUERY> <SCALING_FACTOR> <NUM_RUNS_PER_QUERY>"
    echo "Example ./run_opt_diff.sh Q3 1 3"
    exit
fi

GEN_OUT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

eval "cd $GEN_OUT_DIR/.."

QUERY=$1
SF=$2
NUMRUNS=$3
EXECUTABLE="$GEN_OUT_DIR/$QUERY.out"
VERBOSE=true
LANG="_C"

ARCHIVE_DIR="$GEN_OUT_DIR/archive-$(date +"%Y%m%d-%H%M%S")"

eval "mkdir $ARCHIVE_DIR"

RESULT_CSV="$ARCHIVE_DIR/result.csv"

eval "rm -f $RESULT_CSV"

eval "echo -n 'Query,' >> $RESULT_CSV"
LAST_CHAR=""
for (( i = 1; i <= $NUMRUNS; i+=1 ))
do
    if [ "$i" = "$NUMRUNS" ]; then
        LAST_CHAR=""
    else
        LAST_CHAR=","
    fi
    eval "echo -n 'RUN #$i$LAST_CHAR' >> $RESULT_CSV"
done 
eval "echo '' >> $RESULT_CSV"

declare -a optArr=("+hm2set" "+set2arr" "+set2ll" "+cont-flat" "+cstore" "+part" "+hm-part" "+malloc-hoist" "+const-arr" "+comprStrings" "+no-let" "+if-agg" "+old-carr")
declare -a optArrAcronym=("h2s" "s2a" "s2ll" "cf" "cstor" "part" "hpart" "mhoist" "carr" "cmpstr" "nlet" "ifag" "ocar")
declare -a optComb=("0" "0:1" "0:2" "0:2:3" "4" "5" "6" "7" "8" "10" "11" "12")

optCombLen=${#optComb[@]}

for (( idx = 0; idx < $optCombLen; idx+=1 ))
do
    currentOptsIdxs=${optComb[$idx]}
    opts=(${currentOptsIdxs//:/ })
    optsLen=${#opts[@]}
    currentOpts=""
    currentOptsAcronym=""
    for (( opt = 0; opt < $optsLen; opt+=1 ))
    do
        currentOpts="$currentOpts ${optArr[${opts[opt]}]}"
        currentOptsAcronym="$currentOptsAcronym-${optArrAcronym[${opts[opt]}]}"
    done

    eval "rm -rf $GEN_OUT_DIR/*.out*"

    eval "rm -rf $GEN_OUT_DIR/*.c"

    eval "sbt ';project legocompiler ;generate-test $DATA_FOLDER $SF $QUERY$LANG $currentOpts'"

    eval "make -C $GEN_OUT_DIR"

    eval "echo -n '$QUERY,$currentOptsAcronym,' >> $RESULT_CSV"

    LAST_COMMAND=""
    for (( i = 1; i <= $NUMRUNS; i+=1 ))
    do
        if [ "$i" = "$NUMRUNS" ]; then
            LAST_COMMAND=""
        else
            LAST_COMMAND=" | tr '\n' ','"
        fi
        eval "$EXECUTABLE | grep Generated | sed 's/Generated code run in //g' | sed 's/ milliseconds.//g'$LAST_COMMAND >> $RESULT_CSV"
        eval "rm -f $GEN_OUT_DIR/result.csv"
        eval "cp $RESULT_CSV $GEN_OUT_DIR/result.csv"
    done

    eval "mv $EXECUTABLE $ARCHIVE_DIR/$QUERY$currentOptsAcronym.out"
    eval "mv $GEN_OUT_DIR/$QUERY.c $ARCHIVE_DIR/$QUERY$currentOptsAcronym.c"
done

