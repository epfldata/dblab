#!/bin/bash

# This script is meant to generate code for different optimization combinations
# for a specific query, and will generate the statistics in CSV format


GEN_OUT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

#reading parameters from config file: DATA_FOLDER
CONFIG_FILE="run_config.cfg"
if [ ! -f "$GEN_OUT_DIR/$CONFIG_FILE" ]; then
    echo "Config file does not exist."
    echo "You can rename run_config.example.cfg to $CONFIG_FILE and change the parameters inside."
    exit 1
fi
source $CONFIG_FILE
if [ -z "$DATA_FOLDER" ]; then echo "DATA_FOLDER is not declared in $CONFIG_FILE."; exit 1; fi
if [ -z "$OPTIMIZATION_COMBINATIONS" ]; then echo "OPTIMIZATION_COMBINATIONS is not declared in $CONFIG_FILE."; exit 1; fi

if [ $# -ne 3 ]; then
    echo "Incorrect number of command line arguments provided (you gave $# and 3 are needed)."
    echo "Usage ./run_opt_diff.sh <TPCH_QUERY list> <SCALING_FACTOR> <NUM_RUNS_PER_QUERY>"
    echo "Example ./run_opt_diff.sh Q3:Q18:Q7 1 3"
    exit
fi

declare -a optArr=("+hm2set" "+set2arr" "+set2ll" "+cont-flat" "+cstore" "+part" "+hm-part" "+malloc-hoist" "+const-arr" "+comprStrings" "+no-let" "+if-agg" "+old-carr" "+bad-rec" "+str-opt" "+hm-no-col" "+large-out")
declare -a optArrAcronym=("h2s" "s2a" "s2ll" "cf" "cstor" "part" "hpart" "mhoist" "carr" "cmpstr" "nlet" "ifag" "ocar" "brec" "stropt" "hnocol" "largeout")

eval "cd $GEN_OUT_DIR/.."

SELECTED_QUERIES=$1
SF=$2
NUMRUNS=$3
VERBOSE=true
LANG="_C"
UNDERLINE="_"

ARCHIVE_DIR="$GEN_OUT_DIR/archive-$(date +"%Y%m%d-%H%M%S")"

if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
    if [ "$SF" = "0.1" ] || [ "$SF" = "1" ]  || [ "$SF" = "8" ]; then
        CHECK_CORRECTNESS="TRUE"
    else
        CHECK_CORRECTNESS="FALSE"
    fi
fi

eval "mkdir $ARCHIVE_DIR"

RESULT_CSV="$ARCHIVE_DIR/result.csv"

eval "rm -f $RESULT_CSV"

eval "echo -n 'Query,OptimizationCombinationAccronym,OptimizationCombinationFullName' >> $RESULT_CSV"
for (( i = 1; i <= $NUMRUNS; i+=1 ))
do
    eval "echo -n ',RUN #$i$LAST_CHAR' >> $RESULT_CSV"
done
if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
    for (( i = 1; i <= $NUMRUNS; i+=1 ))
    do
        eval "echo -n ',RUN #$i$LAST_CHAR CHECK' >> $RESULT_CSV"
    done 
fi
eval "echo '' >> $RESULT_CSV"

optCombLen=${#OPTIMIZATION_COMBINATIONS[@]}


QUERIES=(${SELECTED_QUERIES//:/ })
NUM_QUERIES=${#QUERIES[@]}
for (( qIdx = 0; qIdx < $NUM_QUERIES; qIdx+=1 ))
do
    QUERY=${QUERIES[$qIdx]}
    EXECUTABLE="$GEN_OUT_DIR/$QUERY.out"

    for (( idx = 0; idx < $optCombLen; idx+=1 ))
    do
        currentOptsIdxs=${OPTIMIZATION_COMBINATIONS[$idx]}
        opts=(${currentOptsIdxs//:/ })
        optsLen=${#opts[@]}
        currentOpts=""
        currentOptsAcronym=""
        currentOptsFullName=""
        for (( opt = 0; opt < $optsLen; opt+=1 ))
        do
            currentOpts="$currentOpts ${optArr[${opts[opt]}]}"
            currentOptsAcronym="$currentOptsAcronym-${optArrAcronym[${opts[opt]}]}"
            currentOptsFullName="$currentOptsFullName ${optArr[${opts[opt]}]}"
        done

        eval "rm -rf $GEN_OUT_DIR/*.out*"
        eval "rm -rf $GEN_OUT_DIR/*.c"

        eval "sed -i -e 's/val numRuns: scala.Int = [0-9]*/val numRuns: scala.Int = $NUMRUNS/g' $GEN_OUT_DIR/../lego/src/main/scala/Config.scala"
        eval "sbt ';project legocompiler ;generate-test $DATA_FOLDER $SF $QUERY$LANG $currentOpts'"
        eval "sed -i -e 's/val numRuns: scala.Int = [0-9]*/val numRuns: scala.Int = 1/g' $GEN_OUT_DIR/../lego/src/main/scala/Config.scala"

        eval "make -C $GEN_OUT_DIR"
        eval "rm -rf $GEN_OUT_DIR/output_$QUERY*"

        eval "echo -n '$QUERY,$currentOptsAcronym,$currentOptsFullName,' >> $RESULT_CSV"
        eval "$EXECUTABLE > $GEN_OUT_DIR/output_$QUERY.txt"
        if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
            eval "cat $GEN_OUT_DIR/output_$QUERY.txt | grep Generated | sed 's/Generated code run in //g' | sed 's/ milliseconds.//g' | tr '\n' ',' | rev | cut -c 2- | rev | tr '\n' ',' >> $RESULT_CSV"
        else
            eval "cat $GEN_OUT_DIR/output_$QUERY.txt | grep Generated | sed 's/Generated code run in //g' | sed 's/ milliseconds.//g' | tr '\n' ',' | rev | cut -c 2- | rev >> $RESULT_CSV"
        fi
        eval "cat $GEN_OUT_DIR/output_$QUERY.txt | sed -e 's/Gen.*/\:/' | awk '/:/{n++}{if(\$1!=\":\") print > (\"$GEN_OUT_DIR/output_$QUERY$UNDERLINE\" n \".txt\") }' n=1"

        if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
            ECHO_COMMAND="echo -n"
            DELIMITER=","
            for (( i = 1; i <= $NUMRUNS; i+=1 ))
            do
                REF_RESULT_FILE="$GEN_OUT_DIR/../results/$QUERY.result_sf$SF"
                if [ "$SF" = "8" ]; then
                    REF_RESULT_FILE="$GEN_OUT_DIR/../results/sf$SF/$QUERY.result"
                fi

                if [ "$i" = "$NUMRUNS" ]; then
                    ECHO_COMMAND="echo"
                    DELIMITER=""
                fi
                if [ ! -f "$GEN_OUT_DIR/output_$QUERY$UNDERLINE$i.txt" ]; then
                    eval "$ECHO_COMMAND 'NOT_FOUND$DELIMITER' >> $RESULT_CSV"
                else
                    if [ ! -f "$REF_RESULT_FILE" ]; then
                        eval "$ECHO_COMMAND 'NO_REF$DELIMITER' >> $RESULT_CSV"
                    else
                        if `diff $GEN_OUT_DIR/output_$QUERY$UNDERLINE$i.txt $REF_RESULT_FILE >/dev/null` ; then
                            eval "$ECHO_COMMAND 'OK$DELIMITER' >> $RESULT_CSV"
                        else
                            eval "$ECHO_COMMAND 'WRONG_RESULT$DELIMITER' >> $RESULT_CSV"
                        fi
                    fi
                fi
            done
        fi

        eval "rm -f $GEN_OUT_DIR/result.csv"
        eval "cp $RESULT_CSV $GEN_OUT_DIR/result.csv"

        eval "rm -rf $GEN_OUT_DIR/output_$QUERY*"

        eval "mv $EXECUTABLE $ARCHIVE_DIR/$QUERY$currentOptsAcronym.out"
        eval "mv $GEN_OUT_DIR/$QUERY.c $ARCHIVE_DIR/$QUERY$currentOptsAcronym.c"
    done
done
