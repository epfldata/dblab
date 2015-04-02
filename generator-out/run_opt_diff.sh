#!/bin/bash

# This script is meant to generate code for different optimization combinations
# for a specific query, and will generate the statistics in CSV format

exclude () {
  KEY="$1"
  KEY=${KEY//:/_}
  eval hash"$KEY"='TRUE'
}

checkExcluded () {
  KEY="$1"
  KEY=${KEY//:/_}
  eval echo '${hash'"$KEY"'#hash}'
}

GEN_OUT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
eval "cd $GEN_OUT_DIR"

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

ARCHIVE_DIR="$GEN_OUT_DIR/archive-sf$SF-$(date +"%Y%m%d-%H%M%S")"
RESULT_CSV="$ARCHIVE_DIR/result.csv"

eval "mkdir $ARCHIVE_DIR"

if [ "$EXECUTE_GENERATED_CODE" = "TRUE" ]; then
    if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
        if [ "$SF" = "0.1" ] || [ "$SF" = "1" ]  || [ "$SF" = "8" ]; then
            CHECK_CORRECTNESS="TRUE"
        else
            CHECK_CORRECTNESS="FALSE"
        fi
    fi

    eval "rm -f $RESULT_CSV"

    eval "echo -n 'Query,OptimizationCombinationAccronym,OptimizationCombinationFullName,COMPILE TIME (sec)' >> $RESULT_CSV"
    for (( i = 1; i <= $NUMRUNS; i+=1 ))
    do
        eval "echo -n ',RUN #$i (ms)' >> $RESULT_CSV"
    done
    if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
        for (( i = 1; i <= $NUMRUNS; i+=1 ))
        do
            eval "echo -n ',RUN #$i CHECK' >> $RESULT_CSV"
        done 
    fi
    eval "echo ',TOTAL EXECUTION (sec)' >> $RESULT_CSV"
fi

optCombLen=${#OPTIMIZATION_COMBINATIONS[@]}

QUERIES=(${SELECTED_QUERIES//:/ })
NUM_QUERIES=${#QUERIES[@]}
ALL_SELECTED_QUERIES="${SELECTED_QUERIES//:/_C }$LANG"
for (( idx = 0; idx < $optCombLen; idx+=1 ))
do
    START_TIME=$(date +%s)

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
    eval "sbt ';project lego-compiler ;run $DATA_FOLDER $SF $ALL_SELECTED_QUERIES $currentOpts'"
    eval "sed -i -e 's/val numRuns: scala.Int = [0-9]*/val numRuns: scala.Int = 1/g' $GEN_OUT_DIR/../lego/src/main/scala/Config.scala"

    eval "make -C $GEN_OUT_DIR"
    END_TIME=$(date +%s)
    COMPILE_TIME=$(( $END_TIME - $START_TIME ))
    for (( qIdx = 0; qIdx < $NUM_QUERIES; qIdx+=1 ))
    do
        START_TIME=$(date +%s)
        QUERY=${QUERIES[$qIdx]}
        EXECUTABLE="$GEN_OUT_DIR/$QUERY.out"
        EXCLUDED=`checkExcluded $QUERY$UNDERLINE$currentOptsIdxs`
        if [ "$EXCLUDED" != "TRUE" ]; then
            if [ "$EXECUTE_GENERATED_CODE" = "TRUE" ]; then
                eval "rm -rf $GEN_OUT_DIR/output_$QUERY*"
                eval "echo -n '$QUERY,$currentOptsAcronym,$currentOptsFullName,$COMPILE_TIME,' >> $RESULT_CSV"
                #check whether code is generated
                if [ -f "$GEN_OUT_DIR/$QUERY.c" ]; then
                    #check whether compile correctly
                    if [ -f "$EXECUTABLE" ]; then
                        echo "Executing $QUERY with$currentOptsFullName ..."
                        eval "sed -i 's/\/data\/lab\/dashti\/\.\*Q\.\*\.out/generator-out\/Q[0-9]\*\.out/g'  $GEN_OUT_DIR/memusg.sh"
                        eval "perf stat -v -d -o $GEN_OUT_DIR/$QUERY-perf.txt $EXECUTABLE $currentOptsAcronym > $GEN_OUT_DIR/instrumented_output_$QUERY.txt & $GEN_OUT_DIR/memusg.sh > $GEN_OUT_DIR/$QUERY-memory.txt; wait; $EXECUTABLE $currentOptsAcronym > $GEN_OUT_DIR/output_$QUERY.txt;"
                        eval "sed -i 's/generator-out\/Q\[0-9\]\*\.out/\/data\/lab\/dashti\/\.\*Q\.\*\.out/g'  $GEN_OUT_DIR/memusg.sh"
                        if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
                            eval "cat $GEN_OUT_DIR/output_$QUERY.txt | grep 'Generated code run in' | sed 's/Generated code run in //g' | sed 's/ milliseconds.//g' | tr '\n' ',' >> $RESULT_CSV"
                        else
                            eval "cat $GEN_OUT_DIR/output_$QUERY.txt | grep 'Generated code run in' | sed 's/Generated code run in //g' | sed 's/ milliseconds.//g' | tr '\n' ',' | rev | cut -c 2- | rev >> $RESULT_CSV"
                        fi
                        eval "cat $GEN_OUT_DIR/output_$QUERY.txt | sed -e 's/Generated code run in.*/:!~!:/' | awk '/:!~!:/{n++}{if(\$1!=\":!~!:\") print > (\"$GEN_OUT_DIR/output_$QUERY$UNDERLINE\" n \".txt\") }' n=1"

                        if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
                            for (( i = 1; i <= $NUMRUNS; i+=1 ))
                            do
                                REF_RESULT_FILE="$GEN_OUT_DIR/../results/$QUERY.result_sf$SF"
                                if [ "$SF" = "8" ]; then
                                    REF_RESULT_FILE="$GEN_OUT_DIR/../results/sf$SF/$QUERY.result"
                                fi
                                if [ -f "$GEN_OUT_DIR/output_$QUERY$UNDERLINE$i.txt" ]; then
                                    if [ -f "$REF_RESULT_FILE" ]; then
                                        if `diff $GEN_OUT_DIR/output_$QUERY$UNDERLINE$i.txt $REF_RESULT_FILE >/dev/null` ; then
                                            eval "echo -n 'OK,' >> $RESULT_CSV"
                                        else
                                            eval "echo -n 'WRONG_RESULT,' >> $RESULT_CSV"
                                        fi
                                    else
                                        eval "echo -n 'NO_REF,' >> $RESULT_CSV"
                                    fi
                                else
                                    if [ "$i" = "1" ]; then
                                        for (( k = 1; k <= $NUMRUNS; k+=1 ))
                                        do
                                            eval "echo -n ',' >> $RESULT_CSV"
                                        done
                                    fi
                                    eval "echo -n 'NOT_FOUND,' >> $RESULT_CSV"
                                fi
                            done
                        fi
                    else #otherwise, show compilation error in output
                        for (( k = 1; k <= $NUMRUNS; k+=1 ))
                        do
                            eval "echo -n ',' >> $RESULT_CSV"
                        done
                        for (( k = 1; k <= $NUMRUNS; k+=1 ))
                        do
                            eval "echo -n 'COMPILE_ERROR,' >> $RESULT_CSV"
                        done
                    fi
                else #otherwise, show code generation error in output
                    for (( k = 1; k <= $NUMRUNS; k+=1 ))
                    do
                        eval "echo -n ',' >> $RESULT_CSV"
                    done
                    for (( k = 1; k <= $NUMRUNS; k+=1 ))
                    do
                        eval "echo -n 'CODE_GEN_ERROR,' >> $RESULT_CSV"
                    done
                fi

                END_TIME=$(date +%s)
                EXEC_TIME=$(( $END_TIME - $START_TIME ))
                eval "echo '$EXEC_TIME' >> $RESULT_CSV"

                eval "rm -f $GEN_OUT_DIR/result.csv"
                eval "cp $RESULT_CSV $GEN_OUT_DIR/result.csv"
                eval "mv $GEN_OUT_DIR/output_$QUERY* $ARCHIVE_DIR/"
                eval "mv $GEN_OUT_DIR/$QUERY-memory.txt $ARCHIVE_DIR/$QUERY$currentOptsAcronym-memory.txt"
                eval "mv $GEN_OUT_DIR/$QUERY-perf.txt $ARCHIVE_DIR/$QUERY$currentOptsAcronym-perf.txt"
                eval "mv $GEN_OUT_DIR/instrumented_output_$QUERY* $ARCHIVE_DIR/"
            fi
        fi
        eval "mv $EXECUTABLE $ARCHIVE_DIR/$QUERY$currentOptsAcronym.out"
        eval "mv $GEN_OUT_DIR/$QUERY.c $ARCHIVE_DIR/$QUERY$currentOptsAcronym.c"
    done
done

