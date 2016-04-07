#!/bin/bash

NUMRUNS=5
FROM_NODE=1
TO_NODE=10
SELECTED_QUERIES="Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10:Q11:Q12:Q13:Q14:Q15:Q16:Q17:Q18:Q19:Q20:Q21:Q22"
USER="lab"

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

declare -a optArr=("+hm2set" "+set2arr" "+set2ll" "+cont-flat" "+cstore" "+part" "+hm-part" "+malloc-hoist" "+const-arr" "+comprStrings" "+no-let" "+if-agg" "+old-carr" "+bad-rec" "+str-opt" "+hm-no-col" "+large-out" "+no-field-rem")
declare -a optArrAcronym=("h2s" "s2a" "s2ll" "cf" "cstor" "part" "hpart" "mhoist" "carr" "cmpstr" "nlet" "ifag" "ocar" "brec" "stropt" "hnocol" "largeout" "nfrem")

# screen -L 'source ~/.bash_profile && ./gen_all_mod.sh'

# This script is meant to generate code for different optimization combinations
# for all queries

echo "$(date +"%Y-%m-%d %H:%M:%S") > Sourcing the bash_profile, in case you are using screen command"
source ~/.bash_profile

echo "$(date +"%Y-%m-%d %H:%M:%S") > Moving existing archive folders to main_archive (if any exists) ..."
eval "mkdir -p $GEN_OUT_DIR/main_archive"
eval "mv $GEN_OUT_DIR/archive-* $GEN_OUT_DIR/main_archive/ 2>/dev/null"

# echo "$(date +"%Y-%m-%d %H:%M:%S") > Updating and building scala-yinyang ..."
# eval "cd $GEN_OUT_DIR/../../scala-yinyang && git pull && $GEN_OUT_DIR/../../bin/sbt publish-local"

echo "$(date +"%Y-%m-%d %H:%M:%S") > Updating pardis ..."
eval "cd $GEN_OUT_DIR/../../pardis && git pull"

echo "$(date +"%Y-%m-%d %H:%M:%S") > Updating and building pardis/purgatory-sbt-plugin ..."
eval "cd $GEN_OUT_DIR/../../pardis/purgatory-sbt-plugin && $GEN_OUT_DIR/../../bin/sbt publish-local"

echo "$(date +"%Y-%m-%d %H:%M:%S") > Building pardis ..."
eval "cd $GEN_OUT_DIR/../../pardis && $GEN_OUT_DIR/../../bin/sbt embedAll && $GEN_OUT_DIR/../../bin/sbt publish-local && $GEN_OUT_DIR/../../bin/sbt c-scala-lib/publish-local && $GEN_OUT_DIR/../../bin/sbt c-scala-deep/publish-local && $GEN_OUT_DIR/../../bin/sbt shallow-gen/publish-local"

echo "$(date +"%Y-%m-%d %H:%M:%S") > Updating and compiling NewLegoBase ..."
eval "cd $GEN_OUT_DIR/../../NewLegoBase && git pull && ~/bin/sbt embedAll && ~/bin/sbt compile"

NUM_NODES=$(expr $TO_NODE - $FROM_NODE + 1)
NODES=":$FROM_NODE-$TO_NODE"
QUERIES=(${SELECTED_QUERIES//:/ })
NUM_QUERIES=${#QUERIES[@]}
optCombLen=${#OPTIMIZATION_COMBINATIONS[@]}

declare -a SF_ARR=("1" "8")
for SF in "${SF_ARR[@]}"
do
    eval "cd $GEN_OUT_DIR"
    OUTPUT_DIR="$GEN_OUT_DIR/output-archive-sf$SF-$(date +"%Y%m%d-%H%M%S")"
    echo "$(date +"%Y-%m-%d %H:%M:%S") > Generating and compiling code for SF=$SF ..."
    eval "./run_opt_diff.sh $SELECTED_QUERIES $SF $NUMRUNS"
    eval "rm -rf $GEN_OUT_DIR/sf$SF-build"
    for archivedir in archive-sf$SF-*; do cp -r "$archivedir" "$GEN_OUT_DIR/sf$SF-build"; OUTPUT_DIR="$GEN_OUT_DIR/output-$archivedir"; done;

    echo "$(date +"%Y-%m-%d %H:%M:%S") > Pushing the generated SF=$SF executables to the worker nodes..."
    eval "C3_USER=$USER cexec $NODES 'rm -rf /data/lab/dashti/sf$SF-build'"
    eval "C3_USER=$USER cpush $NODES $GEN_OUT_DIR/sf$SF-build /data/lab/dashti/"

    eval "C3_USER=$USER cexec $NODES 'rm -rf /data/lab/dashti/results'"
    eval "C3_USER=$USER cpush $NODES /data/home/dashti/NewLegoBase/results /data/lab/dashti/"

    eval "ls $GEN_OUT_DIR/sf$SF-build/ | grep .out | shuf > qlist-sf$SF.txt"
    eval "split -nl/$NUM_NODES qlist-sf$SF.txt qpart_"
    COUNTER=$(expr $FROM_NODE - 1)
    for qpartfile in qpart_*; do COUNTER=$(expr $COUNTER + 1); mv "$qpartfile" "querypart_$COUNTER.txt"; done;

    for (( idx = $FROM_NODE; idx <= $TO_NODE; idx+=1 )); do eval "C3_USER=$USER cpush :$idx $GEN_OUT_DIR/querypart_$idx.txt /data/lab/dashti/qlist.txt"; done;

    # eval "C3_USER=$USER cexec $NODES 'cat /data/lab/dashti/qlist.txt'"
    eval "C3_USER=$USER cexec $NODES 'rm -f /data/lab/dashti/KilledProcs.txt'"

    eval "C3_USER=$USER cpush $NODES $GEN_OUT_DIR/memusg.sh /data/lab/dashti/"

    eval "C3_USER=$USER cpush $NODES $GEN_OUT_DIR/kill-freezed-query.sh /data/lab/dashti/"
    eval "C3_USER=$USER cexec $NODES sed -i 's/~/\\\/data\\\/lab\\\/dashti/g' /data/lab/dashti/kill-freezed-query.sh"
    eval "C3_USER=$USER cexec $NODES sed -i 's/generator-out\\\/Q\\\[0-9\\\]\\\*.out/\\\/data\\\/lab\\\/dashti\\\/sf$SF-build\\\/Q.\\\*.out/g' /data/lab/dashti/kill-freezed-query.sh"
    eval "C3_USER=$USER cexec $NODES sed -i 's/print[[:space:]]\\\$2/if\\(\\\$11\ ~\ \\\"\\^\\\/data\\\"\\)\ print\ \\\$2/g' /data/lab/dashti/kill-freezed-query.sh"

    # eval "C3_USER=$USER cexec $NODES '/data/lab/dashti/kill-freezed-query.sh'"

    # eval "C3_USER=$USER cexec $NODES 'ps aux | grep kill'"
    eval "C3_USER=$USER ckill $NODES SCREEN"
    eval "C3_USER=$USER cexec $NODES 'screen -d -m -L /data/lab/dashti/kill-freezed-query.sh'"
    eval "C3_USER=$USER cexec $NODES 'rm -rf /data/lab/dashti/sf$SF-output'"
    eval "C3_USER=$USER cexec $NODES 'mkdir -p /data/lab/dashti/sf$SF-output'"
    eval "C3_USER=$USER cexec $NODES 'rm -rf /data/lab/dashti/sf$SF-perf'"
    eval "C3_USER=$USER cexec $NODES 'mkdir -p /data/lab/dashti/sf$SF-perf'"

    run_for_node () {
      eval "C3_USER=$USER cexec :$1 'cat /data/lab/dashti/qlist.txt | while read query; do echo \"Running \$query ...\"; perf stat -v -d -o /data/lab/dashti/sf$SF-perf/\$query-perf.txt /data/lab/dashti/sf$SF-build/\$query > /data/lab/dashti/sf$SF-output/instrumented_\$query.txt & /data/lab/dashti/memusg.sh > /data/lab/dashti/sf$SF-perf/\$query-memory.txt; wait; /data/lab/dashti/sf$SF-build/\$query > /data/lab/dashti/sf$SF-output/\$query.txt; done; echo \"\$(date +\"%Y-%m-%d %H:%M:%S\") > FINISHED_ON NODE $1\";'"
    }

    for (( idx = $FROM_NODE; idx <= $TO_NODE; idx+=1 ))
    do
            run_for_node $idx &
    done

    wait

    echo "$(date +"%Y-%m-%d %H:%M:%S") > All jobs are done for SF=$SF"


    eval "mkdir -p $OUTPUT_DIR"
    eval "cd $OUTPUT_DIR"
    # for (( idx = $FROM_NODE; idx <= $TO_NODE; idx+=1 )); do eval "C3_USER=$USER cget :$idx /data/lab/dashti/KilledProcs.txt $OUTPUT_DIR/KilledProcs_$idx.txt"; done;
    eval "C3_USER=$USER cget $NODES /data/lab/dashti/KilledProcs.txt $OUTPUT_DIR/"
    eval "cat $OUTPUT_DIR/KilledProcs_* > KilledProcs.txt"
    eval "C3_USER=$USER cget $NODES /data/lab/dashti/sf$SF-output $OUTPUT_DIR/"
    # eval "C3_USER=$USER cget $NODES /data/lab/dashti/sf$SF-output/* $OUTPUT_DIR/"
    eval "C3_USER=$USER cget $NODES /data/lab/dashti/sf$SF-perf $OUTPUT_DIR/"
    # eval "C3_USER=$USER cget $NODES /data/lab/dashti/sf$SF-perf/* $OUTPUT_DIR/"
    eval "rm -rf final-output-sf$SF"
    eval "mkdir -p final-output-sf$SF"
    eval "rm -rf final-perf-sf$SF"
    eval "mkdir -p final-perf-sf$SF"
    for outDir in sf$SF-output*; do eval "mv $outDir/sf$SF-output/* final-output-sf$SF"; done;
    for outDir in sf$SF-perf*; do eval "mv $outDir/sf$SF-perf/* final-perf-sf$SF"; done;

    RESULT_CSV="$OUTPUT_DIR/result.csv"
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
    eval "echo ',TOTAL EXECUTION (sec), Peak Memory (KB), TASK CLOCK, CTX SWITCHES' >> $RESULT_CSV"

    for (( qIdx = 0; qIdx < $NUM_QUERIES; qIdx+=1 ))
    do
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
            QUERY=${QUERIES[$qIdx]}
            EXECUTABLE="$GEN_OUT_DIR/sf$SF-build/$QUERY$currentOptsAcronym.out"
            EXCLUDED=`checkExcluded $QUERY$UNDERLINE$currentOptsIdxs`
            if [ "$EXCLUDED" != "TRUE" ]; then
                eval "echo -n '$QUERY,$currentOptsAcronym,$currentOptsFullName,$COMPILE_TIME,' >> $RESULT_CSV"
                #check whether code is generated
                if [ -f "$GEN_OUT_DIR/sf$SF-build/$QUERY$currentOptsAcronym.c" ]; then
                    #check whether compile correctly
                    if [ -f "$EXECUTABLE" ]; then
                        echo "Processing information for $QUERY with$currentOptsFullName ..."
                        if [ -f "$OUTPUT_DIR/final-output-sf$SF/$QUERY$currentOptsAcronym.out.txt" ]; then
                            if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
                                NUM_COMMAS=$(cat $OUTPUT_DIR/final-output-sf$SF/$QUERY$currentOptsAcronym.out.txt | grep 'Generated code run in' | sed 's/Generated code run in //g' | sed 's/ milliseconds.//g' | tr '\n' ',' | grep -o "," | wc -l)
                                eval "cat $OUTPUT_DIR/final-output-sf$SF/$QUERY$currentOptsAcronym.out.txt | grep 'Generated code run in' | sed 's/Generated code run in //g' | sed 's/ milliseconds.//g' | tr '\n' ',' >> $RESULT_CSV"
                            else
                                eval "cat $OUTPUT_DIR/final-output-sf$SF/$QUERY$currentOptsAcronym.out.txt | grep 'Generated code run in' | sed 's/Generated code run in //g' | sed 's/ milliseconds.//g' | tr '\n' ',' | rev | cut -c 2- | rev >> $RESULT_CSV"
                            fi

                            if [ "$CHECK_CORRECTNESS" = "TRUE" ]; then
                                eval "rm -rf $GEN_OUT_DIR/output_*"
                                eval "cat $OUTPUT_DIR/final-output-sf$SF/$QUERY$currentOptsAcronym.out.txt | sed -e 's/Generated code run in.*/:!~!:/' | awk '/:!~!:/{n++}{if(\$1!=\":!~!:\") print > (\"$GEN_OUT_DIR/output_$QUERY$UNDERLINE\" n \".txt\") }' n=1"
                                for (( i = $NUM_COMMAS; i < $NUMRUNS; i+=1 ))
                                do
                                    eval "echo -n ',' >> $RESULT_CSV"
                                done
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
                                        eval "echo -n 'NOT_FOUND,' >> $RESULT_CSV"
                                    fi
                                done
                                eval "rm -rf $GEN_OUT_DIR/output_*"
                            fi
                        else
                            for (( k = 1; k <= $NUMRUNS; k+=1 ))
                            do
                                eval "echo -n ',' >> $RESULT_CSV"
                            done
                            for (( k = 1; k <= $NUMRUNS; k+=1 ))
                            do
                                eval "echo -n 'NO_OUTPUT_FILE,' >> $RESULT_CSV"
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

                EXEC_TIME="--"
                eval "echo -n '$EXEC_TIME,' >> $RESULT_CSV"

                if [ -f "$OUTPUT_DIR/final-perf-sf$SF/$QUERY$currentOptsAcronym.out-memory.txt" ]; then
                    MEM_USAGE=$(cat $OUTPUT_DIR/final-perf-sf$SF/$QUERY$currentOptsAcronym.out-memory.txt)
                    eval "echo -n '${MEM_USAGE//Peak Memory: /},' >> $RESULT_CSV"
                else
                    eval "echo -n '--,' >> $RESULT_CSV"
                fi
                if [ -f "$OUTPUT_DIR/final-perf-sf$SF/$QUERY$currentOptsAcronym.out-perf.txt" ]; then
                    TASK_CLOCK=$(cat $OUTPUT_DIR/final-perf-sf$SF/$QUERY$currentOptsAcronym.out-perf.txt | grep task-clock.*# | sed 's/ task-clock.*//g' | sed -e 's/[[:space:]]*//' | sed 's/,//')
                    CTX_SWITCHES=$(cat $OUTPUT_DIR/final-perf-sf$SF/$QUERY$currentOptsAcronym.out-perf.txt | grep context-switches.*# | sed 's/ context-switches.*//g' | sed -e 's/[[:space:]]*//' | sed 's/,//')
                    CTX_SWITCHES=$(cat $OUTPUT_DIR/final-perf-sf$SF/$QUERY$currentOptsAcronym.out-perf.txt | grep cpu-migrations.*# | sed 's/ cpu-migrations.*//g' | sed -e 's/[[:space:]]*//' | sed 's/,//')
                    eval "echo -n '$TASK_CLOCK,' >> $RESULT_CSV"
                    eval "echo '$CTX_SWITCHES,' >> $RESULT_CSV"
                else
                    eval "echo -n '--,' >> $RESULT_CSV"
                    eval "echo '--,' >> $RESULT_CSV"
                fi
                eval "rm -f $GEN_OUT_DIR/result.csv"
                eval "cp $RESULT_CSV $GEN_OUT_DIR/result.csv"
            fi
        done
    done

done

# eval "C3_USER=$USER cexec $NODES 'rm -f /data/lab/dashti/qlist.txt'"

eval "C3_USER=$USER ckill $NODES SCREEN"


