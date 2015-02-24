#!/bin/bash

# screen -L 'source ~/.bash_profile && ./gen_all_mod.sh'

# This script is meant to generate code for different optimization combinations
# for all queries

echo "Sourcing the bash_profile, in case you are using screen command"
source ~/.bash_profile

GEN_OUT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo "Moving existing archive folders to main_archive (if any exists) ..."
eval "mkdir -p $GEN_OUT_DIR/main_archive"
eval "mv $GEN_OUT_DIR/archive-* $GEN_OUT_DIR/main_archive/ 2>/dev/null"

# echo "Updating and building scala-yinyang ..."
# eval "cd $GEN_OUT_DIR/../../scala-yinyang && git pull && $GEN_OUT_DIR/../../bin/sbt publish-local"

echo "Updating pardis ..."
eval "cd $GEN_OUT_DIR/../../pardis && git pull"

echo "Updating and building pardis/purgatory-sbt-plugin ..."
eval "cd $GEN_OUT_DIR/../../pardis/purgatory-sbt-plugin && $GEN_OUT_DIR/../../bin/sbt publish-local"

echo "Building pardis ..."
eval "cd $GEN_OUT_DIR/../../pardis && $GEN_OUT_DIR/../../bin/sbt embedAll && $GEN_OUT_DIR/../../bin/sbt publish-local && $GEN_OUT_DIR/../../bin/sbt c-scala-lib/publish-local && $GEN_OUT_DIR/../../bin/sbt c-scala-deep/publish-local && $GEN_OUT_DIR/../../bin/sbt shallow-gen/publish-local"

echo "Updating and compiling NewLegoBase ..."
eval "cd $GEN_OUT_DIR/../../NewLegoBase && git pull && ~/bin/sbt embedAll && ~/bin/sbt compile"

eval "cd $GEN_OUT_DIR"


FROM_NODE=2
TO_NODE=10
NUM_NODES=$(expr $TO_NODE - $FROM_NODE + 1)
NODES=":$FROM_NODE-$TO_NODE"
declare -a SF_ARR=("1" "8")
for SF in "${SF_ARR[@]}"
do
    echo "Generating and compiling code for SF=$SF ..."
    eval "./run_opt_diff.sh Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10:Q11:Q12:Q13:Q14:Q15:Q16:Q17:Q18:Q19:Q20:Q21:Q22 $SF 5"
    eval "rm -rf $GEN_OUT_DIR/sf$SF-build"
    for archivedir in archive-sf$SF-*; do cp -r "$archivedir" "$GEN_OUT_DIR/sf$SF-build"; done;

    echo "Pushing the generated SF=$SF executables to the worker nodes..."
    eval "C3_USER=yarn cexec $NODES 'rm -rf /data/lab/dashti/sf$SF-build'"
    eval "C3_USER=yarn cpush $NODES $GEN_OUT_DIR/sf$SF-build /data/lab/dashti/"

    eval "C3_USER=yarn cpush $NODES /data/home/dashti/NewLegoBase/results /data/lab/dashti/"

    eval "cd $GEN_OUT_DIR"
    eval "ls $GEN_OUT_DIR/sf$SF-build/ | grep .out > qlist-sf$SF.txt"
    eval "split -nl/$NUM_NODES qlist-sf$SF.txt qpart_"
    COUNTER=$(expr $FROM_NODE - 1)
    for qpartfile in qpart_*; do COUNTER=$(expr $COUNTER + 1); mv "$qpartfile" "querypart_$COUNTER.txt"; done;

    for (( idx = $FROM_NODE; idx <= $TO_NODE; idx+=1 )); do eval "C3_USER=yarn cpush :$idx $GEN_OUT_DIR/querypart_$idx.txt /data/lab/dashti/qlist.txt"; done;

    # eval "C3_USER=yarn cexec $NODES 'cat /data/lab/dashti/qlist.txt'"
    eval "C3_USER=yarn cexec $NODES 'rm -f /data/lab/dashti/KilledProcs.txt'"

    eval "C3_USER=yarn cpush $NODES $GEN_OUT_DIR/memusg.sh /data/lab/dashti/"

    eval "C3_USER=yarn cpush $NODES $GEN_OUT_DIR/kill-freezed-query.sh /data/lab/dashti/"
    eval "C3_USER=yarn cexec $NODES sed -i 's/~/\\\/data\\\/lab\\\/dashti/g' /data/lab/dashti/kill-freezed-query.sh"
    eval "C3_USER=yarn cexec $NODES sed -i 's/generator-out\\\/Q\\\[0-9\\\]\\\*.out/\\\/data\\\/lab\\\/dashti\\\/sf$SF-build\\\/Q.\\\*.out/g' /data/lab/dashti/kill-freezed-query.sh"
    eval "C3_USER=yarn cexec $NODES sed -i 's/print[[:space:]]\\\$2/if\\(\\\$11\ ~\ \\\"\\^\\\/data\\\"\\)\ print\ \\\$2/g' /data/lab/dashti/kill-freezed-query.sh"

    # eval "C3_USER=yarn cexec $NODES '/data/lab/dashti/kill-freezed-query.sh'"

    # eval "C3_USER=yarn cexec $NODES 'ps aux | grep kill'"
    eval "C3_USER=yarn ckill $NODES SCREEN"
    eval "C3_USER=yarn cexec $NODES 'screen -d -m -L /data/lab/dashti/kill-freezed-query.sh'"
    eval "C3_USER=yarn cexec $NODES 'rm -rf /data/lab/dashti/sf$SF-output'"
    eval "C3_USER=yarn cexec $NODES 'mkdir -p /data/lab/dashti/sf$SF-output'"
    eval "C3_USER=yarn cexec $NODES 'rm -rf /data/lab/dashti/sf$SF-perf'"
    eval "C3_USER=yarn cexec $NODES 'mkdir -p /data/lab/dashti/sf$SF-perf'"

    run_for_node () {
      eval "C3_USER=yarn cexec :$1 'cat /data/lab/dashti/qlist.txt | while read query; do echo \"Running \$query ...\"; perf stat -v -d -o /data/lab/dashti/sf$SF-perf/\$query-perf.txt /data/lab/dashti/sf$SF-build/\$query > /data/lab/dashti/sf$SF-output/\$query.txt & /data/lab/dashti/memusg.sh > /data/lab/dashti/sf$SF-perf/\$query-memory.txt; wait;  done; echo \"FINISHED_ON NODE $1\";'"
    }

    for (( idx = $FROM_NODE; idx <= $TO_NODE; idx+=1 ))
    do
            run_for_node $idx &
    done

    wait

    echo "All jobs are done for SF=$SF"
done

# eval "C3_USER=yarn cexec $NODES 'rm -f /data/lab/dashti/qlist.txt'"

eval "C3_USER=yarn ckill $NODES SCREEN"


