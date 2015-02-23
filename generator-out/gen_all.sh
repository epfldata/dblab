#!/bin/bash

# This script is meant to generate code for different optimization combinations
# for a specific query, and will generate the statistics in CSV format

GEN_OUT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo "Moving existing archive folders to main_archive (if any exists) ..."
eval "mkdir -p $GEN_OUT_DIR/main_archive"
eval "mv $GEN_OUT_DIR/archive-* $GEN_OUT_DIR/main_archive/"

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

echo "Generating and compiling code for SF=1 ..."
eval "./run_opt_diff.sh Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10:Q11:Q12:Q13:Q14:Q15:Q16:Q17:Q18:Q19:Q20:Q21:Q22 1 5"
eval "rm -rf $GEN_OUT_DIR/sf1-build"
for archivedir in archive-sf1-*; do cp -r "$archivedir" "$GEN_OUT_DIR/sf1-build"; done;

echo "Generating and compiling code for SF=8 ..."
eval "./run_opt_diff.sh Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10:Q11:Q12:Q13:Q14:Q15:Q16:Q17:Q18:Q19:Q20:Q21:Q22 8 5"
eval "rm -rf $GEN_OUT_DIR/sf8-build"
for archivedir in archive-sf8-*; do cp -r "$archivedir" "$GEN_OUT_DIR/sf8-build"; done;


echo "Pushing the generated executables to the worker nodes..."
eval "C3_USER=yarn cexec :1-10 'rm -rf /data/lab/dashti/sf1-build'"
eval "C3_USER=yarn cpush :1-10 /data/home/dashti/NewLegoBase/generator-out/sf1-build /data/lab/dashti/"

eval "C3_USER=yarn cexec :1-10 'rm -rf /data/lab/dashti/sf8-build'"
eval "C3_USER=yarn cpush :1-10 /data/home/dashti/NewLegoBase/generator-out/sf8-build /data/lab/dashti/"
