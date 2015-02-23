#!/bin/bash

# This script is meant to generate code for different optimization combinations
# for a specific query, and will generate the statistics in CSV format

GEN_OUT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

eval "cd $GEN_OUT_DIR/../../scala-yinyang && git pull && $GEN_OUT_DIR/../../bin/sbt publish-local"

eval "cd $GEN_OUT_DIR/../../pardis && git pull"

eval "cd $GEN_OUT_DIR/../../pardis/purgatory-sbt-plugin && $GEN_OUT_DIR/../../bin/sbt publish-local"

eval "cd $GEN_OUT_DIR/../../pardis && $GEN_OUT_DIR/../../bin/sbt embedAll && $GEN_OUT_DIR/../../bin/sbt publish-local && $GEN_OUT_DIR/../../bin/sbt c-scala-lib/publish-local && $GEN_OUT_DIR/../../bin/sbt c-scala-deep/publish-local && $GEN_OUT_DIR/../../bin/sbt shallow-gen/publish-local"

eval "cd $GEN_OUT_DIR/../../NewLegoBase && git pull && ~/bin/sbt embedAll && ~/bin/sbt compile"

eval "cd $GEN_OUT_DIR"

eval "./run_opt_diff.sh Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10:Q11:Q12:Q13:Q14:Q15:Q16:Q17:Q18:Q19:Q20:Q21:Q22 1 5"

eval "./run_opt_diff.sh Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10:Q11:Q12:Q13:Q14:Q15:Q16:Q17:Q18:Q19:Q20:Q21:Q22 8 5"


