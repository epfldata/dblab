#!/bin/bash

SCALA_PATH="/data/home/dashti/scala-2.11.5/bin"
HOME="/data/home/dashti"

GEN_OUT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
eval "cd $GEN_OUT_DIR"

END=22
START=1

if [ $# -ne 2 ]; then
    echo "Invalid number of command line arguments."
    echo "USAGE: ./run_shallow.sh <DATA_FOLDER> <SF>"
    exit
fi

rm -rf bin
mkdir bin
CPATH=$HOME/ivy2/local/lego-core/lego-core_2.11/0.1-SNAPSHOT/jars/lego-core_2.11.jar:$HOME/ivy2/local/ch.epfl.data/sc-pardis-library_2.11/0.1-SNAPSHOT/jars/sc-pardis-library_2.11.jar:$HOME/ivy2/local/ch.epfl.data/sc-pardis-core_2.11/0.1-SNAPSHOT/jars/sc-pardis-core_2.11.jar
cd ..
echo "Now running them"
for (( i = $START; i <= $END; i+=1 )); do
	
    $SCALA_PATH/scala -J-Xmx100g -J-Xms100g -classpath $CPATH ch.epfl.data.legobase.MiniDB $1 $2 "Q"$i
done
cd - 1> /dev/null 2> /dev/null
echo "Done!"
