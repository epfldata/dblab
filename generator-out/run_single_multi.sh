#!/bin/bash
QUERY=1
NUM=5
if [ $# -ne 2 ]; then
    echo "Invalid number of command line arguments."
    echo "USAGE: ./run_scala.sh <DATA_FOLDER> <SF>"
    exit
fi

rm -rf bin
mkdir bin
CPATH=$HOME/.ivy2/local/lego-core/lego-core_2.11/0.1-SNAPSHOT/jars/lego-core_2.11.jar:$HOME/.ivy2/local/ch.epfl.data/pardis-library_2.11/0.1-SNAPSHOT/jars/pardis-library_2.11.jar:$HOME/.ivy2/local/ch.epfl.data/pardis-core_2.11/0.1-SNAPSHOT/jars/pardis-core_2.11.jar
	mkdir bin/Q$QUERY
	echo "Compiling Q"$QUERY
	$SCALA_PATH/scalac "Q"${QUERY}".scala" -classpath $CPATH -d bin/Q$QUERY
cd ..
echo "Now running them"
for (( i = 1; i <= $NUM; i+=1 )); do
    $SCALA_PATH/scala -J-Xmx2g -J-Xms2g -classpath generator-out/bin/Q$QUERY:$CPATH ch.epfl.data.legobase.Q$QUERY $1 $2 "Q"$QUERY
done
cd - 1> /dev/null 2> /dev/null
echo "Done!"
