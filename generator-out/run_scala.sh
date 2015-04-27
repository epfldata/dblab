#!/bin/bash
END=2
START=1

if [ $# -ne 2 ]; then
    echo "Invalid number of command line arguments."
    echo "USAGE: ./run_scala.sh <DATA_FOLDER> <SF>"
    exit
fi

rm -rf bin
mkdir bin
CPATH=$HOME/.ivy2/local/lego-core/lego-core_2.11/0.1-SNAPSHOT/jars/lego-core_2.11.jar:$HOME/.ivy2/local/ch.epfl.data/sc-pardis-library_2.11/0.1-SNAPSHOT/jars/sc-pardis-library_2.11.jar:$HOME/.ivy2/local/ch.epfl.data/sc-pardis-core_2.11/0.1-SNAPSHOT/jars/sc-pardis-core_2.11.jar
for (( i = $START; i <= $END; i+=1 )); do
	mkdir bin/Q$i
	echo "Compiling Q"$i
	$SCALA_PATH/scalac "Q"${i}".scala" -classpath $CPATH -d bin/Q$i
done
cd ..
echo "Now running them"
for (( i = $START; i <= $END; i+=1 )); do
    $SCALA_PATH/scala -J-Xmx2g -J-Xms2g -classpath generator-out/bin/Q$i:$CPATH ch.epfl.data.legobase.Q$i $1 $2 "Q"$i
done
cd - 1> /dev/null 2> /dev/null
echo "Done!"
