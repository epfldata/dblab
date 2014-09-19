#!/bin/bash
NUM=22
CPATH=$HOME/.ivy2/local/lego-core/lego-core_2.11/0.1-SNAPSHOT/jars/lego-core_2.11.jar:$HOME/.ivy2/local/ch.epfl.data/pardis-library_2.11/0.1-SNAPSHOT/jars/pardis-library_2.11.jar:$HOME/.ivy2/local/ch.epfl.data/autolifter_2.11/0.1-SNAPSHOT/jars/autolifter_2.11.jar
for (( i = 1; i <= $NUM; i+=1 )); do
	echo "Compiling Q"$i
	$SCALA_PATH/scalac "Q"${i}".scala" -classpath $CPATH -d bin
done
cd ..
echo "Now running them"
for (( i = 1; i <= $NUM; i+=1 )); do
	$SCALA_PATH/scala -J-Xmx12g -J-Xms12g -classpath generator-out/bin:$CPATH ch.epfl.data.legobase.Q$i $1 $2 "Q"$i
done
cd - 1> /dev/null 2> /dev/null
echo "Done!"