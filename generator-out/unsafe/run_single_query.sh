#!/bin/bash
## Path to scala binaries
# SCALA_PATH=
## The location of the UnsafeArray.jar
# LOCATION_UNSAFE_ARRAY=
QUERY=$3
ITERATIONS=$4
if [ $# -ne 4 ]; then
    echo "Invalid number of command line arguments."
    echo "USAGE: ./run_scala.sh <DATA_FOLDER> <SF> <query> <iterations>"
    exit
fi

rm -rf bin
mkdir bin
CPATH=$HOME/.ivy2/local/lego-core/lego-core_2.11/0.1-SNAPSHOT/jars/lego-core_2.11.jar:$HOME/.ivy2/local/ch.epfl.data/pardis-library_2.11/0.1-SNAPSHOT/jars/pardis-library_2.11.jar:$HOME/.ivy2/local/ch.epfl.data/pardis-core_2.11/0.1-SNAPSHOT/jars/pardis-core_2.11.jar:$LOCATION_UNSAFE_ARRAY
    mkdir bin/Q$QUERY
    echo "Compiling Q"$QUERY
    $SCALA_PATH/scalac "Q"${QUERY}".scala" -classpath $CPATH -d bin/Q$QUERY
cd ..
echo "Now running them"
for (( i = 1; i <= $ITERATIONS; i+=1 )); do
    ## Uncomment this line if using a 64-bit JVM
    #env JAVA_OPTS="-XX:-UseCompressedOops" $SCALA_PATH/scala -classpath generator-out/bin/Q$QUERY:$CPATH ch.epfl.data.legobase.Q$QUERY $1 $2 "Q"$QUERY
    ## Uncomment this line if using a 32-bit JVM
    $SCALA_PATH/scala -classpath generator-out/bin/Q$QUERY:$CPATH ch.epfl.data.legobase.Q$QUERY $1 $2 "Q"$QUERY
done