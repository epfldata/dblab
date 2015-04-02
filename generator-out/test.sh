#!/bin/bash
END=22
START=1

if [ $# -ne 2 ]; then
    echo "Invalid number of command line arguments."
    echo "USAGE: ./test.sh <DATA_FOLDER> <SF>"
    exit
fi

declare -a coptions=("+no-let" "+hm2set +no-let" "+hm2set +set2arr +no-let" "+hm2set +set2ll +no-let" "+hm2set +set2ll +cont-flat +no-let")
declare -a scalaoptions=("" "+hm2set" "+hm2set +set2arr" "+hm2set +set2ll" "+hm2set +set2ll +cont-flat")

echo "Testing C started"

for option in "${coptions[@]}"
do
	echo "*** Optimization arguments: "$option" ***"
	rm *.out *.c
	cd ..
	sbt "lego-compiler/run "$1" "$2" testsuite-c $option"
	cd generator-out
	make
	./run_c.sh "$2" "silent"
done

echo "Testing Scala started"

if [ "$SCALA_PATH" == "" ]; then
    echo "SCALA_PATH is not set! Set it using \"export SCALA_PATH=<path to the bin folder of Scala>\""
    exit
fi

for option in "${scalaoptions[@]}"
do
	echo "*** Optimization arguments: "$option" ***"
	cd ..
	sbt "lego-compiler/run "$1" "$2" testsuite-scala $option"
	cd generator-out
	./run_scala.sh "$1" "$2"
done
