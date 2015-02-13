#!/bin/bash

SF=8
NUMRUNS=1
VERBOSE=true

#rm *.out
#make

if [ $# -ne 0 ]; then
    SF=$1
    echo "SF set to "$SF"!"
    if [ $# -eq 2 ]; then
        if [ "$2" == "silent" ]; then
            VERBOSE=false
        fi
    fi
fi

TMPFILE="tmpfile.txt"
if [ "`uname`" == "Linux" ]; then
    TMPFILE=`mktemp`
else
    #assume darwin
    TMPFILE=`mktemp -q /tmp/tmp.XXXXXX`
fi

for f in `ls -1v *.out`
do
    QUERY=`echo $f | cut -d'.' -f1`
    echo "Running query $QUERY..."
    ./$f 1> $TMPFILE 
    # check results
	RESULTS=`cat $TMPFILE | grep -v "Generated code run in"`
    CORR_RESULTS_1=`cat ./../results/$QUERY.result_sf$SF`
    CORR_RESULTS=${CORR_RESULTS_1}
    for (( i = 1; i < $NUMRUNS; i+=1 ))
    do
        CORR_RESULTS=${CORR_RESULTS}$'\n'${CORR_RESULTS_1}
    done
	if [ "$RESULTS" != "$CORR_RESULTS" ]; then
        if [ "$VERBOSE" == "true" ]; then
		    echo "Invalid results for query $QUERY."
	    	echo -e "\n Execution result: \n"
    		echo "$RESULTS"
	    	echo -e "\n Correct results: \n"
	    	echo "$CORR_RESULTS"
	    	echo -e "\n Diff is: \n"
	    	diff -h $TMPFILE ./../results/$QUERY.result_sf$SF
	    fi	
        echo -e "\n[ FAILED ]...\n"
	else 
		echo -e "\n Query $QUERY result: [ OK ] \n"
	fi
    # clenaup
    rm $TMPFILE
    #sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 
done
