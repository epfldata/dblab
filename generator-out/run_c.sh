#!/bin/bash

SF=1
VERBOSE=false

TMPFILE="tmpfile.txt"
if [ "`uname`" == "Linux" ]; then
    TMPFILE=`mktemp`
else
    #assume darwin
    TMPFILE=`mktemp -q /tmp/tmp.XXXXXX`
fi

for f in `ls *.out`
do
    QUERY=`echo $f | cut -d'.' -f1`
    echo "Running query $QUERY..."
    ./$f > $TMPFILE
    # check results
    NUMROWS=`cat $TMPFILE | grep rows | cut -d'(' -f2 | cut -d' ' -f1`
	NUMROWS=`echo "$NUMROWS + 1" | bc`
	RESULTS=`cat $TMPFILE | grep -v Generated | tail -n $NUMROWS`
	CORR_RESULTS=`cat ./../results/$QUERY.result_sf$SF` 
	if [ "$RESULTS" != "$CORR_RESULTS" ]; then
        if [ $VERBOSE == true ]; then
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
    echo $TMPFIL
    # clenaup
    rm $TMPFILE
    #sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 
done
