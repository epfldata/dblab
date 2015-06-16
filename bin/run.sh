#!/bin/bash
#make;
RESULT="`./${1}.out | head -n -1`"
CHECKFILE="`cat ./../results/${1}.result_sf0.1`"
if [ "${RESULT}" != "${CHECKFILE}" ]; then
	echo "Result for ${1} not OK..."
	echo ${RESULT}
	echo ""
	echo ${CHECKFILE}
else 
	echo "Result for ${1} OK!"
fi
