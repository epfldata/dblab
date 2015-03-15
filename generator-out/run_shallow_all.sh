#!/bin/bash

GEN_OUT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
eval "cd $GEN_OUT_DIR"

#reading parameters from config file: DATA_FOLDER
CONFIG_FILE="run_config.cfg"
if [ ! -f "$GEN_OUT_DIR/$CONFIG_FILE" ]; then
    echo "Config file does not exist."
    echo "You can rename run_config.example.cfg to $CONFIG_FILE and change the parameters inside."
    exit 1
fi
source $CONFIG_FILE

#DATA_FOLDER points to the data directory on the workers
DATA_FOLDER="/data/home/dashti/data"

for SF in "${SF_ARR[@]}"
do
	echo "Started running shallow for SF=$SF"
	eval "./run_shallow.sh $DATA_FOLDER $SF"
	echo "Finished running shallow for SF=$SF"
done
