#!/bin/bash
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

# parameter 1: config file for sync_diff_inspector
# parameter 2: max check times

conf=$1
check_time=${2-10}

LOG=$DUMPLING_OUTPUT_DIR/sync_diff_inspector.log

# change output dir "./output" to "$DUMPLING_OUTPUT_DIR/output"
DUMPLING_OUTPUT_DIR_REGEX=$(echo "$DUMPLING_OUTPUT_DIR/output" | sed -e 's/\//\\\//g')
sed "s/.\/output/${DUMPLING_OUTPUT_DIR_REGEX}/g" $conf > $DUMPLING_OUTPUT_DIR/diff_config.toml
conf=$DUMPLING_OUTPUT_DIR/diff_config.toml

i=0
while [ $i -lt $check_time ]
do
    bin/sync_diff_inspector --config=$conf >> $LOG 2>&1
    ret=$?
    if [ "$ret" == 0 ]; then
        echo "check diff successfully"
        break
    fi
    ((i++))
    echo "check diff failed $i-th time, retry later"
    sleep 2
done

if [ $i -ge $check_time ]; then
    echo "check data failed, some data are different!!"
    # show \n and other blanks
    printf "$(cat $LOG)\n"
    exit 1
fi
cd $PWD
