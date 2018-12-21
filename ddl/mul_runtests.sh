#!/bin/bash

cnt=$1
out=$2

declare -i i=1
while ((i<=$cnt))
do
  GO111MODULE=on go test 2>&1|tee test.log
  str=`tail -n 1 test.log | awk '{print $1}'`
  if [ $str != $out ]
  then
	  echo out: $out
  	  echo $i
	  let i=$cnt
  fi
  let ++i
done

