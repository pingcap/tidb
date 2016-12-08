#!/bin/bash
id=1
for ((i=0;i<1000;i++))
do
    go test -check.f=Subquery > ../log1/tmp.log.$id  2>&1
    if [ "$?" -ne "0" ];
    then
        echo $id
        break
    fi
    id=`expr $id + 1`
done
