#! /bin/bash
if [ ! -d "./bin/" ];then
   mkdir bin
else
   rm -r bin
   mkdir bin
fi
go build -race -o bin/tidb-server ./cmd/tidb-server
if [ $? -ne 0 ];then
   echo "build race tidb failed."
   exit 1
fi

echo "build race tidb success."
exit 0
