#!/bin/bash


echo "start"

for ((i = 0 ; i <= 1000 ; i++))
do
	echo "$i"
	go clean -testcache
#	go test -v -vet=off -p 5 -timeout 20m -race github.com/pingcap/tidb/executor > c
	go test ./ddl -check.f TestAddPrimaryKeyRollback1 > a
#	go test -v ./executor/... > c
#	make test  > c
# make test  > c
	if [ $? -ne 0 ]; then
		exit 1
	fi
done
