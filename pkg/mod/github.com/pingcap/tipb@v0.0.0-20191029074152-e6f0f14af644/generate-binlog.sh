#!/usr/bin/env bash
source ./_help.sh

check_gogo_exist_and_version
cd proto/binlog

echo "generate binlog code..."
GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=../../go-binlog binlog.proto
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=plugins=grpc:../../go-binlog pump.proto
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=plugins=grpc:../../go-binlog cistern.proto
cd ../../go-binlog
sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
rm -f *.bak
goimports -w *.pb.go
