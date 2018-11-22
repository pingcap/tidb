source _help.sh

# check gogo protobuf's existence and version
check_gogo_exist_and_version

cd proto
echo "generate go code..."
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=../go-tipb *.proto
cd ../go-tipb
sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
rm -f *.bak
goimports -w *.pb.go

