cd proto

echo "generate go code..."
GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf
find ./Mysqlx -name "*.proto" -exec protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=.. {} \;
cd ..
find ./Mysqlx -name "*.pb.go" -exec sed -i.bak -E 's/import _ \"gogoproto\"//g' {} \;
find ./Mysqlx -name "*.pb.go" -exec sed -i.bak -E 's/import fmt \"fmt\"//g' {} \;
find ./Mysqlx -name "*.pb.go" -exec sed -i.bak -E 's/Mysqlx\//github.com\/pingcap\/tipb\/go-mysqlx\//g' {} \;
find ./Mysqlx -name "*.bak" -exec rm -f {} +
find ./Mysqlx -name "*.pb.go" -exec goimports -w {} \;
rm -rf go-mysqlx && mv Mysqlx go-mysqlx
