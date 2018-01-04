# tipb
TiDB protobuf files

# Usage

### install protobuf compiler.

+ Can be downloaded at https://github.com/google/protobuf

### Write your own protocol file in proto folder.

### Make sure the gogo protobuf and protoc-gen-gofast is installed and checked out to v0.5

+ `go get -u github.com/gogo/protobuf/protoc-gen-gofast`
+ `cd $GOPATH/src/github.com/gogo/protobuf`
+ `git checkout v0.5`
+ `rm $GOPATH/bin/protoc-gen-gofast`
+ `go get github.com/gogo/protobuf/protoc-gen-gofast`

### Generate go and rust code.

We generate all go code in pkg folder and rust in src folder.

+ `make`

### Update the dependent projects.
