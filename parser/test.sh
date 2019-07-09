{
	  mv go.mod1 go.mod
	  mv go.sum1 go.sum
	  GO111MODULE=on go test -race ./... &&
	  GO111MODULE=on go test -covermode=set -coverprofile=coverage.txt -coverpkg=./... ./...
} || {
	  mv go.mod go.mod1
	  mv go.sum go.sum1
}

mv go.mod go.mod1
mv go.sum go.sum1
