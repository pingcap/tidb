module github.com/pingcap/tests/globalkilltest

go 1.16

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pingcap/errors v0.11.5-0.20210513014640-40f9a1999b3b
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20210512015243-d19fbe541bf9
	go.uber.org/goleak v1.1.11 // indirect
	go.uber.org/zap v1.19.1
	google.golang.org/grpc v1.40.0
)

replace github.com/pingcap/tidb => ../../

replace github.com/pingcap/tidb/parser => ../../parser

replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
