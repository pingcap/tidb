module github.com/pingcap/tidb/tests/readonlytest

go 1.16

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/stretchr/testify v1.7.0
	go.uber.org/goleak v1.1.12
)

replace github.com/pingcap/tidb => ../../

replace github.com/pingcap/tidb/parser => ../../parser
