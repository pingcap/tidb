module graceshutdown

go 1.15

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/stretchr/testify v1.7.0
	go.uber.org/goleak v1.1.11
	go.uber.org/zap v1.19.1
)

replace github.com/pingcap/tidb => ../../

replace github.com/pingcap/tidb/parser => ../../parser
