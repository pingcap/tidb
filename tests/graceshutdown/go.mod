module graceshutdown

go 1.15

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/juju/errors v0.0.0-20200330140219-3fe23663418f
	github.com/juju/testing v0.0.0-20210324180055-18c50b0c2098 // indirect
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/stretchr/testify v1.7.0
	go.uber.org/goleak v1.1.11
	go.uber.org/zap v1.19.0
)

replace github.com/pingcap/tidb => ../../
