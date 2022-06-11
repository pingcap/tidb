module graceshutdown

go 1.18

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/log v1.1.0
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/stretchr/testify v1.7.2-0.20220504104629-106ec21d14df
	go.uber.org/goleak v1.1.12
	go.uber.org/zap v1.21.0
)

require (
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/pingcap/tidb => ../../

replace github.com/pingcap/tidb/parser => ../../parser
