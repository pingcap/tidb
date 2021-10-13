module github.com/pingcap/tidb/tests/readonlytest

go 1.16

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/stretchr/testify v1.7.0
	go.uber.org/goleak v1.1.11
)

// update pongo2 version to v4.0.2, remove dependency on juju/errors, this module import by cockroachdb/pebble
replace github.com/flosch/pongo2 => github.com/flosch/pongo2/v4 v4.0.2

replace github.com/pingcap/tidb => ../../

replace github.com/pingcap/tidb/parser => ../../parser
