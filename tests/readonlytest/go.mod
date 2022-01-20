module github.com/pingcap/tidb/tests/readonlytest

go 1.16

require (
	github.com/go-sql-driver/mysql v1.6.0
<<<<<<< HEAD
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
=======
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/stretchr/testify v1.7.0
	go.uber.org/goleak v1.1.12
>>>>>>> 1a146fabd... variables: add constraints on tidb_super_read_only when tidb_restricted_read_only is turned on (#31746)
)
