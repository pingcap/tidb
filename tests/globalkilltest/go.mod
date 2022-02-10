module github.com/pingcap/tests/globalkilltest

go 1.16

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20210512015243-d19fbe541bf9 // indirect
	go.etcd.io/etcd/client/v3 v3.5.2
	go.uber.org/zap v1.19.1
	google.golang.org/grpc v1.40.0
)

// fix potential security issue(CVE-2020-26160) introduced by indirect dependency.
replace github.com/dgrijalva/jwt-go => github.com/form3tech-oss/jwt-go v3.2.6-0.20210809144907-32ab6a8243d7+incompatible

replace github.com/pingcap/tidb => ../../

replace github.com/pingcap/tidb/parser => ../../parser

// replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
