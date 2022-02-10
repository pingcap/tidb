module github.com/pingcap/tidb

go 1.16

require (
	cloud.google.com/go/storage v1.16.1
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v0.12.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.2.0
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/HdrHistogram/hdrhistogram-go v1.1.0 // indirect
	github.com/Jeffail/gabs/v2 v2.5.1
	github.com/aws/aws-sdk-go v1.35.3
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/carlmjohnson/flagext v0.21.0
	github.com/cheggaaa/pb/v3 v3.0.8
	github.com/cheynewallace/tabby v1.1.1
	github.com/cockroachdb/pebble v0.0.0-20210719141320-8c3bd06debb5
	github.com/coocood/freecache v1.1.1
	github.com/coreos/go-semver v0.3.0
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37
	github.com/dgraph-io/ristretto v0.0.1
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/docker/go-units v0.4.0
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/fsouza/fake-gcs-server v1.19.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.3
	github.com/google/btree v1.0.1
	github.com/google/pprof v0.0.0-20211122183932-1daafda22083
	github.com/google/uuid v1.1.2
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/iancoleman/strcase v0.0.0-20191112232945-16388991a334
	github.com/jedib0t/go-pretty/v6 v6.2.2
	github.com/joho/sqltocsv v0.0.0-20210428211105-a6d6801d59df
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/badger v1.5.1-0.20210831093107-2f6cb8008145
	github.com/pingcap/check v0.0.0-20211026125417-57bd13f7b5f0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00
	github.com/pingcap/fn v0.0.0-20200306044125-d5540d389059
	github.com/pingcap/kvproto v0.0.0-20211224055123-d1a140660c39
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/sysutil v0.0.0-20220114020952-ea68d2dbf5b4
	github.com/pingcap/tidb-tools v5.2.2-0.20211019062242-37a8bef2fa17+incompatible
	github.com/pingcap/tidb/parser v0.0.0-20211011031125-9b13dc409c5e
	github.com/pingcap/tipb v0.0.0-20220107024056-3b91949a18a7
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.26.0
	github.com/shirou/gopsutil/v3 v3.21.12
	github.com/shurcooL/httpfs v0.0.0-20190707220628-8d4bc4ba7749 // indirect
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tikv/client-go/v2 v2.0.0-rc.0.20220107040026-d22815099720
	github.com/tikv/pd v1.1.0-beta.0.20220207063535-9268bed87199
	github.com/twmb/murmur3 v1.1.3
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/wangjohn/quickselect v0.0.0-20161129230411-ed8402a42d5f
	github.com/xitongsys/parquet-go v1.5.5-0.20201110004701-b09c49d6d457
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/pkg/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	go.etcd.io/etcd/server/v3 v3.5.2
	go.etcd.io/etcd/tests/v3 v3.5.2
	go.uber.org/atomic v1.9.0
	go.uber.org/automaxprocs v1.4.0
	go.uber.org/goleak v1.1.12
	go.uber.org/multierr v1.7.0
	go.uber.org/zap v1.19.1
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f
	golang.org/x/oauth2 v0.0.0-20210805134026-6f1e6394065a
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	golang.org/x/tools v0.1.8
	google.golang.org/api v0.54.0
	google.golang.org/grpc v1.40.0
	gopkg.in/yaml.v2 v2.4.0
	modernc.org/mathutil v1.4.1
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

// cloud.google.com/go/storage will upgrade grpc to v1.40.0
// we need keep the replacement until go.etcd.io supports the higher version of grpc.
// replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

replace github.com/tikv/pd => github.com/killzoner/pd v1.1.0-beta.0.20220210154558-a3baac06ab3a

replace github.com/tikv/client-go/v2 => github.com/killzoner/client-go/v2 v2.0.0-rc.0.20220210125733-0014b6b3951c

replace github.com/pingcap/tidb-tools => github.com/killzoner/tidb-tools v5.4.1-0.20220210151319-6bc4e51f77cb+incompatible

replace github.com/pingcap/tidb/parser => ./parser

// fix potential security issue(CVE-2020-26160) introduced by indirect dependency.
replace github.com/dgrijalva/jwt-go => github.com/form3tech-oss/jwt-go v3.2.6-0.20210809144907-32ab6a8243d7+incompatible
