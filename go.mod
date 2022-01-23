module github.com/pingcap/tidb

go 1.16

require (
	cloud.google.com/go v0.54.0 // indirect
	cloud.google.com/go/storage v1.6.0
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
	github.com/fsouza/fake-gcs-server v1.19.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.3
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/google/pprof v0.0.0-20210609004039-a478d1d731e9
	github.com/google/uuid v1.1.2
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/iancoleman/strcase v0.0.0-20191112232945-16388991a334
	github.com/jedib0t/go-pretty/v6 v6.2.2
	github.com/joho/sqltocsv v0.0.0-20210428211105-a6d6801d59df
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/badger v1.5.1-0.20200908111422-2e78ee155d19
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/fn v0.0.0-20200306044125-d5540d389059
	github.com/pingcap/kvproto v0.0.0-20210806074406-317f69fb54b4
	github.com/pingcap/log v0.0.0-20210818144256-6455d4a4c6f9
	github.com/pingcap/parser v0.0.0-20210831085004-b5390aa83f65
	github.com/pingcap/sysutil v0.0.0-20210730114356-fcd8a63f68c5
	github.com/pingcap/tidb-tools v5.0.3+incompatible
	github.com/pingcap/tipb v0.0.0-20210708040514-0f154bb0dc0f
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/shirou/gopsutil v3.21.2+incompatible
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tikv/client-go/v2 v2.0.0-alpha.0.20210831090540-391fcd842dc8
	github.com/tikv/pd v1.1.0-beta.0.20210818112400-0c5667766690
	github.com/twmb/murmur3 v1.1.3
	github.com/uber-go/atomic v1.4.0
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/wangjohn/quickselect v0.0.0-20161129230411-ed8402a42d5f
	github.com/xitongsys/parquet-go v1.5.5-0.20201110004701-b09c49d6d457
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.opencensus.io v0.22.5 // indirect
	go.uber.org/atomic v1.9.0
	go.uber.org/automaxprocs v1.4.0
	go.uber.org/goleak v1.1.10
	go.uber.org/multierr v1.7.0
	go.uber.org/zap v1.19.0
	golang.org/x/net v0.0.0-20210503060351-7fd8e65b6420
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c
	golang.org/x/text v0.3.7
	golang.org/x/tools v0.1.5
	google.golang.org/api v0.22.0
	google.golang.org/grpc v1.29.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v2 v2.4.0
	modernc.org/mathutil v1.2.2
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

// FIXME the official repo has some bug makes br_gcs test failed. https://github.com/googleapis/google-cloud-go/pull/3509
// replace cloud.google.com/go/storage => github.com/3pointer/google-cloud-go/storage v1.6.1-0.20210108125931-b59bfa0720b2
