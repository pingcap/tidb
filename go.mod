module github.com/pingcap/tidb

go 1.18

require (
	cloud.google.com/go/storage v1.21.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v0.12.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.2.0
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Jeffail/gabs/v2 v2.5.1
	github.com/Shopify/sarama v1.29.0
	github.com/aws/aws-sdk-go v1.35.3
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/carlmjohnson/flagext v0.21.0
	github.com/cheggaaa/pb/v3 v3.0.8
	github.com/cheynewallace/tabby v1.1.1
	github.com/cockroachdb/pebble v0.0.0-20210719141320-8c3bd06debb5
	github.com/coocood/freecache v1.2.1
	github.com/coreos/go-semver v0.3.0
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37
	github.com/dgraph-io/ristretto v0.1.1-0.20220403145359-8e850b710d6d
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/docker/go-units v0.4.0
	github.com/fsouza/fake-gcs-server v1.19.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/btree v1.0.1
	github.com/google/pprof v0.0.0-20211122183932-1daafda22083
	github.com/google/uuid v1.1.2
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/iancoleman/strcase v0.2.0
	github.com/jedib0t/go-pretty/v6 v6.2.2
	github.com/joho/sqltocsv v0.0.0-20210428211105-a6d6801d59df
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/badger v1.5.1-0.20220314162537-ab58fbf40580
	github.com/pingcap/check v0.0.0-20211026125417-57bd13f7b5f0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20220303073211-00fea37feb66
	github.com/pingcap/fn v0.0.0-20200306044125-d5540d389059
	github.com/pingcap/kvproto v0.0.0-20220328072018-6e75c12dbd73
	github.com/pingcap/log v0.0.0-20211215031037-e024ba4eb0ee
	github.com/pingcap/sysutil v0.0.0-20220114020952-ea68d2dbf5b4
	github.com/pingcap/tidb/parser v0.0.0-20211011031125-9b13dc409c5e
	github.com/pingcap/tipb v0.0.0-20220314125451-bfb5c2c55188
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.32.1
	github.com/shirou/gopsutil/v3 v3.21.12
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546 // indirect
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.4.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tikv/client-go/v2 v2.0.1-0.20220406091203-f73ec0e675f4
	github.com/tikv/pd/client v0.0.0-20220307081149-841fa61e9710
	github.com/twmb/murmur3 v1.1.3
	github.com/uber/jaeger-client-go v2.22.1+incompatible
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
	go.uber.org/multierr v1.8.0
	go.uber.org/zap v1.21.0
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd
	golang.org/x/oauth2 v0.0.0-20211104180415-d3ed0bb246c8
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220408201424-a24fb2fb8a0f
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20220224211638-0e9765cccd65
	golang.org/x/tools v0.1.8
	google.golang.org/api v0.69.0
	google.golang.org/grpc v1.44.0
	gopkg.in/yaml.v2 v2.4.0
	modernc.org/mathutil v1.4.1
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/klauspost/cpuid v1.3.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/shurcooL/httpfs v0.0.0-20190707220628-8d4bc4ba7749 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292 // indirect
)

replace github.com/pingcap/tidb/parser => ./parser

// fix potential security issue(CVE-2020-26160) introduced by indirect dependency.
replace github.com/dgrijalva/jwt-go => github.com/form3tech-oss/jwt-go v3.2.6-0.20210809144907-32ab6a8243d7+incompatible

// fix date race in the testify. it can be remove after merging https://github.com/stretchr/testify/pull/1165
replace github.com/stretchr/testify => github.com/hawkingrei/testify v1.7.1-0.20220318075534-088488aa27f2
