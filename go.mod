module github.com/pingcap/tidb

require (
	cloud.google.com/go v0.51.0 // indirect
	github.com/BurntSushi/toml v0.3.1
	github.com/Jeffail/gabs/v2 v2.5.1
	github.com/aws/aws-sdk-go v1.30.24 // indirect
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/cheggaaa/pb/v3 v3.0.4 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37
	github.com/dgraph-io/ristretto v0.0.2
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/fsouza/fake-gcs-server v1.17.0 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/golang/snappy v0.0.1
	github.com/google/btree v1.0.0
	github.com/google/pprof v0.0.0-20200407044318-7d83b28da2e9
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/grpc-gateway v1.14.3 // indirect
	github.com/iancoleman/strcase v0.0.0-20191112232945-16388991a334
	github.com/klauspost/cpuid v1.2.1
	github.com/kr/pretty v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pingcap/br v4.0.11-0.20210119023619-139df44843ab+incompatible
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/fn v0.0.0-20200306044125-d5540d389059
<<<<<<< HEAD
	github.com/pingcap/goleveldb v0.0.0-20191226122134-f82aafb29989
	github.com/pingcap/kvproto v0.0.0-20210308075244-560097d1309b
	github.com/pingcap/log v0.0.0-20210625125904-98ed8e2eb1c7
	github.com/pingcap/parser v0.0.0-20210421190550-451a84cf120a
	github.com/pingcap/sysutil v0.0.0-20210730114356-fcd8a63f68c5
	github.com/pingcap/tidb-tools v4.0.9-0.20201127090955-2707c97b3853+incompatible
	github.com/pingcap/tipb v0.0.0-20211105090418-71142a4d40e3
=======
	github.com/pingcap/kvproto v0.0.0-20211224055123-d1a140660c39
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/sysutil v0.0.0-20220114020952-ea68d2dbf5b4
	github.com/pingcap/tidb-tools v5.2.2-0.20211019062242-37a8bef2fa17+incompatible
	github.com/pingcap/tidb/parser v0.0.0-20211011031125-9b13dc409c5e
	github.com/pingcap/tipb v0.0.0-20220110031732-29e23c62eeac
>>>>>>> f949e01e0... planner, expression: pushdown AggFuncMode to coprocessor (#31392)
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/shirou/gopsutil v3.21.2+incompatible
	github.com/sirupsen/logrus v1.6.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spaolacci/murmur3 v1.1.0
	github.com/syndtr/goleveldb v1.0.1-0.20190625010220-02440ea7a285 // indirect
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tikv/pd v0.0.0-20210105112549-e5be7fd38659
	github.com/uber-go/atomic v1.4.0
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/atomic v1.7.0
	go.uber.org/automaxprocs v1.2.0
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20201110031124-69a78807bb2b
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20210217105451-b926d437f341
	golang.org/x/text v0.3.5
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	golang.org/x/tools v0.0.0-20201125231158-b5590deeca9b
	google.golang.org/api v0.15.1 // indirect
	google.golang.org/genproto v0.0.0-20200108215221-bd8f9a0ef82f // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.3.0 // indirect
	honnef.co/go/tools v0.0.1-2020.1.6 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

go 1.13
