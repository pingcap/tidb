module github.com/pingcap/tidb

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/HdrHistogram/hdrhistogram-go v0.9.0 // indirect
	github.com/Jeffail/gabs/v2 v2.5.1
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/cheggaaa/pb/v3 v3.0.4 // indirect
	github.com/codahale/hdrhistogram v0.9.0 // indirect
	github.com/coocood/freecache v1.1.1
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37
	github.com/dgraph-io/ristretto v0.0.1
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/frankban/quicktest v1.11.1 // indirect
	github.com/fsouza/fake-gcs-server v1.17.0 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/go-yaml/yaml v2.1.0+incompatible
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/golang/snappy v0.0.2-0.20190904063534-ff6b7dc882cf
	github.com/google/btree v1.0.0
	github.com/google/pprof v0.0.0-20200407044318-7d83b28da2e9
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.7.4
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/iancoleman/strcase v0.0.0-20191112232945-16388991a334
	github.com/klauspost/compress v1.10.5 // indirect
	github.com/klauspost/cpuid v1.2.1
	github.com/kr/text v0.2.0 // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/ngaut/unistore v0.0.0-20201217024625-f53801ce8d4f
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pingcap/badger v1.5.1-0.20200908111422-2e78ee155d19
	github.com/pingcap/br v4.0.0-beta.2.0.20210115100158-7a7b4a421c0a+incompatible
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/fn v0.0.0-20200306044125-d5540d389059
	github.com/pingcap/goleveldb v0.0.0-20191226122134-f82aafb29989
	github.com/pingcap/kvproto v0.0.0-20210125053538-4fb47d0584f2
	github.com/pingcap/log v0.0.0-20201112100606-8f1e84a3abc8
	github.com/pingcap/parser v0.0.0-20210125075924-ffe0fda947cb
	github.com/pingcap/sysutil v0.0.0-20201130064824-f0c8aa6a6966
	github.com/pingcap/tidb-tools v4.0.9-0.20201127090955-2707c97b3853+incompatible
	github.com/pingcap/tipb v0.0.0-20201229060814-148bc717ce4c
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/shirou/gopsutil v2.20.6+incompatible
	github.com/sirupsen/logrus v1.6.0
	github.com/soheilhy/cmux v0.1.4
	github.com/stretchr/testify v1.6.1 // indirect
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tikv/pd v1.1.0-beta.0.20201125070607-d4b90eee0c70
	github.com/twmb/murmur3 v1.1.3
	github.com/uber-go/atomic v1.4.0
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	github.com/zhangjinpeng1987/raft v0.0.0-20200819064223-df31bb68a018 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/atomic v1.7.0
	go.uber.org/automaxprocs v1.2.0
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a // indirect
	golang.org/x/net v0.0.0-20200904194848-62affa334b73
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20200819171115-d785dc25833f
	golang.org/x/text v0.3.4
	golang.org/x/tools v0.0.0-20200820010801-b793a1359eac
	google.golang.org/grpc v1.27.1
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.3.0 // indirect
	honnef.co/go/tools v0.1.0 // indirect
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

go 1.13
