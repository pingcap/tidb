module github.com/pingcap/tidb/v3

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/StackExchange/wmi v0.0.0-20181212234831-e0a55b97c705 // indirect
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/coreos/bbolt v1.3.2 // indirect
	github.com/coreos/etcd v3.3.10+incompatible
	github.com/coreos/go-systemd v0.0.0-20190318101727-c7c1946145b6 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/golang/groupcache v0.0.0-20190129154638-5b532d6fd5ef // indirect
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c
	github.com/gorilla/mux v1.6.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.8.5 // indirect
	github.com/juju/errors v0.0.0-20190207033735-e65537c515d7 // indirect
	github.com/juju/loggo v0.0.0-20190212223446-d976af380377 // indirect
	github.com/juju/testing v0.0.0-20180920084828-472a3e8b2073 // indirect
	github.com/klauspost/cpuid v0.0.0-20170728055534-ae7887de9fa5
	github.com/montanaflynn/stats v0.5.0 // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.1
	github.com/pingcap/gofail v0.0.0-20181217135706-6a951c1e42c3
	github.com/pingcap/goleveldb v0.0.0-20171020122428-b9ff6c35079e
	github.com/pingcap/kvproto v0.0.0-20190215154024-7f2fc73ef562
	github.com/pingcap/log v0.0.0-20190307075452-bd41d9273596
	github.com/pingcap/parser v0.0.0-20190312024907-3f6280b08c8b
	github.com/pingcap/pd v2.1.0-rc.4+incompatible
	github.com/pingcap/tidb v2.0.11+incompatible // indirect
	github.com/pingcap/tidb-tools v2.1.3+incompatible
	github.com/pingcap/tipb v0.0.0-20190107072121-abbec73437b7
	github.com/prometheus/client_golang v0.9.1
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910
	github.com/prometheus/common v0.2.0 // indirect
	github.com/prometheus/procfs v0.0.0-20190315082738-e56f2e22fc76 // indirect
	github.com/shirou/gopsutil v2.18.10+incompatible
	github.com/shurcooL/httpfs v0.0.0-20181222201310-74dc9339e414 // indirect
	github.com/shurcooL/vfsgen v0.0.0-20181202132449-6a9ea43bcacd // indirect
	github.com/sirupsen/logrus v1.3.0
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72
	github.com/struCoder/pidusage v0.1.2
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/twinj/uuid v1.0.0
	github.com/uber/jaeger-client-go v2.15.0+incompatible
	github.com/uber/jaeger-lib v1.5.0
	github.com/ugorji/go/codec v0.0.0-20190316192920-e2bddce071ad // indirect
	github.com/unrolled/render v1.0.0 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.2 // indirect
	go.uber.org/zap v1.9.1
	golang.org/x/net v0.0.0-20190108225652-1e06a53dbb7e
	golang.org/x/text v0.3.1-0.20180807135948-17ff2d5776d2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	golang.org/x/tools v0.0.0-20190130214255-bb1329dc71a0
	google.golang.org/grpc v1.19.0
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	sourcegraph.com/sourcegraph/appdash v0.0.0-20180531100431-4c381bd170b4
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

replace github.com/pingcap/parser => ../parser
