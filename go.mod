module github.com/pingcap/tidb

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/StackExchange/wmi v0.0.0-20180725035823-b12b22c5341f // indirect
	github.com/blacktear23/go-proxyprotocol v0.0.0-20171102103907-62e368e1c470
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/bbolt v1.3.0 // indirect
	github.com/coreos/etcd v3.3.11+incompatible // indirect
	github.com/coreos/go-systemd v0.0.0-20181031085051-9002847aa142 // indirect
	github.com/cznic/mathutil v0.0.0-20181021201202-eba54fb065b7
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eknkc/amber v0.0.0-20171010120322-cdade1c07385 // indirect
	github.com/etcd-io/gofail v0.0.0-20180808172546-51ce9a71510a // indirect
	github.com/go-ole/go-ole v1.2.1 // indirect
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/gogo/protobuf v1.2.0 // indirect
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c
	github.com/google/uuid v1.1.0 // indirect
	github.com/gorilla/mux v1.7.0
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.7.0 // indirect
	github.com/klauspost/cpuid v0.0.0-20170728055534-ae7887de9fa5
	github.com/kr/pretty v0.1.0 // indirect
	github.com/montanaflynn/stats v0.5.0 // indirect
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.0
	github.com/pingcap/gofail v0.0.0-20181217135706-6a951c1e42c3
	github.com/pingcap/goleveldb v0.0.0-20171020122428-b9ff6c35079e
	github.com/pingcap/kvproto v0.0.0-20190110035000-d4fe6b336379
	github.com/pingcap/parser v0.0.0-20190121074657-4b899f19591e
	github.com/pingcap/pd v2.1.0-rc.4+incompatible
	github.com/pingcap/tidb-tools v2.1.3-0.20190116051332-34c808eef588+incompatible
	github.com/pingcap/tipb v0.0.0-20181012112600-11e33c750323
	github.com/pkg/errors v0.8.1 // indirect
	github.com/prometheus/client_golang v0.9.2
	github.com/prometheus/client_model v0.0.0-20190115171406-56726106282f
	github.com/prometheus/common v0.2.0 // indirect
	github.com/prometheus/procfs v0.0.0-20190117184657-bf6a532e95b1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20170806203942-52369c62f446 // indirect
	github.com/shirou/gopsutil v2.18.10+incompatible
	github.com/shurcooL/httpfs v0.0.0-20181222201310-74dc9339e414 // indirect
	github.com/shurcooL/vfsgen v0.0.0-20181020040650-a97a25d856ca // indirect
	github.com/sirupsen/logrus v1.2.0
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/stretchr/testify v1.3.0 // indirect
	github.com/struCoder/pidusage v0.1.2
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/twinj/uuid v1.0.0
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v2.15.0+incompatible
	github.com/uber/jaeger-lib v1.5.0 // indirect
	github.com/ugorji/go/codec v0.0.0-20190126102652-8fd0f8d918c8 // indirect
	github.com/unrolled/render v0.0.0-20190117215946-449f39850074 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/etcd v0.0.0-20190117200926-fcc29894c2e9
	golang.org/x/crypto v0.0.0-20190123085648-057139ce5d2b // indirect
	golang.org/x/net v0.0.0-20190125091013-d26f9f9a57f3
	golang.org/x/sys v0.0.0-20190124100055-b90733256f2e // indirect
	golang.org/x/text v0.3.1-0.20180807135948-17ff2d5776d2
	golang.org/x/time v0.0.0-20181108054448-85acf8d2951c // indirect
	golang.org/x/tools v0.0.0-20190125232054-d66bd3c5d5a6 // indirect
	google.golang.org/genproto v0.0.0-20190128161407-8ac453e89fca // indirect
	google.golang.org/grpc v1.18.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/stretchr/testify.v1 v1.2.2 // indirect
	sourcegraph.com/sourcegraph/appdash v0.0.0-20180531100431-4c381bd170b4
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)
