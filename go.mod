module github.com/pingcap/tidb

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/StackExchange/wmi v0.0.0-20180725035823-b12b22c5341f // indirect
	github.com/beorn7/perks v1.0.0 // indirect
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/bbolt v1.3.0 // indirect
	github.com/coreos/etcd v3.3.12+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/go-ole/go-ole v1.2.1 // indirect
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/groupcache v0.0.0-20190129154638-5b532d6fd5ef // indirect
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1 // indirect
	github.com/gorilla/mux v1.7.1
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4
	github.com/grpc-ecosystem/grpc-gateway v1.8.5 // indirect
	github.com/klauspost/cpuid v0.0.0-20170728055534-ae7887de9fa5
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/montanaflynn/stats v0.5.0 // indirect
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errcode v0.3.0 // indirect
	github.com/pingcap/errors v0.11.1
	github.com/pingcap/failpoint v0.0.0-20190422094118-d8535965f59b
	github.com/pingcap/goleveldb v0.0.0-20171020122428-b9ff6c35079e
	github.com/pingcap/kvproto v0.0.0-20190418102559-894ab99cd91e
	github.com/pingcap/log v0.0.0-20190307075452-bd41d9273596
	github.com/pingcap/parser v0.0.0-20190424024541-e2cdb851bce2
	github.com/pingcap/pd v0.0.0-20190424024702-bd1e2496a669
	github.com/pingcap/tidb-tools v2.1.3-0.20190321065848-1e8b48f5c168+incompatible
	github.com/pingcap/tipb v0.0.0-20190107072121-abbec73437b7
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90
	github.com/prometheus/common v0.3.0 // indirect
	github.com/prometheus/procfs v0.0.0-20190416084830-8368d24ba045 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20170806203942-52369c62f446 // indirect
	github.com/shirou/gopsutil v2.18.10+incompatible
	github.com/shurcooL/httpfs v0.0.0-20171119174359-809beceb2371 // indirect
	github.com/shurcooL/vfsgen v0.0.0-20181020040650-a97a25d856ca // indirect
	github.com/sirupsen/logrus v1.4.1
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/struCoder/pidusage v0.1.2
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/twinj/uuid v1.0.0
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v2.15.0+incompatible
	github.com/uber/jaeger-lib v1.5.0 // indirect
	github.com/unrolled/render v1.0.0 // indirect
	go.uber.org/zap v1.9.1
	golang.org/x/crypto v0.0.0-20190422183909-d864b10871cd // indirect
	golang.org/x/net v0.0.0-20190424024845-afe8014c977f
	golang.org/x/sys v0.0.0-20190422165155-953cdadca894 // indirect
	golang.org/x/text v0.3.1-0.20180807135948-17ff2d5776d2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	golang.org/x/tools v0.0.0-20190424031103-cb2dda6eabdf
	google.golang.org/genproto v0.0.0-20190418145605-e7d98fc518a7 // indirect
	google.golang.org/grpc v1.20.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	sourcegraph.com/sourcegraph/appdash v0.0.0-20180531100431-4c381bd170b4
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)
