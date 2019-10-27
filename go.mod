module github.com/pingcap/tidb

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8
	github.com/dgryski/go-farm v0.0.0-20190104051053-3adb47b1fb0f
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/gogo/protobuf v1.2.0
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.6.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4
	github.com/klauspost/cpuid v0.0.0-20170728055534-ae7887de9fa5
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/failpoint v0.0.0-20190512135322-30cc7431d99c
	github.com/pingcap/goleveldb v0.0.0-20171020122428-b9ff6c35079e
	github.com/pingcap/kvproto v0.0.0-20190910074005-0e61b6f435c1
	github.com/pingcap/log v0.0.0-20191012051959-b742a5d432e9
	github.com/pingcap/parser v0.0.0-20191021083151-7c64f78a5100
	github.com/pingcap/pd v1.1.0-beta.0.20190923032047-5c648dc365e0
	github.com/pingcap/tidb-tools v2.1.3-0.20190321065848-1e8b48f5c168+incompatible
	github.com/pingcap/tipb v0.0.0-20191015023537-709b39e7f8bb
	github.com/prometheus/client_golang v0.9.0
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910
	github.com/qiniu/api.v7/v7 v7.3.1
	github.com/shirou/gopsutil v2.18.10+incompatible
	github.com/sirupsen/logrus v1.3.0
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72
	github.com/struCoder/pidusage v0.1.2
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/uber/jaeger-client-go v2.15.0+incompatible
	go.uber.org/atomic v1.4.0
	go.uber.org/multierr v1.2.0 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20190911031432-227b76d455e7 // indirect
	golang.org/x/net v0.0.0-20190909003024-a7b16738d86b
	golang.org/x/sys v0.0.0-20190910064555-bbd175535a8b
	golang.org/x/text v0.3.2
	golang.org/x/tools v0.0.0-20190911022129-16c5e0f7d110
	google.golang.org/genproto v0.0.0-20190905072037-92dd089d5514 // indirect
	google.golang.org/grpc v1.23.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	sourcegraph.com/sourcegraph/appdash v0.0.0-20180531100431-4c381bd170b4
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

go 1.13

replace github.com/pingcap/parser => /Users/user/pingcap/parser
