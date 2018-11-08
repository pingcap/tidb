module github.com/pingcap/tidb

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/beorn7/perks v0.0.0-20180321164747-3a771d992973 // indirect
	github.com/blacktear23/go-proxyprotocol v0.0.0-20171102103907-62e368e1c470
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/bbolt v1.3.1-coreos.6 // indirect
	github.com/coreos/etcd v3.3.10+incompatible
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20181031085051-9002847aa142 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/cznic/mathutil v0.0.0-20181021201202-eba54fb065b7
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eknkc/amber v0.0.0-20171010120322-cdade1c07385 // indirect
	github.com/etcd-io/gofail v0.0.0-20180808172546-51ce9a71510a
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/gogo/protobuf v1.1.1 // indirect
	github.com/golang/groupcache v0.0.0-20181024230925-c65c006176ff // indirect
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c
	github.com/gorilla/context v1.1.1 // indirect
	github.com/gorilla/mux v1.6.2
	github.com/gorilla/websocket v1.2.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.5.1 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/juju/errors v0.0.0-20181012004132-a4583d0a56ea // indirect
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/juju/testing v0.0.0-20180920084828-472a3e8b2073 // indirect
	github.com/klauspost/cpuid v0.0.0-20170728055534-ae7887de9fa5
	github.com/kr/pretty v0.1.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/montanaflynn/stats v0.0.0-20180911141734-db72e6cae808 // indirect
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/onsi/gomega v1.4.2 // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20171206051426-1c287c953996
	github.com/pingcap/errors v0.11.0
	github.com/pingcap/goleveldb v0.0.0-20171020122428-b9ff6c35079e
	github.com/pingcap/kvproto v0.0.0-20181105061835-1b5d69cd1d26
	github.com/pingcap/parser v0.0.0-20181108112017-a63108d7da4c
	github.com/pingcap/pd v2.1.0-rc.4+incompatible
	github.com/pingcap/tidb-tools v0.0.0-20181101090416-cfac1096162e
	github.com/pingcap/tipb v0.0.0-20181012112600-11e33c750323
	github.com/pkg/errors v0.8.0 // indirect
	github.com/prometheus/client_golang v0.9.0
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910
	github.com/prometheus/common v0.0.0-20181020173914-7e9e6cabbd39 // indirect
	github.com/prometheus/procfs v0.0.0-20181005140218-185b4288413d // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20170806203942-52369c62f446 // indirect
	github.com/sirupsen/logrus v1.2.0
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72
	github.com/tmc/grpc-websocket-proxy v0.0.0-20171017195756-830351dc03c6 // indirect
	github.com/twinj/uuid v1.0.0
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v2.15.0+incompatible
	github.com/uber/jaeger-lib v1.5.0 // indirect
	github.com/ugorji/go v1.1.1 // indirect
	github.com/unrolled/render v0.0.0-20180914162206-b9786414de4d // indirect
	github.com/xiang90/probing v0.0.0-20160813154853-07dd2e8dfe18 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.8.0 // indirect
	golang.org/x/crypto v0.0.0-20181030102418-4d3f4d9ffa16 // indirect
	golang.org/x/net v0.0.0-20181029044818-c44066c5c816
	golang.org/x/sys v0.0.0-20181031143558-9b800f95dbbc // indirect
	golang.org/x/text v0.3.0
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2 // indirect
	google.golang.org/genproto v0.0.0-20181029155118-b69ba1387ce2 // indirect
	google.golang.org/grpc v1.16.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/stretchr/testify.v1 v1.2.2 // indirect
)

replace github.com/pingcap/parser => github.com/tiancaiamao/parser v0.0.0-20181108133527-afafb63df286
