module github.com/pingcap/tidb

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7 // indirect
	github.com/beorn7/perks v0.0.0-20160229213445-3ac7bf7a47d1 // indirect
	github.com/blacktear23/go-proxyprotocol v0.0.0-20171102103907-62e368e1c470
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20160425231609-f8ad88b59a58 // indirect
	github.com/coreos/bbolt v1.3.1-coreos.6 // indirect
	github.com/coreos/etcd v3.2.18+incompatible
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20180202092358-40e2722dffea // indirect
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf // indirect
	github.com/cznic/mathutil v0.0.0-20160613104831-78ad7f262603
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/dustin/go-humanize v0.0.0-20180421182945-02af3965c54e // indirect
	github.com/eknkc/amber v0.0.0-20171010120322-cdade1c07385 // indirect
	github.com/etcd-io/gofail v0.0.0-20180808172546-51ce9a71510a // indirect
	github.com/fsnotify/fsnotify v1.4.7 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/gogo/protobuf v1.1.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20181024230925-c65c006176ff // indirect
	github.com/golang/protobuf v1.1.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/google/btree v0.0.0-20161217183710-316fb6d3f031
	github.com/gorilla/context v0.0.0-20160226214623-1ea25387ff6f // indirect
	github.com/gorilla/mux v0.0.0-20170228224354-599cba5e7b61
	github.com/grpc-ecosystem/go-grpc-middleware v0.0.0-20171020063731-82921fcf811d
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20160910222444-6b7015e65d36
	github.com/grpc-ecosystem/grpc-gateway v1.4.1 // indirect
	github.com/hpcloud/tail v1.0.0 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/montanaflynn/stats v0.0.0-20151014174947-eeaced052adb // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.1 // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20171206051426-1c287c953996
	github.com/pingcap/errors v0.11.0
	github.com/pingcap/gofail v0.0.0-20181217135706-6a951c1e42c3
	github.com/pingcap/goleveldb v0.0.0-20171020084629-8d44bfdf1030
	github.com/pingcap/kvproto v0.0.0-20181109035735-8e3f33ac4929
	github.com/pingcap/parser v0.0.0-20190221074811-3d110205ce12
	github.com/pingcap/pd v2.1.0-rc.4+incompatible
	github.com/pingcap/tidb-tools v2.1.3-0.20190116051332-34c808eef588+incompatible
	github.com/pingcap/tipb v0.0.0-20180910045846-371b48b15d93
	github.com/pkg/errors v0.8.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v0.8.0
	github.com/prometheus/client_model v0.0.0-20171117100541-99fa1f4be8e5
	github.com/prometheus/common v0.0.0-20180426121432-d811d2e9bf89 // indirect
	github.com/prometheus/procfs v0.0.0-20180408092902-8b1c2da0d56d // indirect
	github.com/sirupsen/logrus v0.0.0-20170323161349-3bcb09397d6d
	github.com/spaolacci/murmur3 v0.0.0-20150829172844-0d12bf811670
	github.com/stretchr/testify v1.2.2 // indirect
	github.com/twinj/uuid v0.0.0-20150629100731-70cac2bcd273
	github.com/uber/jaeger-client-go v2.8.0+incompatible
	github.com/uber/jaeger-lib v1.1.0 // indirect
	github.com/ugorji/go v1.1.1 // indirect
	github.com/unrolled/render v0.0.0-20171102162132-65450fb6b2d3 // indirect
	github.com/xiang90/probing v0.0.0-20160813154853-07dd2e8dfe18 // indirect
	golang.org/x/crypto v0.0.0-20180503215945-1f94bef427e3 // indirect
	golang.org/x/net v0.0.0-20180906233101-161cd47e91fd
	golang.org/x/sync v0.0.0-20180314180146-1d60e4601c6f // indirect
	golang.org/x/sys v0.0.0-20180909124046-d0be0721c37e // indirect
	golang.org/x/text v0.3.0
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2 // indirect
	golang.org/x/tools v0.0.0-20181105230042-78dc5bac0cac
	google.golang.org/genproto v0.0.0-20180427144745-86e600f69ee4 // indirect
	google.golang.org/grpc v1.12.0
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0-20161104145732-dd45e6a67c53
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v2 v2.2.1 // indirect
)
