module github.com/pingcap/tidb

require (
<<<<<<< HEAD
	github.com/BurntSushi/toml v0.3.1
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7 // indirect
	github.com/beorn7/perks v0.0.0-20160229213445-3ac7bf7a47d1 // indirect
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20180202092358-40e2722dffea // indirect
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf // indirect
	github.com/cznic/mathutil v0.0.0-20160613104831-78ad7f262603
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/dustin/go-humanize v0.0.0-20180421182945-02af3965c54e // indirect
	github.com/eknkc/amber v0.0.0-20171010120322-cdade1c07385 // indirect
	github.com/fsnotify/fsnotify v1.4.7 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/gogo/protobuf v1.1.1
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20181024230925-c65c006176ff // indirect
	github.com/golang/protobuf v1.1.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/context v0.0.0-20160226214623-1ea25387ff6f // indirect
	github.com/gorilla/mux v0.0.0-20170228224354-599cba5e7b61
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v0.0.0-20171020063731-82921fcf811d
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20160910222444-6b7015e65d36
	github.com/grpc-ecosystem/grpc-gateway v1.4.1 // indirect
	github.com/hpcloud/tail v1.0.0 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/json-iterator/go v1.1.6 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
=======
	cloud.google.com/go/storage v1.28.1
	github.com/Azure/azure-sdk-for-go/sdk/azcore v0.20.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v0.12.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.2.0
	github.com/BurntSushi/toml v1.2.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Jeffail/gabs/v2 v2.5.1
	github.com/Masterminds/semver v1.5.0
	github.com/Shopify/sarama v1.29.0
	github.com/aliyun/alibaba-cloud-sdk-go v1.61.1581
	github.com/apache/skywalking-eyes v0.4.0
	github.com/ashanbrown/makezero v1.1.1
	github.com/aws/aws-sdk-go v1.44.48
	github.com/blacktear23/go-proxyprotocol v1.0.5
	github.com/carlmjohnson/flagext v0.21.0
	github.com/charithe/durationcheck v0.0.9
	github.com/cheggaaa/pb/v3 v3.0.8
	github.com/cheynewallace/tabby v1.1.1
	github.com/cloudfoundry/gosigar v1.3.6
	github.com/cockroachdb/errors v1.8.1
	github.com/cockroachdb/pebble v0.0.0-20210719141320-8c3bd06debb5
	github.com/coocood/freecache v1.2.1
	github.com/coreos/go-semver v0.3.0
	github.com/daixiang0/gci v0.9.1
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37
	github.com/dgraph-io/ristretto v0.1.1
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/docker/go-units v0.4.0
	github.com/emirpasic/gods v1.18.1
	github.com/fatanugraha/noloopclosure v0.1.1
	github.com/fatih/color v1.15.0
	github.com/fsouza/fake-gcs-server v1.44.0
	github.com/go-sql-driver/mysql v1.7.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/golangci/gofmt v0.0.0-20220901101216-f2edd75033f2
	github.com/golangci/golangci-lint v1.51.2
	github.com/golangci/gosec v0.0.0-20180901114220-8afd9cbb6cfb
	github.com/golangci/misspell v0.4.0
	github.com/golangci/prealloc v0.0.0-20180630174525-215b22d4de21
	github.com/google/btree v1.1.2
	github.com/google/pprof v0.0.0-20211122183932-1daafda22083
	github.com/google/uuid v1.3.0
	github.com/gordonklaus/ineffassign v0.0.0-20230107090616-13ace0543b28
	github.com/gorilla/mux v1.8.0
	github.com/gostaticanalysis/forcetypeassert v0.1.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/iancoleman/strcase v0.2.0
	github.com/jarcoal/httpmock v1.2.0
	github.com/jedib0t/go-pretty/v6 v6.2.2
	github.com/jingyugao/rowserrcheck v1.1.1
	github.com/joho/sqltocsv v0.0.0-20210428211105-a6d6801d59df
	github.com/kisielk/errcheck v1.6.3
	github.com/klauspost/compress v1.15.13
	github.com/kyoh86/exportloopref v0.1.11
	github.com/lestrrat-go/jwx/v2 v2.0.6
	github.com/mgechev/revive v1.3.1
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/nishanths/predeclared v0.2.2
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/badger v1.5.1-0.20230103063557-828f39b09b6d
	github.com/pingcap/errors v0.11.5-0.20221009092201-b66cddb77c32
	github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c
	github.com/pingcap/fn v0.0.0-20200306044125-d5540d389059
	github.com/pingcap/kvproto v0.0.0-20230317010544-b47a4830141f
	github.com/pingcap/log v1.1.1-0.20230317032135-a0d097d16e22
	github.com/pingcap/sysutil v0.0.0-20220114020952-ea68d2dbf5b4
	github.com/pingcap/tidb/parser v0.0.0-20211011031125-9b13dc409c5e
	github.com/pingcap/tipb v0.0.0-20230310043643-5362260ee6f7
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/client_model v0.3.0
	github.com/prometheus/common v0.39.0
	github.com/prometheus/prometheus v0.0.0-20190525122359-d20e84d0fb64
	github.com/sasha-s/go-deadlock v0.2.0
	github.com/shirou/gopsutil/v3 v3.23.1
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.6.1
	github.com/spf13/pflag v1.0.5
	github.com/spkg/bom v1.0.0
	github.com/stathat/consistent v1.0.0
	github.com/stretchr/testify v1.8.2
	github.com/tdakkota/asciicheck v0.1.1
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tikv/client-go/v2 v2.0.7-0.20230317032622-884a634378d4
	github.com/tikv/pd/client v0.0.0-20230321033841-af5b01913628
	github.com/timakin/bodyclose v0.0.0-20221125081123-e39cf3fc478e
	github.com/twmb/murmur3 v1.1.6
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/vbauerster/mpb/v7 v7.5.3
	github.com/wangjohn/quickselect v0.0.0-20161129230411-ed8402a42d5f
	github.com/xitongsys/parquet-go v1.5.5-0.20201110004701-b09c49d6d457
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/pkg/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	go.etcd.io/etcd/server/v3 v3.5.2
	go.etcd.io/etcd/tests/v3 v3.5.2
	go.opencensus.io v0.24.0
	go.uber.org/atomic v1.10.0
	go.uber.org/automaxprocs v1.4.0
	go.uber.org/goleak v1.2.1
	go.uber.org/multierr v1.9.0
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20221023144134-a1e5550cf13e
	golang.org/x/net v0.8.0
	golang.org/x/oauth2 v0.6.0
	golang.org/x/sync v0.1.0
	golang.org/x/sys v0.6.0
	golang.org/x/term v0.6.0
	golang.org/x/text v0.8.0
	golang.org/x/time v0.3.0
	golang.org/x/tools v0.7.0
	google.golang.org/api v0.106.0
	google.golang.org/grpc v1.52.3
	gopkg.in/yaml.v2 v2.4.0
	honnef.co/go/tools v0.4.3
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

require (
	cloud.google.com/go v0.105.0 // indirect
	cloud.google.com/go/compute v1.14.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.8.0 // indirect
	cloud.google.com/go/pubsub v1.28.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v0.8.1 // indirect
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.1.1 // indirect
	github.com/Masterminds/sprig/v3 v3.2.2 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/apache/thrift v0.13.1-0.20201008052519-daf620915714 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bmatcuk/doublestar/v2 v2.0.4 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chavacava/garif v0.0.0-20230227094218-b8c73b2037b8 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/coocood/bbloom v0.0.0-20190830030839-58deb6228d64 // indirect
	github.com/coocood/rtutil v0.0.0-20190304133409-c84515f646f2 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/elastic/gosigar v0.14.2 // indirect
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/felixge/httpsnoop v1.0.2 // indirect
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/go-kit/kit v0.9.0 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/licensecheck v0.3.1 // indirect
	github.com/google/renameio/v2 v2.0.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.1 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/gostaticanalysis/analysisutil v0.7.1 // indirect
	github.com/gostaticanalysis/comment v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hexops/gotextdiff v1.0.3 // indirect
	github.com/huandu/xstrings v1.3.1 // indirect
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid v1.3.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lestrrat-go/blackmagic v1.0.1 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/httprc v1.0.4 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.1 // indirect
>>>>>>> 942186f3051 (config,go.mod: support timeout for log writting (#42212))
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/montanaflynn/stats v0.0.0-20151014174947-eeaced052adb // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.1 // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/failpoint v0.0.0-20200603062251-b230c36c413c
	github.com/pingcap/gofail v0.0.0-20181217135706-6a951c1e42c3 // indirect
	github.com/pingcap/goleveldb v0.0.0-20171020084629-8d44bfdf1030
	github.com/pingcap/kvproto v0.0.0-20190826051950-fc8799546726
	github.com/pingcap/log v0.0.0-20190715063458-479153f07ebd
	github.com/pingcap/parser v2.1.20-0.20200616054907-4b5081dc242e+incompatible
	github.com/pingcap/pd v2.1.12+incompatible
	github.com/pingcap/tidb-tools v2.1.3-0.20190116051332-34c808eef588+incompatible
	github.com/pingcap/tipb v0.0.0-20200401051341-79a721ff4a15
	github.com/prometheus/client_golang v0.8.0
	github.com/prometheus/client_model v0.0.0-20171117100541-99fa1f4be8e5
	github.com/prometheus/common v0.0.0-20180426121432-d811d2e9bf89 // indirect
	github.com/prometheus/procfs v0.0.0-20180408092902-8b1c2da0d56d // indirect
	github.com/sirupsen/logrus v0.0.0-20170323161349-3bcb09397d6d
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/spaolacci/murmur3 v0.0.0-20150829172844-0d12bf811670
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/uber/jaeger-client-go v2.8.0+incompatible
	github.com/uber/jaeger-lib v1.1.0 // indirect
	github.com/unrolled/render v0.0.0-20171102162132-65450fb6b2d3 // indirect
	github.com/xiang90/probing v0.0.0-20160813154853-07dd2e8dfe18 // indirect
	go.etcd.io/bbolt v1.3.3 // indirect
	go.uber.org/atomic v1.3.2
	go.uber.org/zap v1.9.1
	golang.org/x/crypto v0.0.0-20180503215945-1f94bef427e3 // indirect
	golang.org/x/net v0.0.0-20180906233101-161cd47e91fd
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e // indirect
	golang.org/x/sys v0.0.0-20191010194322-b09406accb47 // indirect
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2 // indirect
	golang.org/x/tools v0.0.0-20181105230042-78dc5bac0cac
	google.golang.org/genproto v0.0.0-20180427144745-86e600f69ee4 // indirect
	google.golang.org/grpc v1.12.0
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
)

go 1.13
