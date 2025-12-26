module github.com/pingcap/tidb

go 1.25.5

require (
	cloud.google.com/go/kms v1.15.8
	cloud.google.com/go/storage v1.39.1
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.14.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.7.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.0.0
	github.com/BurntSushi/toml v1.6.0
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Masterminds/semver v1.5.0
	github.com/YangKeao/go-mysql-driver v0.0.0-20240627104025-dd5589458cfa
	github.com/aliyun/alibaba-cloud-sdk-go v1.61.1581
	github.com/apache/arrow-go/v18 v18.0.0
	github.com/apache/skywalking-eyes v0.4.0
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2
	github.com/ashanbrown/forbidigo/v2 v2.1.0
	github.com/ashanbrown/makezero v1.2.0
	github.com/aws/aws-sdk-go-v2 v1.38.1
	github.com/aws/aws-sdk-go-v2/config v1.29.17
	github.com/aws/aws-sdk-go-v2/credentials v1.17.70
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.84
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.45.3
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.232.0
	github.com/aws/aws-sdk-go-v2/service/kms v1.41.2
	github.com/aws/aws-sdk-go-v2/service/s3 v1.87.1
	github.com/aws/aws-sdk-go-v2/service/sts v1.34.0
	github.com/aws/smithy-go v1.22.5
	github.com/bazelbuild/buildtools v0.0.0-20230926111657-7d855c59baeb
	github.com/bazelbuild/rules_go v0.42.1-0.20231101215950-df20c987afcb
	github.com/bits-and-blooms/bitset v1.14.3
	github.com/blacktear23/go-proxyprotocol v1.0.6
	github.com/butuzov/mirror v1.3.0
	github.com/carlmjohnson/flagext v0.21.0
	github.com/charithe/durationcheck v0.0.10
	github.com/cheggaaa/pb/v3 v3.0.8
	github.com/cheynewallace/tabby v1.1.1
	github.com/ckaznocha/intrange v0.3.1
	github.com/cloudfoundry/gosigar v1.3.6
	github.com/cockroachdb/pebble v1.1.4-0.20250120151818-5dd133a1e6fb
	github.com/coocood/freecache v1.2.1
	github.com/coreos/go-semver v0.3.1
	github.com/daixiang0/gci v0.13.7
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37
	github.com/dgraph-io/ristretto v0.1.1
	github.com/dgryski/go-farm v0.0.0-20240924180020-3414d57e47da
	github.com/docker/go-units v0.5.0
	github.com/dolthub/swiss v0.2.1
	github.com/emirpasic/gods v1.18.1
	github.com/fatih/color v1.18.0
	github.com/felixge/fgprof v0.9.3
	github.com/fsouza/fake-gcs-server v1.44.0
	github.com/go-ldap/ldap/v3 v3.4.4
	github.com/go-resty/resty/v2 v2.11.0
	github.com/go-sql-driver/mysql v1.7.1
	github.com/gobwas/glob v0.2.3
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v0.0.4
	github.com/golangci/gofmt v0.0.0-20250106114630-d62b90e6713d
	github.com/golangci/golangci-lint/v2 v2.4.0
	github.com/golangci/gosec v0.0.0-20180901114220-8afd9cbb6cfb
	github.com/golangci/misspell v0.7.0
	github.com/golangci/prealloc v0.0.0-20180630174525-215b22d4de21
	github.com/google/btree v1.1.2
	github.com/google/pprof v0.0.0-20250903194437-c28834ac2320
	github.com/google/skylark v0.0.0-20181101142754-a5f7082aabed
	github.com/google/uuid v1.6.0
	github.com/gordonklaus/ineffassign v0.1.0
	github.com/gorilla/mux v1.8.1
	github.com/gostaticanalysis/forcetypeassert v0.2.0
	github.com/grafana/pyroscope-go v1.2.7
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/hashicorp/go-version v1.8.0
	github.com/jedib0t/go-pretty/v6 v6.2.2
	github.com/jellydator/ttlcache/v3 v3.0.1
	github.com/jfcg/sorty/v2 v2.1.0
	github.com/jingyugao/rowserrcheck v1.1.1
	github.com/johannesboyne/gofakes3 v0.0.0-20230506070712-04da935ef877
	github.com/joho/sqltocsv v0.0.0-20210428211105-a6d6801d59df
	github.com/karamaru-alpha/copyloopvar v1.2.1
	github.com/kisielk/errcheck v1.9.0
	github.com/klauspost/compress v1.18.0
	github.com/ks3sdklib/aws-sdk-go v1.2.9
	github.com/ldez/exptostd v0.4.5
	github.com/lestrrat-go/jwx/v2 v2.0.21
	github.com/mgechev/revive v1.13.0
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef
	github.com/nishanths/predeclared v0.2.2
	github.com/openai/openai-go v0.1.0-alpha.59
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/badger v1.5.1-0.20241015064302-38533b6cbf8d
	github.com/pingcap/errors v0.11.5-0.20250523034308-74f78ae071ee
	github.com/pingcap/failpoint v0.0.0-20240528011301-b51a646c7c86
	github.com/pingcap/fn v1.0.0
	github.com/pingcap/kvproto v0.0.0-20251202064041-b6fd818387cd
	github.com/pingcap/log v1.1.1-0.20250917021125-19901e015dc9
	github.com/pingcap/metering_sdk v0.0.0-20251110022152-dac449ac5389
	github.com/pingcap/sysutil v1.0.1-0.20240311050922-ae81ee01f3a5
	github.com/pingcap/tidb/pkg/parser v0.0.0-20211011031125-9b13dc409c5e
	github.com/pingcap/tipb v0.0.0-20250928030846-9fd33ded6f2c
	github.com/prometheus/client_golang v1.23.0
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.65.0
	github.com/prometheus/prometheus v0.50.1
	github.com/qri-io/jsonschema v0.2.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/sasha-s/go-deadlock v0.3.6
	github.com/shirou/gopsutil/v3 v3.24.5
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/afero v1.15.0
	github.com/spf13/cobra v1.9.1
	github.com/spf13/pflag v1.0.7
	github.com/spkg/bom v1.0.0
	github.com/stathat/consistent v1.0.0
	github.com/stretchr/testify v1.10.0
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/tikv/client-go/v2 v2.0.8-0.20251215121014-01758810e841
	github.com/tikv/pd/client v0.0.0-20251216162211-a0bf0e9fc204
	github.com/timakin/bodyclose v0.0.0-20241222091800-1db5c5ca4d67
	github.com/twmb/murmur3 v1.1.6
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/vbauerster/mpb/v7 v7.5.3
	github.com/wangjohn/quickselect v0.0.0-20161129230411-ed8402a42d5f
	github.com/zyedidia/generic v1.2.1
	go.etcd.io/etcd/api/v3 v3.5.15
	go.etcd.io/etcd/client/pkg/v3 v3.5.15
	go.etcd.io/etcd/client/v3 v3.5.15
	go.etcd.io/etcd/server/v3 v3.5.15
	go.etcd.io/etcd/tests/v3 v3.5.12
	go.opencensus.io v0.24.0
	go.uber.org/atomic v1.11.0
	go.uber.org/automaxprocs v1.6.0
	go.uber.org/goleak v1.3.0
	go.uber.org/mock v0.5.2
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.1
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0
	golang.org/x/net v0.48.0
	golang.org/x/oauth2 v0.33.0
	golang.org/x/sync v0.19.0
	golang.org/x/sys v0.39.0
	golang.org/x/term v0.38.0
	golang.org/x/text v0.32.0
	golang.org/x/time v0.14.0
	golang.org/x/tools v0.40.0
	google.golang.org/api v0.170.0
	google.golang.org/grpc v1.63.2
	gopkg.in/yaml.v2 v2.4.0
	gorm.io/driver/mysql v1.5.7
	gorm.io/gorm v1.25.12
	honnef.co/go/tools v0.6.1
	k8s.io/api v0.29.11
	modernc.org/mathutil v1.7.1
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

require (
	codeberg.org/chavacava/garif v0.2.0 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/alibabacloud-go/alibabacloud-gateway-spi v0.0.5 // indirect
	github.com/alibabacloud-go/darabonba-openapi/v2 v2.0.11 // indirect
	github.com/alibabacloud-go/debug v1.0.1 // indirect
	github.com/alibabacloud-go/endpoint-util v1.1.0 // indirect
	github.com/alibabacloud-go/openapi-util v0.1.1 // indirect
	github.com/alibabacloud-go/sts-20150401/v2 v2.0.4 // indirect
	github.com/alibabacloud-go/tea v1.3.11 // indirect
	github.com/alibabacloud-go/tea-utils/v2 v2.0.7 // indirect
	github.com/alibabacloud-go/tea-xml v1.1.3 // indirect
	github.com/aliyun/alibabacloud-oss-go-sdk-v2 v1.2.3 // indirect
	github.com/aliyun/credentials-go v1.4.7 // indirect
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/aws/aws-sdk-go v1.55.7 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.0 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.32 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.3 // indirect
	github.com/clbanning/mxj/v2 v2.7.0 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240606204812-0bbfbd93a7ce // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/ldez/grignotin v0.10.0 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/qri-io/jsonpointer v0.1.1 // indirect
	github.com/segmentio/fasthash v1.0.3 // indirect
	github.com/tidwall/gjson v1.14.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	github.com/tjfoc/gmsm v1.4.1 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/telemetry v0.0.0-20251203150158-8fff8a5912fc // indirect
	golang.org/x/tools/godoc v0.1.0-deprecated // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
)

require (
	cloud.google.com/go v0.112.2 // indirect
	cloud.google.com/go/compute/metadata v0.3.0 // indirect
	cloud.google.com/go/iam v1.1.7 // indirect
	cloud.google.com/go/pubsub v1.37.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.10.0 // indirect
	github.com/Azure/go-ntlmssp v0.0.0-20221128193559-754e69321358 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.2.2 // indirect
	github.com/DataDog/zstd v1.5.5 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.3.1 // indirect
	github.com/Masterminds/sprig/v3 v3.2.2 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/apache/thrift v0.21.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bmatcuk/doublestar/v2 v2.0.4 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/coocood/bbloom v0.0.0-20190830030839-58deb6228d64 // indirect
	github.com/coocood/rtutil v0.0.0-20190304133409-c84515f646f2 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/dolthub/maphash v0.1.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fatih/structtag v1.2.0
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.4 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/goccy/go-json v0.10.4 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.2 // indirect
	github.com/golang/glog v1.2.4 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/licensecheck v0.3.1 // indirect
	github.com/google/renameio/v2 v2.0.0 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.3 // indirect
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/gorilla/websocket v1.5.1 // indirect
	github.com/gostaticanalysis/analysisutil v0.7.1 // indirect
	github.com/gostaticanalysis/comment v1.5.0 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.9 // indirect
	github.com/grafana/regexp v0.0.0-20221122212121-6b5c0a4cb7fd // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.19.1 // indirect
	github.com/hexops/gotextdiff v1.0.3 // indirect
	github.com/huandu/xstrings v1.3.1 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/influxdata/tdigest v0.0.1
	github.com/jfcg/sixb v1.3.8 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/cpuid v1.3.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lestrrat-go/blackmagic v1.0.2 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/httprc v1.0.5 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20230326075908-cb1d2100619a // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nbutton23/zxcvbn-go v0.0.0-20210217022336-fa2cb2858354 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/petermattis/goid v0.0.0-20250813065127-a731cc31b4fe // indirect
	github.com/pingcap/goleveldb v0.0.0-20191226122134-f82aafb29989 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pkg/errors v0.9.1
	github.com/pkg/xattr v0.4.9 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/prometheus/procfs v0.19.2 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shabbyrobe/gocovmerge v0.0.0-20190829150210-3e036491d500 // indirect
	github.com/shoenig/go-m1cpu v0.1.7 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/shurcooL/httpfs v0.0.0-20230704072500-f1e31cf0ba5c // indirect
	github.com/shurcooL/vfsgen v0.0.0-20181202132449-6a9ea43bcacd // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/tiancaiamao/gp v0.0.0-20221230034425-4025bc8a4d4a
	github.com/tklauser/go-sysconf v0.3.15 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20220101234140-673ab2c3ae75 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/xiang90/probing v0.0.0-20221125231312-a49e3df8f510 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.etcd.io/bbolt v1.3.10 // indirect
	go.etcd.io/etcd/client/v2 v2.305.15 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.15 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.15 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.22.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.22.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/sdk v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	go.opentelemetry.io/proto/otlp v1.1.0 // indirect
	golang.org/x/crypto v0.46.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20250620022241-b7579e27df2b // indirect
	golang.org/x/mod v0.31.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250425173222-7b384671a197 // indirect
	google.golang.org/protobuf v1.36.10
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apimachinery v0.29.11 // indirect
	k8s.io/klog/v2 v2.120.1 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
	stathat.com/c/consistent v1.0.0 // indirect
)

replace (
	// Downgrade grpc to v1.63.2, as well as other related modules.
	github.com/apache/arrow-go/v18 => github.com/joechenrh/arrow-go/v18 v18.0.0-20250911101656-62c34c9a3b82
	github.com/go-ldap/ldap/v3 => github.com/YangKeao/ldap/v3 v3.4.5-0.20230421065457-369a3bab1117
	github.com/pingcap/tidb/pkg/parser => ./pkg/parser

	// TODO: `sourcegraph.com/sourcegraph/appdash` has been archived, and the original host has been removed.
	// Please remove these dependencies.
	sourcegraph.com/sourcegraph/appdash => github.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0
	sourcegraph.com/sourcegraph/appdash-data => github.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)
