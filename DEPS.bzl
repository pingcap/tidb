load("@bazel_gazelle//:deps.bzl", "go_repository")

def go_deps():
    # NOTE: We ensure that we pin to these specific dependencies by calling
    # this function FIRST, before calls to pull in dependencies for
    # third-party libraries (e.g. rules_go, gazelle, etc.)
    go_repository(
        name = "cc_mvdan_gofumpt",
        build_file_proto_mode = "disable_global",
        importpath = "mvdan.cc/gofumpt",
        sha256 = "7a05a751cc4b8eed6ca0b3f17720f21005ae14136472fc0a358864b559f36249",
        strip_prefix = "mvdan.cc/gofumpt@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/mvdan.cc/gofumpt/cc_mvdan_gofumpt-v0.6.0.zip",
            "http://ats.apps.svc/gomod/mvdan.cc/gofumpt/cc_mvdan_gofumpt-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/mvdan.cc/gofumpt/cc_mvdan_gofumpt-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/mvdan.cc/gofumpt/cc_mvdan_gofumpt-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "cc_mvdan_interfacer",
        build_file_proto_mode = "disable_global",
        importpath = "mvdan.cc/interfacer",
        sha256 = "5d9b8763a76321403a154a4172b61b356a18d9389e9fcadd11c0df9562069445",
        strip_prefix = "mvdan.cc/interfacer@v0.0.0-20180901003855-c20040233aed",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/mvdan.cc/interfacer/cc_mvdan_interfacer-v0.0.0-20180901003855-c20040233aed.zip",
            "http://ats.apps.svc/gomod/mvdan.cc/interfacer/cc_mvdan_interfacer-v0.0.0-20180901003855-c20040233aed.zip",
            "https://cache.hawkingrei.com/gomod/mvdan.cc/interfacer/cc_mvdan_interfacer-v0.0.0-20180901003855-c20040233aed.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/mvdan.cc/interfacer/cc_mvdan_interfacer-v0.0.0-20180901003855-c20040233aed.zip",
        ],
    )
    go_repository(
        name = "cc_mvdan_lint",
        build_file_proto_mode = "disable_global",
        importpath = "mvdan.cc/lint",
        sha256 = "2fe25817456b3b78355b6946de6bc20f057ec5b996977f868172c6127cf66905",
        strip_prefix = "mvdan.cc/lint@v0.0.0-20170908181259-adc824a0674b",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/mvdan.cc/lint/cc_mvdan_lint-v0.0.0-20170908181259-adc824a0674b.zip",
            "http://ats.apps.svc/gomod/mvdan.cc/lint/cc_mvdan_lint-v0.0.0-20170908181259-adc824a0674b.zip",
            "https://cache.hawkingrei.com/gomod/mvdan.cc/lint/cc_mvdan_lint-v0.0.0-20170908181259-adc824a0674b.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/mvdan.cc/lint/cc_mvdan_lint-v0.0.0-20170908181259-adc824a0674b.zip",
        ],
    )
    go_repository(
        name = "cc_mvdan_unparam",
        build_file_proto_mode = "disable_global",
        importpath = "mvdan.cc/unparam",
        sha256 = "e2d0554cad489ddd9a5e06425139ddb223447007682b11fc6fe75fb98d194f38",
        strip_prefix = "mvdan.cc/unparam@v0.0.0-20240104100049-c549a3470d14",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/mvdan.cc/unparam/cc_mvdan_unparam-v0.0.0-20240104100049-c549a3470d14.zip",
            "http://ats.apps.svc/gomod/mvdan.cc/unparam/cc_mvdan_unparam-v0.0.0-20240104100049-c549a3470d14.zip",
            "https://cache.hawkingrei.com/gomod/mvdan.cc/unparam/cc_mvdan_unparam-v0.0.0-20240104100049-c549a3470d14.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/mvdan.cc/unparam/cc_mvdan_unparam-v0.0.0-20240104100049-c549a3470d14.zip",
        ],
    )
    go_repository(
        name = "co_honnef_go_tools",
        build_file_proto_mode = "disable_global",
        importpath = "honnef.co/go/tools",
        sha256 = "84b304edb7aa479c9aa7dcbbb0937f21f97c3c1790ee204eab35daf60ad245fa",
        strip_prefix = "honnef.co/go/tools@v0.4.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/honnef.co/go/tools/co_honnef_go_tools-v0.4.7.zip",
            "http://ats.apps.svc/gomod/honnef.co/go/tools/co_honnef_go_tools-v0.4.7.zip",
            "https://cache.hawkingrei.com/gomod/honnef.co/go/tools/co_honnef_go_tools-v0.4.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/honnef.co/go/tools/co_honnef_go_tools-v0.4.7.zip",
        ],
    )
    go_repository(
        name = "com_4d63_gocheckcompilerdirectives",
        build_file_proto_mode = "disable_global",
        importpath = "4d63.com/gocheckcompilerdirectives",
        sha256 = "5345b619a528329a0be9177c6f0232230d82038db7050b1dc712521046dfc04c",
        strip_prefix = "4d63.com/gocheckcompilerdirectives@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/4d63.com/gocheckcompilerdirectives/com_4d63_gocheckcompilerdirectives-v1.2.1.zip",
            "http://ats.apps.svc/gomod/4d63.com/gocheckcompilerdirectives/com_4d63_gocheckcompilerdirectives-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/4d63.com/gocheckcompilerdirectives/com_4d63_gocheckcompilerdirectives-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/4d63.com/gocheckcompilerdirectives/com_4d63_gocheckcompilerdirectives-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_4d63_gochecknoglobals",
        build_file_proto_mode = "disable_global",
        importpath = "4d63.com/gochecknoglobals",
        sha256 = "c2ef46bc1df05d593abe0f62a79aa4e4dce8caa69f4e6e7982e6ed165c80bb97",
        strip_prefix = "4d63.com/gochecknoglobals@v0.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/4d63.com/gochecknoglobals/com_4d63_gochecknoglobals-v0.2.1.zip",
            "http://ats.apps.svc/gomod/4d63.com/gochecknoglobals/com_4d63_gochecknoglobals-v0.2.1.zip",
            "https://cache.hawkingrei.com/gomod/4d63.com/gochecknoglobals/com_4d63_gochecknoglobals-v0.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/4d63.com/gochecknoglobals/com_4d63_gochecknoglobals-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_4meepo_tagalign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/4meepo/tagalign",
        sha256 = "7787f1327d989f71ad57d73d86a771a4e5376268991c29e4361da0a69ddecfdf",
        strip_prefix = "github.com/4meepo/tagalign@v1.3.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.3.3.zip",
            "http://ats.apps.svc/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.3.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.3.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.3.3.zip",
        ],
    )
    go_repository(
        name = "com_github_abirdcfly_dupword",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Abirdcfly/dupword",
        sha256 = "dc4f47aeb49f248e75680fa61f10573b5d39412c4ee29a854c0cc6a1e072d9e4",
        strip_prefix = "github.com/Abirdcfly/dupword@v0.0.13",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Abirdcfly/dupword/com_github_abirdcfly_dupword-v0.0.13.zip",
            "http://ats.apps.svc/gomod/github.com/Abirdcfly/dupword/com_github_abirdcfly_dupword-v0.0.13.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Abirdcfly/dupword/com_github_abirdcfly_dupword-v0.0.13.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Abirdcfly/dupword/com_github_abirdcfly_dupword-v0.0.13.zip",
        ],
    )
    go_repository(
        name = "com_github_acarl005_stripansi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/acarl005/stripansi",
        sha256 = "5169858a54f6f06f3089c45db233290fbaf1ebc2c9776649705b6cd9dc58a40c",
        strip_prefix = "github.com/acarl005/stripansi@v0.0.0-20180116102854-5a71ef0e047d",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/acarl005/stripansi/com_github_acarl005_stripansi-v0.0.0-20180116102854-5a71ef0e047d.zip",
            "http://ats.apps.svc/gomod/github.com/acarl005/stripansi/com_github_acarl005_stripansi-v0.0.0-20180116102854-5a71ef0e047d.zip",
            "https://cache.hawkingrei.com/gomod/github.com/acarl005/stripansi/com_github_acarl005_stripansi-v0.0.0-20180116102854-5a71ef0e047d.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/acarl005/stripansi/com_github_acarl005_stripansi-v0.0.0-20180116102854-5a71ef0e047d.zip",
        ],
    )
    go_repository(
        name = "com_github_ajg_form",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajg/form",
        sha256 = "b063b07639670ce9b6a0065b4dc35ef9e4cebc0c601be27f5494a3e6a87eb78b",
        strip_prefix = "github.com/ajg/form@v1.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ajg/form/com_github_ajg_form-v1.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/ajg/form/com_github_ajg_form-v1.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ajg/form/com_github_ajg_form-v1.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ajg/form/com_github_ajg_form-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_ajstarks_svgo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajstarks/svgo",
        sha256 = "cdf6900823539ab02e7f6e0edcdb4c12b3dcec97068a350e564ff622132ae7fc",
        strip_prefix = "github.com/ajstarks/svgo@v0.0.0-20180226025133-644b8db467af",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ajstarks/svgo/com_github_ajstarks_svgo-v0.0.0-20180226025133-644b8db467af.zip",
            "http://ats.apps.svc/gomod/github.com/ajstarks/svgo/com_github_ajstarks_svgo-v0.0.0-20180226025133-644b8db467af.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ajstarks/svgo/com_github_ajstarks_svgo-v0.0.0-20180226025133-644b8db467af.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ajstarks/svgo/com_github_ajstarks_svgo-v0.0.0-20180226025133-644b8db467af.zip",
        ],
    )
    go_repository(
        name = "com_github_alecthomas_go_check_sumtype",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/go-check-sumtype",
        sha256 = "efed0eaa770376e5f3b8efa4004d05cfbe564343032a5a9a1f04f5504cc084ea",
        strip_prefix = "github.com/alecthomas/go-check-sumtype@v0.1.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alecthomas/go-check-sumtype/com_github_alecthomas_go_check_sumtype-v0.1.4.zip",
            "http://ats.apps.svc/gomod/github.com/alecthomas/go-check-sumtype/com_github_alecthomas_go_check_sumtype-v0.1.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alecthomas/go-check-sumtype/com_github_alecthomas_go_check_sumtype-v0.1.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alecthomas/go-check-sumtype/com_github_alecthomas_go_check_sumtype-v0.1.4.zip",
        ],
    )
    go_repository(
        name = "com_github_alecthomas_kingpin_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/kingpin/v2",
        sha256 = "ef1ea6fead21e5fcc9e1532187888c8c7c4f3ebbdb00587ab67a19245206ca66",
        strip_prefix = "github.com/alecthomas/kingpin/v2@v2.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alecthomas/kingpin/v2/com_github_alecthomas_kingpin_v2-v2.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/alecthomas/kingpin/v2/com_github_alecthomas_kingpin_v2-v2.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alecthomas/kingpin/v2/com_github_alecthomas_kingpin_v2-v2.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alecthomas/kingpin/v2/com_github_alecthomas_kingpin_v2-v2.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_alecthomas_units",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/units",
        sha256 = "0aa254cfcd2f946563e0e9f7875edad87366b595fbe973eb6c01e9da99b35d68",
        strip_prefix = "github.com/alecthomas/units@v0.0.0-20231202071711-9a357b53e9c9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alecthomas/units/com_github_alecthomas_units-v0.0.0-20231202071711-9a357b53e9c9.zip",
            "http://ats.apps.svc/gomod/github.com/alecthomas/units/com_github_alecthomas_units-v0.0.0-20231202071711-9a357b53e9c9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alecthomas/units/com_github_alecthomas_units-v0.0.0-20231202071711-9a357b53e9c9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alecthomas/units/com_github_alecthomas_units-v0.0.0-20231202071711-9a357b53e9c9.zip",
        ],
    )
    go_repository(
        name = "com_github_alexbrainman_sspi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alexbrainman/sspi",
        sha256 = "f094ecfc4554a9ca70f0ade41747123f3161a15fb1a6112305b99731befc8648",
        strip_prefix = "github.com/alexbrainman/sspi@v0.0.0-20210105120005-909beea2cc74",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alexbrainman/sspi/com_github_alexbrainman_sspi-v0.0.0-20210105120005-909beea2cc74.zip",
            "http://ats.apps.svc/gomod/github.com/alexbrainman/sspi/com_github_alexbrainman_sspi-v0.0.0-20210105120005-909beea2cc74.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alexbrainman/sspi/com_github_alexbrainman_sspi-v0.0.0-20210105120005-909beea2cc74.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alexbrainman/sspi/com_github_alexbrainman_sspi-v0.0.0-20210105120005-909beea2cc74.zip",
        ],
    )
    go_repository(
        name = "com_github_alexkohler_nakedret_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alexkohler/nakedret/v2",
        sha256 = "f7d75939f4ad3a19372ed0733d77c078ec5d63f51b25475db2a63dee71bf51ec",
        strip_prefix = "github.com/alexkohler/nakedret/v2@v2.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alexkohler/nakedret/v2/com_github_alexkohler_nakedret_v2-v2.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/alexkohler/nakedret/v2/com_github_alexkohler_nakedret_v2-v2.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alexkohler/nakedret/v2/com_github_alexkohler_nakedret_v2-v2.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alexkohler/nakedret/v2/com_github_alexkohler_nakedret_v2-v2.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_alexkohler_prealloc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alexkohler/prealloc",
        sha256 = "3da3c6aebc2917ecd1322724060b6aa02f0fa83eb546e07809b94e1d687aeece",
        strip_prefix = "github.com/alexkohler/prealloc@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alexkohler/prealloc/com_github_alexkohler_prealloc-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/alexkohler/prealloc/com_github_alexkohler_prealloc-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alexkohler/prealloc/com_github_alexkohler_prealloc-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alexkohler/prealloc/com_github_alexkohler_prealloc-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_alingse_asasalint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alingse/asasalint",
        sha256 = "e808d5f9e1410fbb686189d9a074d0fe67763b0ff0829c7627f477f71c59783c",
        strip_prefix = "github.com/alingse/asasalint@v0.0.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alingse/asasalint/com_github_alingse_asasalint-v0.0.11.zip",
            "http://ats.apps.svc/gomod/github.com/alingse/asasalint/com_github_alingse_asasalint-v0.0.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alingse/asasalint/com_github_alingse_asasalint-v0.0.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alingse/asasalint/com_github_alingse_asasalint-v0.0.11.zip",
        ],
    )
    go_repository(
        name = "com_github_aliyun_alibaba_cloud_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aliyun/alibaba-cloud-sdk-go",
        sha256 = "21a5b01952452ecd963ba9f2c96ed4a5281341b1ee7b52b32e2562f9397e6961",
        strip_prefix = "github.com/aliyun/alibaba-cloud-sdk-go@v1.61.1581",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/aliyun/alibaba-cloud-sdk-go/com_github_aliyun_alibaba_cloud_sdk_go-v1.61.1581.zip",
            "http://ats.apps.svc/gomod/github.com/aliyun/alibaba-cloud-sdk-go/com_github_aliyun_alibaba_cloud_sdk_go-v1.61.1581.zip",
            "https://cache.hawkingrei.com/gomod/github.com/aliyun/alibaba-cloud-sdk-go/com_github_aliyun_alibaba_cloud_sdk_go-v1.61.1581.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/aliyun/alibaba-cloud-sdk-go/com_github_aliyun_alibaba_cloud_sdk_go-v1.61.1581.zip",
        ],
    )
    go_repository(
        name = "com_github_andreasbriese_bbloom",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/AndreasBriese/bbloom",
        sha256 = "6d7c1af06f8597fde1e86166f26416057392f1b0bdb84f2af555aa461282dd18",
        strip_prefix = "github.com/AndreasBriese/bbloom@v0.0.0-20190306092124-e2d15f34fcf9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/AndreasBriese/bbloom/com_github_andreasbriese_bbloom-v0.0.0-20190306092124-e2d15f34fcf9.zip",
            "http://ats.apps.svc/gomod/github.com/AndreasBriese/bbloom/com_github_andreasbriese_bbloom-v0.0.0-20190306092124-e2d15f34fcf9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/AndreasBriese/bbloom/com_github_andreasbriese_bbloom-v0.0.0-20190306092124-e2d15f34fcf9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/AndreasBriese/bbloom/com_github_andreasbriese_bbloom-v0.0.0-20190306092124-e2d15f34fcf9.zip",
        ],
    )
    go_repository(
        name = "com_github_antihax_optional",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/antihax/optional",
        sha256 = "15ab4d41bdbb72ee0ac63db616cdefc7671c79e13d0f73b58355a6a88219c97f",
        strip_prefix = "github.com/antihax/optional@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/antihax/optional/com_github_antihax_optional-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/antihax/optional/com_github_antihax_optional-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/antihax/optional/com_github_antihax_optional-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/antihax/optional/com_github_antihax_optional-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_antonboom_errname",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Antonboom/errname",
        sha256 = "b153fac1a78bbeb14a1e2898307bdbb683dd62bd0f3e7a2a99610b9865081438",
        strip_prefix = "github.com/Antonboom/errname@v0.1.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Antonboom/errname/com_github_antonboom_errname-v0.1.12.zip",
            "http://ats.apps.svc/gomod/github.com/Antonboom/errname/com_github_antonboom_errname-v0.1.12.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Antonboom/errname/com_github_antonboom_errname-v0.1.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Antonboom/errname/com_github_antonboom_errname-v0.1.12.zip",
        ],
    )
    go_repository(
        name = "com_github_antonboom_nilnil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Antonboom/nilnil",
        sha256 = "f82e42fd3a6601252d881b50ba52d91131a981e3835cc160c55c4735734feade",
        strip_prefix = "github.com/Antonboom/nilnil@v0.1.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Antonboom/nilnil/com_github_antonboom_nilnil-v0.1.7.zip",
            "http://ats.apps.svc/gomod/github.com/Antonboom/nilnil/com_github_antonboom_nilnil-v0.1.7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Antonboom/nilnil/com_github_antonboom_nilnil-v0.1.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Antonboom/nilnil/com_github_antonboom_nilnil-v0.1.7.zip",
        ],
    )
    go_repository(
        name = "com_github_antonboom_testifylint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Antonboom/testifylint",
        sha256 = "eee3030c2343d5a2ee776ad0cbc596dea2b1ceaf9739d5e16e0f5cdc436c5e6f",
        strip_prefix = "github.com/Antonboom/testifylint@v1.1.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Antonboom/testifylint/com_github_antonboom_testifylint-v1.1.2.zip",
            "http://ats.apps.svc/gomod/github.com/Antonboom/testifylint/com_github_antonboom_testifylint-v1.1.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Antonboom/testifylint/com_github_antonboom_testifylint-v1.1.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Antonboom/testifylint/com_github_antonboom_testifylint-v1.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_apache_skywalking_eyes",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/apache/skywalking-eyes",
        sha256 = "4fb4df2319ec798ec72d31a13e90c51e3fa4405cb69e5e4b701bb55dbfd4a360",
        strip_prefix = "github.com/apache/skywalking-eyes@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/apache/skywalking-eyes/com_github_apache_skywalking_eyes-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/apache/skywalking-eyes/com_github_apache_skywalking_eyes-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/apache/skywalking-eyes/com_github_apache_skywalking_eyes-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/apache/skywalking-eyes/com_github_apache_skywalking_eyes-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_apache_thrift",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/apache/thrift",
        sha256 = "50d5c610df30fa2a6039394d5142382b7d9938870dfb12ef46bddfa3da250893",
        strip_prefix = "github.com/apache/thrift@v0.16.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/apache/thrift/com_github_apache_thrift-v0.16.0.zip",
            "http://ats.apps.svc/gomod/github.com/apache/thrift/com_github_apache_thrift-v0.16.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/apache/thrift/com_github_apache_thrift-v0.16.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/apache/thrift/com_github_apache_thrift-v0.16.0.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_consul_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/consul-api",
        sha256 = "091b79667f16ae245785956c490fe05ee26970a89f8ecdbe858ae3510d725088",
        strip_prefix = "github.com/armon/consul-api@v0.0.0-20180202201655-eb2c6b5be1b6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/armon/consul-api/com_github_armon_consul_api-v0.0.0-20180202201655-eb2c6b5be1b6.zip",
            "http://ats.apps.svc/gomod/github.com/armon/consul-api/com_github_armon_consul_api-v0.0.0-20180202201655-eb2c6b5be1b6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/armon/consul-api/com_github_armon_consul_api-v0.0.0-20180202201655-eb2c6b5be1b6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/armon/consul-api/com_github_armon_consul_api-v0.0.0-20180202201655-eb2c6b5be1b6.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_go_metrics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/go-metrics",
        sha256 = "f1b9155b8635eea48fb8929934b1268bf624cec2d51fcef8b62fa4aa91e05cc9",
        strip_prefix = "github.com/armon/go-metrics@v0.4.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/armon/go-metrics/com_github_armon_go_metrics-v0.4.1.zip",
            "http://ats.apps.svc/gomod/github.com/armon/go-metrics/com_github_armon_go_metrics-v0.4.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/armon/go-metrics/com_github_armon_go_metrics-v0.4.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/armon/go-metrics/com_github_armon_go_metrics-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_go_socks5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/go-socks5",
        sha256 = "f473e6dce826a0552639833cf72cfaa8bc7141daa7b537622d7f78eacfd9dfb3",
        strip_prefix = "github.com/armon/go-socks5@v0.0.0-20160902184237-e75332964ef5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/armon/go-socks5/com_github_armon_go_socks5-v0.0.0-20160902184237-e75332964ef5.zip",
            "http://ats.apps.svc/gomod/github.com/armon/go-socks5/com_github_armon_go_socks5-v0.0.0-20160902184237-e75332964ef5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/armon/go-socks5/com_github_armon_go_socks5-v0.0.0-20160902184237-e75332964ef5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/armon/go-socks5/com_github_armon_go_socks5-v0.0.0-20160902184237-e75332964ef5.zip",
        ],
    )
    go_repository(
        name = "com_github_asaskevich_govalidator",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/asaskevich/govalidator",
        sha256 = "0f8ec67bbc585d29ec115c0885cef6f2431a422cc1cc10008e466ebe8be5dc37",
        strip_prefix = "github.com/asaskevich/govalidator@v0.0.0-20230301143203-a9d515a09cc2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/asaskevich/govalidator/com_github_asaskevich_govalidator-v0.0.0-20230301143203-a9d515a09cc2.zip",
            "http://ats.apps.svc/gomod/github.com/asaskevich/govalidator/com_github_asaskevich_govalidator-v0.0.0-20230301143203-a9d515a09cc2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/asaskevich/govalidator/com_github_asaskevich_govalidator-v0.0.0-20230301143203-a9d515a09cc2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/asaskevich/govalidator/com_github_asaskevich_govalidator-v0.0.0-20230301143203-a9d515a09cc2.zip",
        ],
    )
    go_repository(
        name = "com_github_ashanbrown_forbidigo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ashanbrown/forbidigo",
        sha256 = "42476799732e399e46d47ced87090adb564f58c68097446296451cbae9e5580d",
        strip_prefix = "github.com/ashanbrown/forbidigo@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ashanbrown/forbidigo/com_github_ashanbrown_forbidigo-v1.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/ashanbrown/forbidigo/com_github_ashanbrown_forbidigo-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ashanbrown/forbidigo/com_github_ashanbrown_forbidigo-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ashanbrown/forbidigo/com_github_ashanbrown_forbidigo-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ashanbrown_makezero",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ashanbrown/makezero",
        sha256 = "46c7b3da763b02a05f70272662bb247475d5c50d928b55004876ad31b40744e9",
        strip_prefix = "github.com/ashanbrown/makezero@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ashanbrown/makezero/com_github_ashanbrown_makezero-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/ashanbrown/makezero/com_github_ashanbrown_makezero-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ashanbrown/makezero/com_github_ashanbrown_makezero-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ashanbrown/makezero/com_github_ashanbrown_makezero-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go",
        sha256 = "a34e669cf2f2caa9dab2f4db1c2e4445c3be627f3f3086a82fc678287adb28a8",
        strip_prefix = "github.com/aws/aws-sdk-go@v1.48.14",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/aws/aws-sdk-go/com_github_aws_aws_sdk_go-v1.48.14.zip",
            "http://ats.apps.svc/gomod/github.com/aws/aws-sdk-go/com_github_aws_aws_sdk_go-v1.48.14.zip",
            "https://cache.hawkingrei.com/gomod/github.com/aws/aws-sdk-go/com_github_aws_aws_sdk_go-v1.48.14.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/aws/aws-sdk-go/com_github_aws_aws_sdk_go-v1.48.14.zip",
        ],
    )
    go_repository(
        name = "com_github_aymerick_raymond",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aymerick/raymond",
        sha256 = "0a759716a73b587a436b3b4a95416a58bb1ffa1decf2cd7a92f1eeb2f9c654c1",
        strip_prefix = "github.com/aymerick/raymond@v2.0.3-0.20180322193309-b565731e1464+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/aymerick/raymond/com_github_aymerick_raymond-v2.0.3-0.20180322193309-b565731e1464+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/aymerick/raymond/com_github_aymerick_raymond-v2.0.3-0.20180322193309-b565731e1464+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/aymerick/raymond/com_github_aymerick_raymond-v2.0.3-0.20180322193309-b565731e1464+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/aymerick/raymond/com_github_aymerick_raymond-v2.0.3-0.20180322193309-b565731e1464+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azcore",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azcore",
        sha256 = "bdca5cf77bf71564df8f4cc53b607da0a966c5d8611a2fc0cdfee4acc2bf8cc1",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/azcore@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/azcore/com_github_azure_azure_sdk_for_go_sdk_azcore-v1.9.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/azcore/com_github_azure_azure_sdk_for_go_sdk_azcore-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/azcore/com_github_azure_azure_sdk_for_go_sdk_azcore-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/azcore/com_github_azure_azure_sdk_for_go_sdk_azcore-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azidentity",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azidentity",
        sha256 = "39566249254f05e58d8a8a1324cd44c0545ca4091b34d5d86dfb832062b8302c",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/azidentity@v1.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/azidentity/com_github_azure_azure_sdk_for_go_sdk_azidentity-v1.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/azidentity/com_github_azure_azure_sdk_for_go_sdk_azidentity-v1.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/azidentity/com_github_azure_azure_sdk_for_go_sdk_azidentity-v1.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/azidentity/com_github_azure_azure_sdk_for_go_sdk_azidentity-v1.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_internal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/internal",
        sha256 = "d7e0270a6da5d9d2e2a6f799d366080f1a6b038ccbe76d036131a0da0835aff8",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/internal@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/internal/com_github_azure_azure_sdk_for_go_sdk_internal-v1.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/internal/com_github_azure_azure_sdk_for_go_sdk_internal-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/internal/com_github_azure_azure_sdk_for_go_sdk_internal-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/internal/com_github_azure_azure_sdk_for_go_sdk_internal-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v4",
        sha256 = "b0c3b75b9e8fc156c488016d93e411f3089b5b97cd8250ac30a4746a558d3b62",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v4@v4.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v4/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v4-v4.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v4/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v4-v4.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v4/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v4-v4.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v4/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v4-v4.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork",
        sha256 = "ea444a1c3fcddb0477f7d1df7716c4d9a9edf5d89b12bbd5c92e89c036a1c01b",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2",
        sha256 = "4e0253514cf7072a29ddb22adf71cea03a44935a05de3897910a3932ae0034e3",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2@v2.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v2-v2.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v2-v2.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v2-v2.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v2-v2.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_storage_azblob",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob",
        sha256 = "9bb69aea32f1d59711701f9562d66432c9c0374205e5009d1d1a62f03fb4fdad",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/com_github_azure_azure_sdk_for_go_sdk_storage_azblob-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/com_github_azure_azure_sdk_for_go_sdk_storage_azblob-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/com_github_azure_azure_sdk_for_go_sdk_storage_azblob-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/com_github_azure_azure_sdk_for_go_sdk_storage_azblob-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_ntlmssp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-ntlmssp",
        sha256 = "cc6d4e9caf938a71c9217f3aa8bdbb1c072faff3444bb680a2759c947da2085c",
        strip_prefix = "github.com/Azure/go-ntlmssp@v0.0.0-20221128193559-754e69321358",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/go-ntlmssp/com_github_azure_go_ntlmssp-v0.0.0-20221128193559-754e69321358.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/go-ntlmssp/com_github_azure_go_ntlmssp-v0.0.0-20221128193559-754e69321358.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/go-ntlmssp/com_github_azure_go_ntlmssp-v0.0.0-20221128193559-754e69321358.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/go-ntlmssp/com_github_azure_go_ntlmssp-v0.0.0-20221128193559-754e69321358.zip",
        ],
    )
    go_repository(
        name = "com_github_azuread_microsoft_authentication_library_for_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/AzureAD/microsoft-authentication-library-for-go",
        sha256 = "6f933f00d5310409c8f3fe25917c3c48abb94fa9c582a9ce6ae35eaafe80d06c",
        strip_prefix = "github.com/AzureAD/microsoft-authentication-library-for-go@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/AzureAD/microsoft-authentication-library-for-go/com_github_azuread_microsoft_authentication_library_for_go-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/AzureAD/microsoft-authentication-library-for-go/com_github_azuread_microsoft_authentication_library_for_go-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/AzureAD/microsoft-authentication-library-for-go/com_github_azuread_microsoft_authentication_library_for_go-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/AzureAD/microsoft-authentication-library-for-go/com_github_azuread_microsoft_authentication_library_for_go-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_bazelbuild_buildtools",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bazelbuild/buildtools",
        sha256 = "5ec0befc70edf16728838d94b240dfd01ba576f8a3901de84c0861c0ce2b8db6",
        strip_prefix = "github.com/bazelbuild/buildtools@v0.0.0-20230926111657-7d855c59baeb",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bazelbuild/buildtools/com_github_bazelbuild_buildtools-v0.0.0-20230926111657-7d855c59baeb.zip",
            "http://ats.apps.svc/gomod/github.com/bazelbuild/buildtools/com_github_bazelbuild_buildtools-v0.0.0-20230926111657-7d855c59baeb.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bazelbuild/buildtools/com_github_bazelbuild_buildtools-v0.0.0-20230926111657-7d855c59baeb.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bazelbuild/buildtools/com_github_bazelbuild_buildtools-v0.0.0-20230926111657-7d855c59baeb.zip",
        ],
    )
    go_repository(
        name = "com_github_bazelbuild_rules_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bazelbuild/rules_go",
        sha256 = "f39abb77746d12e017795acf52262756e1c74fd2105d6ad8164d10a27407f2c0",
        strip_prefix = "github.com/bazelbuild/rules_go@v0.42.1-0.20231101215950-df20c987afcb",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bazelbuild/rules_go/com_github_bazelbuild_rules_go-v0.42.1-0.20231101215950-df20c987afcb.zip",
            "http://ats.apps.svc/gomod/github.com/bazelbuild/rules_go/com_github_bazelbuild_rules_go-v0.42.1-0.20231101215950-df20c987afcb.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bazelbuild/rules_go/com_github_bazelbuild_rules_go-v0.42.1-0.20231101215950-df20c987afcb.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bazelbuild/rules_go/com_github_bazelbuild_rules_go-v0.42.1-0.20231101215950-df20c987afcb.zip",
        ],
    )
    go_repository(
        name = "com_github_benbjohnson_clock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/benbjohnson/clock",
        sha256 = "b615224e45f86907cfb0acc2b198dacea85ced624ed6c497ca2e7e705a53f2f9",
        strip_prefix = "github.com/benbjohnson/clock@v1.3.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/benbjohnson/clock/com_github_benbjohnson_clock-v1.3.5.zip",
            "http://ats.apps.svc/gomod/github.com/benbjohnson/clock/com_github_benbjohnson_clock-v1.3.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/benbjohnson/clock/com_github_benbjohnson_clock-v1.3.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/benbjohnson/clock/com_github_benbjohnson_clock-v1.3.5.zip",
        ],
    )
    go_repository(
        name = "com_github_beorn7_perks",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/beorn7/perks",
        sha256 = "25bd9e2d94aca770e6dbc1f53725f84f6af4432f631d35dd2c46f96ef0512f1a",
        strip_prefix = "github.com/beorn7/perks@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/beorn7/perks/com_github_beorn7_perks-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/beorn7/perks/com_github_beorn7_perks-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/beorn7/perks/com_github_beorn7_perks-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/beorn7/perks/com_github_beorn7_perks-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_bkielbasa_cyclop",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bkielbasa/cyclop",
        sha256 = "f5e2d2dd17ec6f79111c4773c69077950f3bb739f21aea38be2195dc6541d53a",
        strip_prefix = "github.com/bkielbasa/cyclop@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bkielbasa/cyclop/com_github_bkielbasa_cyclop-v1.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/bkielbasa/cyclop/com_github_bkielbasa_cyclop-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bkielbasa/cyclop/com_github_bkielbasa_cyclop-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bkielbasa/cyclop/com_github_bkielbasa_cyclop-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_blacktear23_go_proxyprotocol",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/blacktear23/go-proxyprotocol",
        sha256 = "156ac8095023f9aa7a0bf0706508601443492fc063f0b73dd20e728e912c5bd0",
        strip_prefix = "github.com/blacktear23/go-proxyprotocol@v1.0.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/blacktear23/go-proxyprotocol/com_github_blacktear23_go_proxyprotocol-v1.0.6.zip",
            "http://ats.apps.svc/gomod/github.com/blacktear23/go-proxyprotocol/com_github_blacktear23_go_proxyprotocol-v1.0.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/blacktear23/go-proxyprotocol/com_github_blacktear23_go_proxyprotocol-v1.0.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/blacktear23/go-proxyprotocol/com_github_blacktear23_go_proxyprotocol-v1.0.6.zip",
        ],
    )
    go_repository(
        name = "com_github_blizzy78_varnamelen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/blizzy78/varnamelen",
        sha256 = "2f7dd2db1e40fd541088d2bd3e0e68e430653ad644c6f1656de42d7d01d0b261",
        strip_prefix = "github.com/blizzy78/varnamelen@v0.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/blizzy78/varnamelen/com_github_blizzy78_varnamelen-v0.8.0.zip",
            "http://ats.apps.svc/gomod/github.com/blizzy78/varnamelen/com_github_blizzy78_varnamelen-v0.8.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/blizzy78/varnamelen/com_github_blizzy78_varnamelen-v0.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/blizzy78/varnamelen/com_github_blizzy78_varnamelen-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_bmatcuk_doublestar_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bmatcuk/doublestar/v2",
        sha256 = "aa78ea07acab3278737ba2b2d31bae185f414afe187f76589178b25db8aa7b8c",
        strip_prefix = "github.com/bmatcuk/doublestar/v2@v2.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bmatcuk/doublestar/v2/com_github_bmatcuk_doublestar_v2-v2.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/bmatcuk/doublestar/v2/com_github_bmatcuk_doublestar_v2-v2.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bmatcuk/doublestar/v2/com_github_bmatcuk_doublestar_v2-v2.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bmatcuk/doublestar/v2/com_github_bmatcuk_doublestar_v2-v2.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_bombsimon_wsl_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bombsimon/wsl/v4",
        sha256 = "b43bea65b820b7ca13b4b378902dbf3111965b94490a92055a5121a2c3a573b3",
        strip_prefix = "github.com/bombsimon/wsl/v4@v4.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bombsimon/wsl/v4/com_github_bombsimon_wsl_v4-v4.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/bombsimon/wsl/v4/com_github_bombsimon_wsl_v4-v4.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bombsimon/wsl/v4/com_github_bombsimon_wsl_v4-v4.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bombsimon/wsl/v4/com_github_bombsimon_wsl_v4-v4.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_breml_bidichk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/breml/bidichk",
        sha256 = "ceab3b883c2afc022dbc17831abadf3ffb1ddea7be9bdf200d9d98b84cce46e8",
        strip_prefix = "github.com/breml/bidichk@v0.2.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/breml/bidichk/com_github_breml_bidichk-v0.2.7.zip",
            "http://ats.apps.svc/gomod/github.com/breml/bidichk/com_github_breml_bidichk-v0.2.7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/breml/bidichk/com_github_breml_bidichk-v0.2.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/breml/bidichk/com_github_breml_bidichk-v0.2.7.zip",
        ],
    )
    go_repository(
        name = "com_github_breml_errchkjson",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/breml/errchkjson",
        sha256 = "ce3fa45f053a2df5c88273addb0e4abeaada62ba7225e9e6248df43ca2aa1013",
        strip_prefix = "github.com/breml/errchkjson@v0.3.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/breml/errchkjson/com_github_breml_errchkjson-v0.3.6.zip",
            "http://ats.apps.svc/gomod/github.com/breml/errchkjson/com_github_breml_errchkjson-v0.3.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/breml/errchkjson/com_github_breml_errchkjson-v0.3.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/breml/errchkjson/com_github_breml_errchkjson-v0.3.6.zip",
        ],
    )
    go_repository(
        name = "com_github_burntsushi_toml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/BurntSushi/toml",
        sha256 = "5de246a0cb4c256f3fd5d0db8a08a114f58af0c2e193bbf0ad9012104adbb6b2",
        strip_prefix = "github.com/BurntSushi/toml@v1.3.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/BurntSushi/toml/com_github_burntsushi_toml-v1.3.2.zip",
            "http://ats.apps.svc/gomod/github.com/BurntSushi/toml/com_github_burntsushi_toml-v1.3.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/BurntSushi/toml/com_github_burntsushi_toml-v1.3.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/BurntSushi/toml/com_github_burntsushi_toml-v1.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_burntsushi_xgb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/BurntSushi/xgb",
        sha256 = "f52962c7fbeca81ea8a777d1f8b1f1d25803dc437fbb490f253344232884328e",
        strip_prefix = "github.com/BurntSushi/xgb@v0.0.0-20160522181843-27f122750802",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/BurntSushi/xgb/com_github_burntsushi_xgb-v0.0.0-20160522181843-27f122750802.zip",
            "http://ats.apps.svc/gomod/github.com/BurntSushi/xgb/com_github_burntsushi_xgb-v0.0.0-20160522181843-27f122750802.zip",
            "https://cache.hawkingrei.com/gomod/github.com/BurntSushi/xgb/com_github_burntsushi_xgb-v0.0.0-20160522181843-27f122750802.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/BurntSushi/xgb/com_github_burntsushi_xgb-v0.0.0-20160522181843-27f122750802.zip",
        ],
    )
    go_repository(
        name = "com_github_butuzov_ireturn",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/butuzov/ireturn",
        sha256 = "50c41a3c2bc7c0caa749e941bbc6899764c4d6194e59592f786ca889f1357a96",
        strip_prefix = "github.com/butuzov/ireturn@v0.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/butuzov/ireturn/com_github_butuzov_ireturn-v0.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/butuzov/ireturn/com_github_butuzov_ireturn-v0.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/butuzov/ireturn/com_github_butuzov_ireturn-v0.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/butuzov/ireturn/com_github_butuzov_ireturn-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_butuzov_mirror",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/butuzov/mirror",
        sha256 = "6ac5d5075646123f8d7b0f3659087c50e6762b06606497ad05fadc3a8f196c06",
        strip_prefix = "github.com/butuzov/mirror@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/butuzov/mirror/com_github_butuzov_mirror-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/butuzov/mirror/com_github_butuzov_mirror-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/butuzov/mirror/com_github_butuzov_mirror-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/butuzov/mirror/com_github_butuzov_mirror-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cakturk_go_netstat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cakturk/go-netstat",
        sha256 = "c8c3a7b894b4522d56bef918d1299b848ea78c566e19d3e35afa7ce0a207b5ab",
        strip_prefix = "github.com/cakturk/go-netstat@v0.0.0-20200220111822-e5b49efee7a5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cakturk/go-netstat/com_github_cakturk_go_netstat-v0.0.0-20200220111822-e5b49efee7a5.zip",
            "http://ats.apps.svc/gomod/github.com/cakturk/go-netstat/com_github_cakturk_go_netstat-v0.0.0-20200220111822-e5b49efee7a5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cakturk/go-netstat/com_github_cakturk_go_netstat-v0.0.0-20200220111822-e5b49efee7a5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cakturk/go-netstat/com_github_cakturk_go_netstat-v0.0.0-20200220111822-e5b49efee7a5.zip",
        ],
    )
    go_repository(
        name = "com_github_carlmjohnson_flagext",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/carlmjohnson/flagext",
        sha256 = "a0ddd38aeb139c1c9c1a4439601782d03cb4eefe2b137d1e908494de43d234b9",
        strip_prefix = "github.com/carlmjohnson/flagext@v0.21.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/carlmjohnson/flagext/com_github_carlmjohnson_flagext-v0.21.0.zip",
            "http://ats.apps.svc/gomod/github.com/carlmjohnson/flagext/com_github_carlmjohnson_flagext-v0.21.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/carlmjohnson/flagext/com_github_carlmjohnson_flagext-v0.21.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/carlmjohnson/flagext/com_github_carlmjohnson_flagext-v0.21.0.zip",
        ],
    )
    go_repository(
        name = "com_github_catenacyber_perfsprint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/catenacyber/perfsprint",
        sha256 = "52cb92e138ff4823c45fb5b6cd8b6c67fd540d88ff44a105fd38bd9910c14dc5",
        strip_prefix = "github.com/catenacyber/perfsprint@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/catenacyber/perfsprint/com_github_catenacyber_perfsprint-v0.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/catenacyber/perfsprint/com_github_catenacyber_perfsprint-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/catenacyber/perfsprint/com_github_catenacyber_perfsprint-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/catenacyber/perfsprint/com_github_catenacyber_perfsprint-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ccojocar_zxcvbn_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ccojocar/zxcvbn-go",
        sha256 = "5f5ed8c7bd3469315edbf5726ce0d9dc6a66a94e854a652b66b53e082cdfd399",
        strip_prefix = "github.com/ccojocar/zxcvbn-go@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ccojocar/zxcvbn-go/com_github_ccojocar_zxcvbn_go-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/ccojocar/zxcvbn-go/com_github_ccojocar_zxcvbn_go-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ccojocar/zxcvbn-go/com_github_ccojocar_zxcvbn_go-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ccojocar/zxcvbn-go/com_github_ccojocar_zxcvbn_go-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_cenkalti_backoff_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cenkalti/backoff/v4",
        sha256 = "0b1d9cedebb1b814f4fbc03a47fdd2c2bb91d8cf14dbb1a71d3bc1482600cd2a",
        strip_prefix = "github.com/cenkalti/backoff/v4@v4.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cenkalti/backoff/v4/com_github_cenkalti_backoff_v4-v4.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/cenkalti/backoff/v4/com_github_cenkalti_backoff_v4-v4.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cenkalti/backoff/v4/com_github_cenkalti_backoff_v4-v4.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cenkalti/backoff/v4/com_github_cenkalti_backoff_v4-v4.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_census_instrumentation_opencensus_proto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/census-instrumentation/opencensus-proto",
        sha256 = "6fce66b7dcd2cba031ed9d73d77d6b21c2fe749c5de27cbb416a2d2cc1c68719",
        strip_prefix = "github.com/census-instrumentation/opencensus-proto@v0.4.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/census-instrumentation/opencensus-proto/com_github_census_instrumentation_opencensus_proto-v0.4.1.zip",
            "http://ats.apps.svc/gomod/github.com/census-instrumentation/opencensus-proto/com_github_census_instrumentation_opencensus_proto-v0.4.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/census-instrumentation/opencensus-proto/com_github_census_instrumentation_opencensus_proto-v0.4.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/census-instrumentation/opencensus-proto/com_github_census_instrumentation_opencensus_proto-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_cespare_xxhash_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cespare/xxhash/v2",
        sha256 = "fc180cdb0c00fbffbd39b774a72cdb5f0c32ace25370d5135195918a8c3fbd25",
        strip_prefix = "github.com/cespare/xxhash/v2@v2.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cespare/xxhash/v2/com_github_cespare_xxhash_v2-v2.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/cespare/xxhash/v2/com_github_cespare_xxhash_v2-v2.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cespare/xxhash/v2/com_github_cespare_xxhash_v2-v2.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cespare/xxhash/v2/com_github_cespare_xxhash_v2-v2.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_charithe_durationcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/charithe/durationcheck",
        sha256 = "250aebaee51d0596b00135b96c0920cbe463134494e69346da38da67cd3b0c8f",
        strip_prefix = "github.com/charithe/durationcheck@v0.0.10",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/charithe/durationcheck/com_github_charithe_durationcheck-v0.0.10.zip",
            "http://ats.apps.svc/gomod/github.com/charithe/durationcheck/com_github_charithe_durationcheck-v0.0.10.zip",
            "https://cache.hawkingrei.com/gomod/github.com/charithe/durationcheck/com_github_charithe_durationcheck-v0.0.10.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/charithe/durationcheck/com_github_charithe_durationcheck-v0.0.10.zip",
        ],
    )
    go_repository(
        name = "com_github_chavacava_garif",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chavacava/garif",
        sha256 = "b1dfcd738139918fc2008bea5115fb9ddbbf2dc361d65448c47101a23072f088",
        strip_prefix = "github.com/chavacava/garif@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/chavacava/garif/com_github_chavacava_garif-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/chavacava/garif/com_github_chavacava_garif-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/chavacava/garif/com_github_chavacava_garif-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/chavacava/garif/com_github_chavacava_garif-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cheggaaa_pb_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cheggaaa/pb/v3",
        sha256 = "1dbcbfbc4edfe2fe24ae27e3e7003583cccbfb67c75b78b7285ae360cd674888",
        strip_prefix = "github.com/cheggaaa/pb/v3@v3.0.8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cheggaaa/pb/v3/com_github_cheggaaa_pb_v3-v3.0.8.zip",
            "http://ats.apps.svc/gomod/github.com/cheggaaa/pb/v3/com_github_cheggaaa_pb_v3-v3.0.8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cheggaaa/pb/v3/com_github_cheggaaa_pb_v3-v3.0.8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cheggaaa/pb/v3/com_github_cheggaaa_pb_v3-v3.0.8.zip",
        ],
    )
    go_repository(
        name = "com_github_cheynewallace_tabby",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cheynewallace/tabby",
        sha256 = "8d58c5f49571b35da7a88224744cfb145fe8aa40b5a84c3e203c491f846e70c1",
        strip_prefix = "github.com/cheynewallace/tabby@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cheynewallace/tabby/com_github_cheynewallace_tabby-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/cheynewallace/tabby/com_github_cheynewallace_tabby-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cheynewallace/tabby/com_github_cheynewallace_tabby-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cheynewallace/tabby/com_github_cheynewallace_tabby-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_chromedp_cdproto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chromedp/cdproto",
        sha256 = "23440cb9922bc66da55e23455aaf53799b4e838516dfca92202f29d21f9f4ad3",
        strip_prefix = "github.com/chromedp/cdproto@v0.0.0-20230802225258-3cf4e6d46a89",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/chromedp/cdproto/com_github_chromedp_cdproto-v0.0.0-20230802225258-3cf4e6d46a89.zip",
            "http://ats.apps.svc/gomod/github.com/chromedp/cdproto/com_github_chromedp_cdproto-v0.0.0-20230802225258-3cf4e6d46a89.zip",
            "https://cache.hawkingrei.com/gomod/github.com/chromedp/cdproto/com_github_chromedp_cdproto-v0.0.0-20230802225258-3cf4e6d46a89.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/chromedp/cdproto/com_github_chromedp_cdproto-v0.0.0-20230802225258-3cf4e6d46a89.zip",
        ],
    )
    go_repository(
        name = "com_github_chromedp_chromedp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chromedp/chromedp",
        sha256 = "f141d0c242b87bafe550404588cd86ba1e6ba05d9d1774ce96d4d097455b51d6",
        strip_prefix = "github.com/chromedp/chromedp@v0.9.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/chromedp/chromedp/com_github_chromedp_chromedp-v0.9.2.zip",
            "http://ats.apps.svc/gomod/github.com/chromedp/chromedp/com_github_chromedp_chromedp-v0.9.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/chromedp/chromedp/com_github_chromedp_chromedp-v0.9.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/chromedp/chromedp/com_github_chromedp_chromedp-v0.9.2.zip",
        ],
    )
    go_repository(
        name = "com_github_chromedp_sysutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chromedp/sysutil",
        sha256 = "0d2f5cf0478bef0a8ee71e8b60a9279fd55b07cbfc66dbcfbf5a5f4ccb905c62",
        strip_prefix = "github.com/chromedp/sysutil@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/chromedp/sysutil/com_github_chromedp_sysutil-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/chromedp/sysutil/com_github_chromedp_sysutil-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/chromedp/sysutil/com_github_chromedp_sysutil-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/chromedp/sysutil/com_github_chromedp_sysutil-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_chzyer_logex",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chzyer/logex",
        sha256 = "2c94771c1e335a2c58a96444b3768b8e00297747d6ce7e7c14bab2e8b39d91bd",
        strip_prefix = "github.com/chzyer/logex@v1.1.10",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/chzyer/logex/com_github_chzyer_logex-v1.1.10.zip",
            "http://ats.apps.svc/gomod/github.com/chzyer/logex/com_github_chzyer_logex-v1.1.10.zip",
            "https://cache.hawkingrei.com/gomod/github.com/chzyer/logex/com_github_chzyer_logex-v1.1.10.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/chzyer/logex/com_github_chzyer_logex-v1.1.10.zip",
        ],
    )
    go_repository(
        name = "com_github_chzyer_readline",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chzyer/readline",
        sha256 = "ce25854a8beae5c20bdde840d5142e6fbd1f86f0e58442705b8fb21dfce48501",
        strip_prefix = "github.com/chzyer/readline@v1.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/chzyer/readline/com_github_chzyer_readline-v1.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/chzyer/readline/com_github_chzyer_readline-v1.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/chzyer/readline/com_github_chzyer_readline-v1.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/chzyer/readline/com_github_chzyer_readline-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_chzyer_test",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chzyer/test",
        sha256 = "ad8550bed3c4a94bbef57b9fc5bb15806eaceda00925716404320580d60e2f7d",
        strip_prefix = "github.com/chzyer/test@v0.0.0-20180213035817-a1ea475d72b1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/chzyer/test/com_github_chzyer_test-v0.0.0-20180213035817-a1ea475d72b1.zip",
            "http://ats.apps.svc/gomod/github.com/chzyer/test/com_github_chzyer_test-v0.0.0-20180213035817-a1ea475d72b1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/chzyer/test/com_github_chzyer_test-v0.0.0-20180213035817-a1ea475d72b1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/chzyer/test/com_github_chzyer_test-v0.0.0-20180213035817-a1ea475d72b1.zip",
        ],
    )
    go_repository(
        name = "com_github_client9_misspell",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/client9/misspell",
        sha256 = "a3af206372e131dd10a68ac470c66a1b18eaf51c6afacb55b2e2a06e39b90728",
        strip_prefix = "github.com/client9/misspell@v0.3.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/client9/misspell/com_github_client9_misspell-v0.3.4.zip",
            "http://ats.apps.svc/gomod/github.com/client9/misspell/com_github_client9_misspell-v0.3.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/client9/misspell/com_github_client9_misspell-v0.3.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/client9/misspell/com_github_client9_misspell-v0.3.4.zip",
        ],
    )
    go_repository(
        name = "com_github_cloudfoundry_gosigar",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cloudfoundry/gosigar",
        sha256 = "44bd2b560d804fe98453100d6adfc4dd9c92f76713cfb543700a347317d5dc11",
        strip_prefix = "github.com/cloudfoundry/gosigar@v1.3.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cloudfoundry/gosigar/com_github_cloudfoundry_gosigar-v1.3.6.zip",
            "http://ats.apps.svc/gomod/github.com/cloudfoundry/gosigar/com_github_cloudfoundry_gosigar-v1.3.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cloudfoundry/gosigar/com_github_cloudfoundry_gosigar-v1.3.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cloudfoundry/gosigar/com_github_cloudfoundry_gosigar-v1.3.6.zip",
        ],
    )
    go_repository(
        name = "com_github_cloudykit_fastprinter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/CloudyKit/fastprinter",
        sha256 = "6e4b00c3d8de85c23b7e90e6b6fe4863d3317775493a81197155e0a410d6ed57",
        strip_prefix = "github.com/CloudyKit/fastprinter@v0.0.0-20170127035650-74b38d55f37a",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/CloudyKit/fastprinter/com_github_cloudykit_fastprinter-v0.0.0-20170127035650-74b38d55f37a.zip",
            "http://ats.apps.svc/gomod/github.com/CloudyKit/fastprinter/com_github_cloudykit_fastprinter-v0.0.0-20170127035650-74b38d55f37a.zip",
            "https://cache.hawkingrei.com/gomod/github.com/CloudyKit/fastprinter/com_github_cloudykit_fastprinter-v0.0.0-20170127035650-74b38d55f37a.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/CloudyKit/fastprinter/com_github_cloudykit_fastprinter-v0.0.0-20170127035650-74b38d55f37a.zip",
        ],
    )
    go_repository(
        name = "com_github_cloudykit_jet",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/CloudyKit/jet",
        sha256 = "9191d5a10096ea10bbb4ea576131ba943a6ec600fd7358237e21f995240ec72f",
        strip_prefix = "github.com/CloudyKit/jet@v2.1.3-0.20180809161101-62edd43e4f88+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/CloudyKit/jet/com_github_cloudykit_jet-v2.1.3-0.20180809161101-62edd43e4f88+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/CloudyKit/jet/com_github_cloudykit_jet-v2.1.3-0.20180809161101-62edd43e4f88+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/CloudyKit/jet/com_github_cloudykit_jet-v2.1.3-0.20180809161101-62edd43e4f88+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/CloudyKit/jet/com_github_cloudykit_jet-v2.1.3-0.20180809161101-62edd43e4f88+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_cncf_udpa_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cncf/udpa/go",
        sha256 = "8fe1585f25d40a5e3cd4243a92143d71ae4ee92e915e7192e72387047539438e",
        strip_prefix = "github.com/cncf/udpa/go@v0.0.0-20220112060539-c52dc94e7fbe",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cncf/udpa/go/com_github_cncf_udpa_go-v0.0.0-20220112060539-c52dc94e7fbe.zip",
            "http://ats.apps.svc/gomod/github.com/cncf/udpa/go/com_github_cncf_udpa_go-v0.0.0-20220112060539-c52dc94e7fbe.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cncf/udpa/go/com_github_cncf_udpa_go-v0.0.0-20220112060539-c52dc94e7fbe.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cncf/udpa/go/com_github_cncf_udpa_go-v0.0.0-20220112060539-c52dc94e7fbe.zip",
        ],
    )
    go_repository(
        name = "com_github_cncf_xds_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cncf/xds/go",
        sha256 = "ab0d2fd980b15a582708a728cf8080ebb88778e59f3003b67c6aafaa9ad0f447",
        strip_prefix = "github.com/cncf/xds/go@v0.0.0-20231128003011-0fa0005c9caa",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cncf/xds/go/com_github_cncf_xds_go-v0.0.0-20231128003011-0fa0005c9caa.zip",
            "http://ats.apps.svc/gomod/github.com/cncf/xds/go/com_github_cncf_xds_go-v0.0.0-20231128003011-0fa0005c9caa.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cncf/xds/go/com_github_cncf_xds_go-v0.0.0-20231128003011-0fa0005c9caa.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cncf/xds/go/com_github_cncf_xds_go-v0.0.0-20231128003011-0fa0005c9caa.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_datadriven",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/datadriven",
        sha256 = "1818b828715b773ea9eaf415fa3cc176c411e18f645ec85440b14abaf1f387c4",
        strip_prefix = "github.com/cockroachdb/datadriven@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/datadriven/com_github_cockroachdb_datadriven-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/datadriven/com_github_cockroachdb_datadriven-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/datadriven/com_github_cockroachdb_datadriven-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/datadriven/com_github_cockroachdb_datadriven-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/errors",
        sha256 = "52552b154f458c03a5c514ccbacb21d7574e0a6d0428b63f16b9d6f7a655969b",
        strip_prefix = "github.com/cockroachdb/errors@v1.8.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/errors/com_github_cockroachdb_errors-v1.8.1.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/errors/com_github_cockroachdb_errors-v1.8.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/errors/com_github_cockroachdb_errors-v1.8.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/errors/com_github_cockroachdb_errors-v1.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_logtags",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/logtags",
        sha256 = "e0ff78268deed42414d58c55115e2a7db8d6b76f4165c02d8ba40d6cd32495a1",
        strip_prefix = "github.com/cockroachdb/logtags@v0.0.0-20190617123548-eb05cc24525f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/logtags/com_github_cockroachdb_logtags-v0.0.0-20190617123548-eb05cc24525f.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/logtags/com_github_cockroachdb_logtags-v0.0.0-20190617123548-eb05cc24525f.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/logtags/com_github_cockroachdb_logtags-v0.0.0-20190617123548-eb05cc24525f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/logtags/com_github_cockroachdb_logtags-v0.0.0-20190617123548-eb05cc24525f.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_pebble",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/pebble",
        sha256 = "6ecda6200019988f0453986e2f736fe588b94f62bb6db6cdc9f8ba783261d750",
        strip_prefix = "github.com/cockroachdb/pebble@v0.0.0-20220415182917-06c9d3be25b3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/pebble/com_github_cockroachdb_pebble-v0.0.0-20220415182917-06c9d3be25b3.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/pebble/com_github_cockroachdb_pebble-v0.0.0-20220415182917-06c9d3be25b3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/pebble/com_github_cockroachdb_pebble-v0.0.0-20220415182917-06c9d3be25b3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/pebble/com_github_cockroachdb_pebble-v0.0.0-20220415182917-06c9d3be25b3.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_redact",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/redact",
        sha256 = "2f583202b4c71b69102283e05b0528ace64ee7fde143c2260a4a4ec73a68e331",
        strip_prefix = "github.com/cockroachdb/redact@v1.0.8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/redact/com_github_cockroachdb_redact-v1.0.8.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/redact/com_github_cockroachdb_redact-v1.0.8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/redact/com_github_cockroachdb_redact-v1.0.8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/redact/com_github_cockroachdb_redact-v1.0.8.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_sentry_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/sentry-go",
        sha256 = "fbb2207d02aecfdd411b1357efe1192dbb827959e36b7cab7491731ac55935c9",
        strip_prefix = "github.com/cockroachdb/sentry-go@v0.6.1-cockroachdb.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/sentry-go/com_github_cockroachdb_sentry_go-v0.6.1-cockroachdb.2.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/sentry-go/com_github_cockroachdb_sentry_go-v0.6.1-cockroachdb.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/sentry-go/com_github_cockroachdb_sentry_go-v0.6.1-cockroachdb.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/sentry-go/com_github_cockroachdb_sentry_go-v0.6.1-cockroachdb.2.zip",
        ],
    )
    go_repository(
        name = "com_github_codahale_hdrhistogram",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/codahale/hdrhistogram",
        sha256 = "e7e117da64da2f921b1f9dc57c524430a7f74a78c4b0bad718d85b08e8374e78",
        strip_prefix = "github.com/codahale/hdrhistogram@v0.0.0-20161010025455-3a0bb77429bd",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/codahale/hdrhistogram/com_github_codahale_hdrhistogram-v0.0.0-20161010025455-3a0bb77429bd.zip",
            "http://ats.apps.svc/gomod/github.com/codahale/hdrhistogram/com_github_codahale_hdrhistogram-v0.0.0-20161010025455-3a0bb77429bd.zip",
            "https://cache.hawkingrei.com/gomod/github.com/codahale/hdrhistogram/com_github_codahale_hdrhistogram-v0.0.0-20161010025455-3a0bb77429bd.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/codahale/hdrhistogram/com_github_codahale_hdrhistogram-v0.0.0-20161010025455-3a0bb77429bd.zip",
        ],
    )
    go_repository(
        name = "com_github_code_hex_go_generics_cache",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Code-Hex/go-generics-cache",
        sha256 = "e545aab31a9ce268856afe920755ad0774289642eaa4b57a3d57eb003827eda0",
        strip_prefix = "github.com/Code-Hex/go-generics-cache@v1.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Code-Hex/go-generics-cache/com_github_code_hex_go_generics_cache-v1.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/Code-Hex/go-generics-cache/com_github_code_hex_go_generics_cache-v1.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Code-Hex/go-generics-cache/com_github_code_hex_go_generics_cache-v1.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Code-Hex/go-generics-cache/com_github_code_hex_go_generics_cache-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_codegangsta_inject",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/codegangsta/inject",
        sha256 = "0a324d56992bffd288fa70a6d10eb9b8a9467665b0b1eb749ac6ae80e8977ee2",
        strip_prefix = "github.com/codegangsta/inject@v0.0.0-20150114235600-33e0aa1cb7c0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/codegangsta/inject/com_github_codegangsta_inject-v0.0.0-20150114235600-33e0aa1cb7c0.zip",
            "http://ats.apps.svc/gomod/github.com/codegangsta/inject/com_github_codegangsta_inject-v0.0.0-20150114235600-33e0aa1cb7c0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/codegangsta/inject/com_github_codegangsta_inject-v0.0.0-20150114235600-33e0aa1cb7c0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/codegangsta/inject/com_github_codegangsta_inject-v0.0.0-20150114235600-33e0aa1cb7c0.zip",
        ],
    )
    go_repository(
        name = "com_github_colinmarc_hdfs_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/colinmarc/hdfs/v2",
        sha256 = "6a40084f999e3ddbd9a8566b1333646424201fc2ad28aa1a40ddf51aaf8fbc51",
        strip_prefix = "github.com/colinmarc/hdfs/v2@v2.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/colinmarc/hdfs/v2/com_github_colinmarc_hdfs_v2-v2.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/colinmarc/hdfs/v2/com_github_colinmarc_hdfs_v2-v2.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/colinmarc/hdfs/v2/com_github_colinmarc_hdfs_v2-v2.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/colinmarc/hdfs/v2/com_github_colinmarc_hdfs_v2-v2.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_coocood_bbloom",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coocood/bbloom",
        sha256 = "95b9a3b14d92069c4cd70942cf693db8abef720d7a38521cafb7323077e72d55",
        strip_prefix = "github.com/coocood/bbloom@v0.0.0-20190830030839-58deb6228d64",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coocood/bbloom/com_github_coocood_bbloom-v0.0.0-20190830030839-58deb6228d64.zip",
            "http://ats.apps.svc/gomod/github.com/coocood/bbloom/com_github_coocood_bbloom-v0.0.0-20190830030839-58deb6228d64.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coocood/bbloom/com_github_coocood_bbloom-v0.0.0-20190830030839-58deb6228d64.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coocood/bbloom/com_github_coocood_bbloom-v0.0.0-20190830030839-58deb6228d64.zip",
        ],
    )
    go_repository(
        name = "com_github_coocood_freecache",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coocood/freecache",
        sha256 = "e0f3b9924ea5919fbae2043680d6e6ae6bac8e9765159aa9ba2a67a4b8dd43ca",
        strip_prefix = "github.com/coocood/freecache@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coocood/freecache/com_github_coocood_freecache-v1.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/coocood/freecache/com_github_coocood_freecache-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coocood/freecache/com_github_coocood_freecache-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coocood/freecache/com_github_coocood_freecache-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_coocood_rtutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coocood/rtutil",
        sha256 = "0a48ef669128ba717cc35afc270aa74d93cbb9837ed007e7d00344d4daeb2699",
        strip_prefix = "github.com/coocood/rtutil@v0.0.0-20190304133409-c84515f646f2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coocood/rtutil/com_github_coocood_rtutil-v0.0.0-20190304133409-c84515f646f2.zip",
            "http://ats.apps.svc/gomod/github.com/coocood/rtutil/com_github_coocood_rtutil-v0.0.0-20190304133409-c84515f646f2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coocood/rtutil/com_github_coocood_rtutil-v0.0.0-20190304133409-c84515f646f2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coocood/rtutil/com_github_coocood_rtutil-v0.0.0-20190304133409-c84515f646f2.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_etcd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/etcd",
        sha256 = "6d4f268491a5e80078b3f80a94a8780c3c04bad50efb371ef10bbc80652ec122",
        strip_prefix = "github.com/coreos/etcd@v3.3.10+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/etcd/com_github_coreos_etcd-v3.3.10+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/etcd/com_github_coreos_etcd-v3.3.10+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/etcd/com_github_coreos_etcd-v3.3.10+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/etcd/com_github_coreos_etcd-v3.3.10+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_etcd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-etcd",
        sha256 = "4b226732835b9298af65db5d075024a5971aa11ef4b456899a3830bccd435b07",
        strip_prefix = "github.com/coreos/go-etcd@v2.0.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/go-etcd/com_github_coreos_go_etcd-v2.0.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/go-etcd/com_github_coreos_go_etcd-v2.0.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/go-etcd/com_github_coreos_go_etcd-v2.0.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/go-etcd/com_github_coreos_go_etcd-v2.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_semver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-semver",
        sha256 = "e72820542b5913afe0a52e956e0b3834e9fbb080641fed183117f862fab74e8a",
        strip_prefix = "github.com/coreos/go-semver@v0.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/go-semver/com_github_coreos_go_semver-v0.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/go-semver/com_github_coreos_go_semver-v0.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/go-semver/com_github_coreos_go_semver-v0.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/go-semver/com_github_coreos_go_semver-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_systemd_v22",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-systemd/v22",
        sha256 = "4c44e3a6b84de4db393e341537c7124031fa98d5f98860ad31b32b4890f2234c",
        strip_prefix = "github.com/coreos/go-systemd/v22@v22.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/go-systemd/v22/com_github_coreos_go_systemd_v22-v22.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/go-systemd/v22/com_github_coreos_go_systemd_v22-v22.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/go-systemd/v22/com_github_coreos_go_systemd_v22-v22.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/go-systemd/v22/com_github_coreos_go_systemd_v22-v22.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cpuguy83_go_md2man",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cpuguy83/go-md2man",
        sha256 = "b9b153bb97e2a702ec5c41f6815985d4295524cdf4f2a9e5633f98e9739f4d6e",
        strip_prefix = "github.com/cpuguy83/go-md2man@v1.0.10",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cpuguy83/go-md2man/com_github_cpuguy83_go_md2man-v1.0.10.zip",
            "http://ats.apps.svc/gomod/github.com/cpuguy83/go-md2man/com_github_cpuguy83_go_md2man-v1.0.10.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cpuguy83/go-md2man/com_github_cpuguy83_go_md2man-v1.0.10.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cpuguy83/go-md2man/com_github_cpuguy83_go_md2man-v1.0.10.zip",
        ],
    )
    go_repository(
        name = "com_github_cpuguy83_go_md2man_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cpuguy83/go-md2man/v2",
        sha256 = "aa86a286ada95599a9c8e297623d12c4d4eb6ec6334c79d6dc8b3353a748f10d",
        strip_prefix = "github.com/cpuguy83/go-md2man/v2@v2.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cpuguy83/go-md2man/v2/com_github_cpuguy83_go_md2man_v2-v2.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/cpuguy83/go-md2man/v2/com_github_cpuguy83_go_md2man_v2-v2.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cpuguy83/go-md2man/v2/com_github_cpuguy83_go_md2man_v2-v2.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cpuguy83/go-md2man/v2/com_github_cpuguy83_go_md2man_v2-v2.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_creack_pty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/creack/pty",
        sha256 = "d6594fd4844c242a5c7d6e9b25516182460cffa820e47e8ffb8eea625991986c",
        strip_prefix = "github.com/creack/pty@v1.1.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/creack/pty/com_github_creack_pty-v1.1.11.zip",
            "http://ats.apps.svc/gomod/github.com/creack/pty/com_github_creack_pty-v1.1.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/creack/pty/com_github_creack_pty-v1.1.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/creack/pty/com_github_creack_pty-v1.1.11.zip",
        ],
    )
    go_repository(
        name = "com_github_curioswitch_go_reassign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/curioswitch/go-reassign",
        sha256 = "a64c6823d2b8b21c31b8cc32168c7fe9687a2b8b870e6f8acdcd299a865259ae",
        strip_prefix = "github.com/curioswitch/go-reassign@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/curioswitch/go-reassign/com_github_curioswitch_go_reassign-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/curioswitch/go-reassign/com_github_curioswitch_go_reassign-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/curioswitch/go-reassign/com_github_curioswitch_go_reassign-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/curioswitch/go-reassign/com_github_curioswitch_go_reassign-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cznic_mathutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cznic/mathutil",
        sha256 = "8f69a36f60d885e011b0a90b91246a7e88223cb2883dc6e71eab3f42d653231b",
        strip_prefix = "github.com/cznic/mathutil@v0.0.0-20181122101859-297441e03548",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cznic/mathutil/com_github_cznic_mathutil-v0.0.0-20181122101859-297441e03548.zip",
            "http://ats.apps.svc/gomod/github.com/cznic/mathutil/com_github_cznic_mathutil-v0.0.0-20181122101859-297441e03548.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cznic/mathutil/com_github_cznic_mathutil-v0.0.0-20181122101859-297441e03548.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cznic/mathutil/com_github_cznic_mathutil-v0.0.0-20181122101859-297441e03548.zip",
        ],
    )
    go_repository(
        name = "com_github_cznic_sortutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cznic/sortutil",
        sha256 = "67783879c1ae4472fdabb377b1772e4e4c5ced181528c2fc4569b565cb47a57b",
        strip_prefix = "github.com/cznic/sortutil@v0.0.0-20181122101858-f5f958428db8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cznic/sortutil/com_github_cznic_sortutil-v0.0.0-20181122101858-f5f958428db8.zip",
            "http://ats.apps.svc/gomod/github.com/cznic/sortutil/com_github_cznic_sortutil-v0.0.0-20181122101858-f5f958428db8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cznic/sortutil/com_github_cznic_sortutil-v0.0.0-20181122101858-f5f958428db8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cznic/sortutil/com_github_cznic_sortutil-v0.0.0-20181122101858-f5f958428db8.zip",
        ],
    )
    go_repository(
        name = "com_github_cznic_strutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cznic/strutil",
        sha256 = "867902276444cbffca84d9d5f63754e8b22092d93a94480d8dfebd234ac8ffbd",
        strip_prefix = "github.com/cznic/strutil@v0.0.0-20181122101858-275e90344537",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cznic/strutil/com_github_cznic_strutil-v0.0.0-20181122101858-275e90344537.zip",
            "http://ats.apps.svc/gomod/github.com/cznic/strutil/com_github_cznic_strutil-v0.0.0-20181122101858-275e90344537.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cznic/strutil/com_github_cznic_strutil-v0.0.0-20181122101858-275e90344537.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cznic/strutil/com_github_cznic_strutil-v0.0.0-20181122101858-275e90344537.zip",
        ],
    )
    go_repository(
        name = "com_github_daixiang0_gci",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/daixiang0/gci",
        sha256 = "12f86e960c7bb59b10ea07d6c7bb1b977d25fc83d5fa1b58ff7d463744ae1dfe",
        strip_prefix = "github.com/daixiang0/gci@v0.12.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/daixiang0/gci/com_github_daixiang0_gci-v0.12.1.zip",
            "http://ats.apps.svc/gomod/github.com/daixiang0/gci/com_github_daixiang0_gci-v0.12.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/daixiang0/gci/com_github_daixiang0_gci-v0.12.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/daixiang0/gci/com_github_daixiang0_gci-v0.12.1.zip",
        ],
    )
    go_repository(
        name = "com_github_danjacques_gofslock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/danjacques/gofslock",
        sha256 = "a5883b567196955c9b588bcfa8f21bf841e9234225c8437a0b84104ecc4a3b19",
        strip_prefix = "github.com/danjacques/gofslock@v0.0.0-20191023191349-0a45f885bc37",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/danjacques/gofslock/com_github_danjacques_gofslock-v0.0.0-20191023191349-0a45f885bc37.zip",
            "http://ats.apps.svc/gomod/github.com/danjacques/gofslock/com_github_danjacques_gofslock-v0.0.0-20191023191349-0a45f885bc37.zip",
            "https://cache.hawkingrei.com/gomod/github.com/danjacques/gofslock/com_github_danjacques_gofslock-v0.0.0-20191023191349-0a45f885bc37.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/danjacques/gofslock/com_github_danjacques_gofslock-v0.0.0-20191023191349-0a45f885bc37.zip",
        ],
    )
    go_repository(
        name = "com_github_data_dog_go_sqlmock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/DATA-DOG/go-sqlmock",
        sha256 = "25720bfcbd739305238408ab54263224b69ff6934923dfd9caed76d3871d0151",
        strip_prefix = "github.com/DATA-DOG/go-sqlmock@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/DATA-DOG/go-sqlmock/com_github_data_dog_go_sqlmock-v1.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/DATA-DOG/go-sqlmock/com_github_data_dog_go_sqlmock-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/DATA-DOG/go-sqlmock/com_github_data_dog_go_sqlmock-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/DATA-DOG/go-sqlmock/com_github_data_dog_go_sqlmock-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_datadog_zstd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/DataDog/zstd",
        sha256 = "0a4ae14405084422c64e0bf5372e85f5975601438c41e6d6786f03fdf9223e62",
        strip_prefix = "github.com/DataDog/zstd@v1.4.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/DataDog/zstd/com_github_datadog_zstd-v1.4.5.zip",
            "http://ats.apps.svc/gomod/github.com/DataDog/zstd/com_github_datadog_zstd-v1.4.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/DataDog/zstd/com_github_datadog_zstd-v1.4.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/DataDog/zstd/com_github_datadog_zstd-v1.4.5.zip",
        ],
    )
    go_repository(
        name = "com_github_davecgh_go_spew",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/davecgh/go-spew",
        sha256 = "b4d0923b169b194f0016ec46f3df1ab0c68e27999743e43fe2de59ecb2484128",
        strip_prefix = "github.com/davecgh/go-spew@v1.1.2-0.20180830191138-d8f796af33cc",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/davecgh/go-spew/com_github_davecgh_go_spew-v1.1.2-0.20180830191138-d8f796af33cc.zip",
            "http://ats.apps.svc/gomod/github.com/davecgh/go-spew/com_github_davecgh_go_spew-v1.1.2-0.20180830191138-d8f796af33cc.zip",
            "https://cache.hawkingrei.com/gomod/github.com/davecgh/go-spew/com_github_davecgh_go_spew-v1.1.2-0.20180830191138-d8f796af33cc.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/davecgh/go-spew/com_github_davecgh_go_spew-v1.1.2-0.20180830191138-d8f796af33cc.zip",
        ],
    )
    go_repository(
        name = "com_github_decred_dcrd_crypto_blake256",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/decred/dcrd/crypto/blake256",
        sha256 = "e4343d55494a93eb7bb7b59be9359fb8007fd36652b27a725db024f61605d515",
        strip_prefix = "github.com/decred/dcrd/crypto/blake256@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/decred/dcrd/crypto/blake256/com_github_decred_dcrd_crypto_blake256-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/decred/dcrd/crypto/blake256/com_github_decred_dcrd_crypto_blake256-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/decred/dcrd/crypto/blake256/com_github_decred_dcrd_crypto_blake256-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/decred/dcrd/crypto/blake256/com_github_decred_dcrd_crypto_blake256-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_decred_dcrd_dcrec_secp256k1_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/decred/dcrd/dcrec/secp256k1/v4",
        sha256 = "9b3594cedab7e820108cd9f2f7f17a9edf60345baf91f7e1bd298413dba44c63",
        strip_prefix = "github.com/decred/dcrd/dcrec/secp256k1/v4@v4.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/decred/dcrd/dcrec/secp256k1/v4/com_github_decred_dcrd_dcrec_secp256k1_v4-v4.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/decred/dcrd/dcrec/secp256k1/v4/com_github_decred_dcrd_dcrec_secp256k1_v4-v4.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/decred/dcrd/dcrec/secp256k1/v4/com_github_decred_dcrd_dcrec_secp256k1_v4-v4.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/decred/dcrd/dcrec/secp256k1/v4/com_github_decred_dcrd_dcrec_secp256k1_v4-v4.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_denis_tingaikin_go_header",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/denis-tingaikin/go-header",
        sha256 = "423e8cbd0166a082695e12cadb136db6ff89011c7078be482117685f867d86e8",
        strip_prefix = "github.com/denis-tingaikin/go-header@v0.4.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/denis-tingaikin/go-header/com_github_denis_tingaikin_go_header-v0.4.3.zip",
            "http://ats.apps.svc/gomod/github.com/denis-tingaikin/go-header/com_github_denis_tingaikin_go_header-v0.4.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/denis-tingaikin/go-header/com_github_denis_tingaikin_go_header-v0.4.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/denis-tingaikin/go-header/com_github_denis_tingaikin_go_header-v0.4.3.zip",
        ],
    )
    go_repository(
        name = "com_github_dennwc_varint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dennwc/varint",
        sha256 = "2918e66c0fb5a82dbfc8cca1ed34cb8ccff8188e876c0ca25f85b8247e53626f",
        strip_prefix = "github.com/dennwc/varint@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dennwc/varint/com_github_dennwc_varint-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/dennwc/varint/com_github_dennwc_varint-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dennwc/varint/com_github_dennwc_varint-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dennwc/varint/com_github_dennwc_varint-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dgraph_io_badger",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgraph-io/badger",
        sha256 = "8329ae390aebec6ae360356e77a2743357ad4e0d0bd4c3ae03b7d17e01ad70aa",
        strip_prefix = "github.com/dgraph-io/badger@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dgraph-io/badger/com_github_dgraph_io_badger-v1.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/dgraph-io/badger/com_github_dgraph_io_badger-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dgraph-io/badger/com_github_dgraph_io_badger-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dgraph-io/badger/com_github_dgraph_io_badger-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dgraph_io_ristretto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgraph-io/ristretto",
        sha256 = "fe7bd94580481fd4a25a72becb8b30c60142492a3e83320e1bbc4262baa533da",
        strip_prefix = "github.com/dgraph-io/ristretto@v0.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dgraph-io/ristretto/com_github_dgraph_io_ristretto-v0.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/dgraph-io/ristretto/com_github_dgraph_io_ristretto-v0.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dgraph-io/ristretto/com_github_dgraph_io_ristretto-v0.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dgraph-io/ristretto/com_github_dgraph_io_ristretto-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_dgrijalva_jwt_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgrijalva/jwt-go",
        sha256 = "ebe8386761761d53fac2de5f8f575ddf66c114ec9835947c761131662f1d38f3",
        strip_prefix = "github.com/form3tech-oss/jwt-go@v3.2.6-0.20210809144907-32ab6a8243d7+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/form3tech-oss/jwt-go/com_github_form3tech_oss_jwt_go-v3.2.6-0.20210809144907-32ab6a8243d7+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/form3tech-oss/jwt-go/com_github_form3tech_oss_jwt_go-v3.2.6-0.20210809144907-32ab6a8243d7+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/form3tech-oss/jwt-go/com_github_form3tech_oss_jwt_go-v3.2.6-0.20210809144907-32ab6a8243d7+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/form3tech-oss/jwt-go/com_github_form3tech_oss_jwt_go-v3.2.6-0.20210809144907-32ab6a8243d7+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_dgryski_go_farm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgryski/go-farm",
        sha256 = "bdf602cab00a24c2898aabad0b40c7b1d76a29cf8dd3319ef87046a5f4b1726f",
        strip_prefix = "github.com/dgryski/go-farm@v0.0.0-20200201041132-a6ae2369ad13",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dgryski/go-farm/com_github_dgryski_go_farm-v0.0.0-20200201041132-a6ae2369ad13.zip",
            "http://ats.apps.svc/gomod/github.com/dgryski/go-farm/com_github_dgryski_go_farm-v0.0.0-20200201041132-a6ae2369ad13.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dgryski/go-farm/com_github_dgryski_go_farm-v0.0.0-20200201041132-a6ae2369ad13.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dgryski/go-farm/com_github_dgryski_go_farm-v0.0.0-20200201041132-a6ae2369ad13.zip",
        ],
    )
    go_repository(
        name = "com_github_digitalocean_godo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/digitalocean/godo",
        sha256 = "cb363a73dc6524f61a8c349c9071470d1f709b0911f5faccbf6073da73bb9393",
        strip_prefix = "github.com/digitalocean/godo@v1.106.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/digitalocean/godo/com_github_digitalocean_godo-v1.106.0.zip",
            "http://ats.apps.svc/gomod/github.com/digitalocean/godo/com_github_digitalocean_godo-v1.106.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/digitalocean/godo/com_github_digitalocean_godo-v1.106.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/digitalocean/godo/com_github_digitalocean_godo-v1.106.0.zip",
        ],
    )
    go_repository(
        name = "com_github_djarvur_go_err113",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Djarvur/go-err113",
        sha256 = "f2c6d8ae044f430048ae675330d2adcbe1927a8a369549d98c4d1e62608b582a",
        strip_prefix = "github.com/Djarvur/go-err113@v0.0.0-20210108212216-aea10b59be24",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Djarvur/go-err113/com_github_djarvur_go_err113-v0.0.0-20210108212216-aea10b59be24.zip",
            "http://ats.apps.svc/gomod/github.com/Djarvur/go-err113/com_github_djarvur_go_err113-v0.0.0-20210108212216-aea10b59be24.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Djarvur/go-err113/com_github_djarvur_go_err113-v0.0.0-20210108212216-aea10b59be24.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Djarvur/go-err113/com_github_djarvur_go_err113-v0.0.0-20210108212216-aea10b59be24.zip",
        ],
    )
    go_repository(
        name = "com_github_dnaeon_go_vcr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dnaeon/go-vcr",
        sha256 = "6d34b7e17c158d51ffc34f6ac64df05ab736b2ae50c0db07be4a9556dac10c52",
        strip_prefix = "github.com/dnaeon/go-vcr@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dnaeon/go-vcr/com_github_dnaeon_go_vcr-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/dnaeon/go-vcr/com_github_dnaeon_go_vcr-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dnaeon/go-vcr/com_github_dnaeon_go_vcr-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dnaeon/go-vcr/com_github_dnaeon_go_vcr-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_distribution",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/distribution",
        sha256 = "9e0a17bbcaa1419232cd44e3a79209be26d9ccfa079e32e0e9999c81c0991477",
        strip_prefix = "github.com/docker/distribution@v2.8.2+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/docker/distribution/com_github_docker_distribution-v2.8.2+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/docker/distribution/com_github_docker_distribution-v2.8.2+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/docker/distribution/com_github_docker_distribution-v2.8.2+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/docker/distribution/com_github_docker_distribution-v2.8.2+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_docker",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/docker",
        sha256 = "a9bf4b53188caf41ada5a5490d99c44e808d2f4a725028e2717d9b31c87f5002",
        strip_prefix = "github.com/docker/docker@v24.0.7+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/docker/docker/com_github_docker_docker-v24.0.7+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/docker/docker/com_github_docker_docker-v24.0.7+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/docker/docker/com_github_docker_docker-v24.0.7+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/docker/docker/com_github_docker_docker-v24.0.7+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_go_connections",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/go-connections",
        sha256 = "570ebcee7e6fd844e00c89eeab2b1922081d6969df76078dfe4ffacd3db56ada",
        strip_prefix = "github.com/docker/go-connections@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/docker/go-connections/com_github_docker_go_connections-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/docker/go-connections/com_github_docker_go_connections-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/docker/go-connections/com_github_docker_go_connections-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/docker/go-connections/com_github_docker_go_connections-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_go_units",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/go-units",
        sha256 = "039d53ebe64af1aefa0be94ce42c621a17a3052c58ad15e5b3f357529beeaff6",
        strip_prefix = "github.com/docker/go-units@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/docker/go-units/com_github_docker_go_units-v0.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/docker/go-units/com_github_docker_go_units-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/docker/go-units/com_github_docker_go_units-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/docker/go-units/com_github_docker_go_units-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dolthub_maphash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dolthub/maphash",
        sha256 = "ba69ef526a9613cb059c8490c1a4f032649879c316a1c4305e2355815eb32e41",
        strip_prefix = "github.com/dolthub/maphash@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dolthub/maphash/com_github_dolthub_maphash-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/dolthub/maphash/com_github_dolthub_maphash-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dolthub/maphash/com_github_dolthub_maphash-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dolthub/maphash/com_github_dolthub_maphash-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dolthub_swiss",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dolthub/swiss",
        sha256 = "e911b7cea9aaed1255544fb8b53c19780f91b713e6d0fc71fb310232e4800dcc",
        strip_prefix = "github.com/dolthub/swiss@v0.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dolthub/swiss/com_github_dolthub_swiss-v0.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/dolthub/swiss/com_github_dolthub_swiss-v0.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dolthub/swiss/com_github_dolthub_swiss-v0.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dolthub/swiss/com_github_dolthub_swiss-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_dustin_go_humanize",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dustin/go-humanize",
        sha256 = "319404ea84c8a4e2d3d83f30988b006e7dd04976de3e1a1a90484ad94679fa46",
        strip_prefix = "github.com/dustin/go-humanize@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dustin/go-humanize/com_github_dustin_go_humanize-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/dustin/go-humanize/com_github_dustin_go_humanize-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dustin/go-humanize/com_github_dustin_go_humanize-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dustin/go-humanize/com_github_dustin_go_humanize-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_eapache_go_resiliency",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eapache/go-resiliency",
        sha256 = "39333303f947a85e0c35e9969d56e05776034b1ae91e75cbf9211ead5870d982",
        strip_prefix = "github.com/eapache/go-resiliency@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/eapache/go-resiliency/com_github_eapache_go_resiliency-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/eapache/go-resiliency/com_github_eapache_go_resiliency-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/eapache/go-resiliency/com_github_eapache_go_resiliency-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/eapache/go-resiliency/com_github_eapache_go_resiliency-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_eapache_go_xerial_snappy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eapache/go-xerial-snappy",
        sha256 = "785264afffdcfe50573a1cb0df85ff4186e9e7e4e3a04513752f52d3da1054af",
        strip_prefix = "github.com/eapache/go-xerial-snappy@v0.0.0-20180814174437-776d5712da21",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/eapache/go-xerial-snappy/com_github_eapache_go_xerial_snappy-v0.0.0-20180814174437-776d5712da21.zip",
            "http://ats.apps.svc/gomod/github.com/eapache/go-xerial-snappy/com_github_eapache_go_xerial_snappy-v0.0.0-20180814174437-776d5712da21.zip",
            "https://cache.hawkingrei.com/gomod/github.com/eapache/go-xerial-snappy/com_github_eapache_go_xerial_snappy-v0.0.0-20180814174437-776d5712da21.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/eapache/go-xerial-snappy/com_github_eapache_go_xerial_snappy-v0.0.0-20180814174437-776d5712da21.zip",
        ],
    )
    go_repository(
        name = "com_github_eapache_queue",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eapache/queue",
        sha256 = "1dc1b4972e8505c4763c65424b19604c65c944911d16c18c5cbd35aae45626fb",
        strip_prefix = "github.com/eapache/queue@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/eapache/queue/com_github_eapache_queue-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/eapache/queue/com_github_eapache_queue-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/eapache/queue/com_github_eapache_queue-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/eapache/queue/com_github_eapache_queue-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_edsrzf_mmap_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/edsrzf/mmap-go",
        sha256 = "1c2fa2b55d253fb95d4b253ec39348deba3d46a184bc0a4393a355807b8e5df7",
        strip_prefix = "github.com/edsrzf/mmap-go@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/edsrzf/mmap-go/com_github_edsrzf_mmap_go-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/edsrzf/mmap-go/com_github_edsrzf_mmap_go-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/edsrzf/mmap-go/com_github_edsrzf_mmap_go-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/edsrzf/mmap-go/com_github_edsrzf_mmap_go-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_eknkc_amber",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eknkc/amber",
        sha256 = "b1dde9f3713742ad0961825a2d962bd99d9390daf8596e7680dfb5f395e54e22",
        strip_prefix = "github.com/eknkc/amber@v0.0.0-20171010120322-cdade1c07385",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/eknkc/amber/com_github_eknkc_amber-v0.0.0-20171010120322-cdade1c07385.zip",
            "http://ats.apps.svc/gomod/github.com/eknkc/amber/com_github_eknkc_amber-v0.0.0-20171010120322-cdade1c07385.zip",
            "https://cache.hawkingrei.com/gomod/github.com/eknkc/amber/com_github_eknkc_amber-v0.0.0-20171010120322-cdade1c07385.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/eknkc/amber/com_github_eknkc_amber-v0.0.0-20171010120322-cdade1c07385.zip",
        ],
    )
    go_repository(
        name = "com_github_emicklei_go_restful_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/emicklei/go-restful/v3",
        sha256 = "42f1f1e5d986212ba6c7d96f6e76ba2a28b1d17fad9a40b0c45d1505d39bda26",
        strip_prefix = "github.com/emicklei/go-restful/v3@v3.10.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/emicklei/go-restful/v3/com_github_emicklei_go_restful_v3-v3.10.2.zip",
            "http://ats.apps.svc/gomod/github.com/emicklei/go-restful/v3/com_github_emicklei_go_restful_v3-v3.10.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/emicklei/go-restful/v3/com_github_emicklei_go_restful_v3-v3.10.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/emicklei/go-restful/v3/com_github_emicklei_go_restful_v3-v3.10.2.zip",
        ],
    )
    go_repository(
        name = "com_github_emirpasic_gods",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/emirpasic/gods",
        sha256 = "1d75e291ac15cf9ca2fcd8bd24e2f7203abad319cd3622cd1b19db5c4fb9daa5",
        strip_prefix = "github.com/emirpasic/gods@v1.18.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/emirpasic/gods/com_github_emirpasic_gods-v1.18.1.zip",
            "http://ats.apps.svc/gomod/github.com/emirpasic/gods/com_github_emirpasic_gods-v1.18.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/emirpasic/gods/com_github_emirpasic_gods-v1.18.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/emirpasic/gods/com_github_emirpasic_gods-v1.18.1.zip",
        ],
    )
    go_repository(
        name = "com_github_envoyproxy_go_control_plane",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/envoyproxy/go-control-plane",
        sha256 = "dc6b843aff8edab08ad7147b542a88ed8f6105ae8cad9b4c4f61acee4b784209",
        strip_prefix = "github.com/envoyproxy/go-control-plane@v0.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/envoyproxy/go-control-plane/com_github_envoyproxy_go_control_plane-v0.12.0.zip",
            "http://ats.apps.svc/gomod/github.com/envoyproxy/go-control-plane/com_github_envoyproxy_go_control_plane-v0.12.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/envoyproxy/go-control-plane/com_github_envoyproxy_go_control_plane-v0.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/envoyproxy/go-control-plane/com_github_envoyproxy_go_control_plane-v0.12.0.zip",
        ],
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sha256 = "8c7149e937f9750d7a3527396f0836aa28fcb070e067f2b99a0349c532403a03",
        strip_prefix = "github.com/envoyproxy/protoc-gen-validate@v1.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/envoyproxy/protoc-gen-validate/com_github_envoyproxy_protoc_gen_validate-v1.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/envoyproxy/protoc-gen-validate/com_github_envoyproxy_protoc_gen_validate-v1.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/envoyproxy/protoc-gen-validate/com_github_envoyproxy_protoc_gen_validate-v1.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/envoyproxy/protoc-gen-validate/com_github_envoyproxy_protoc_gen_validate-v1.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_esimonov_ifshort",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/esimonov/ifshort",
        sha256 = "1ee8321acead41b55c4b4c8d832c027aa8686dcbd6930cb4747ac8468079745a",
        strip_prefix = "github.com/esimonov/ifshort@v1.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/esimonov/ifshort/com_github_esimonov_ifshort-v1.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/esimonov/ifshort/com_github_esimonov_ifshort-v1.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/esimonov/ifshort/com_github_esimonov_ifshort-v1.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/esimonov/ifshort/com_github_esimonov_ifshort-v1.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_etcd_io_bbolt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/etcd-io/bbolt",
        sha256 = "6630d7aad4b10f76aea88ee6d9086a1edffe371651cc2432edfd0de6beb99120",
        strip_prefix = "github.com/etcd-io/bbolt@v1.3.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/etcd-io/bbolt/com_github_etcd_io_bbolt-v1.3.3.zip",
            "http://ats.apps.svc/gomod/github.com/etcd-io/bbolt/com_github_etcd_io_bbolt-v1.3.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/etcd-io/bbolt/com_github_etcd_io_bbolt-v1.3.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/etcd-io/bbolt/com_github_etcd_io_bbolt-v1.3.3.zip",
        ],
    )
    go_repository(
        name = "com_github_etcd_io_gofail",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/etcd-io/gofail",
        sha256 = "4d73950b1116d15fed5bd1c5525439e633becd9f15539c27f5aab03a95a0a901",
        strip_prefix = "github.com/etcd-io/gofail@v0.0.0-20190801230047-ad7f989257ca",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/etcd-io/gofail/com_github_etcd_io_gofail-v0.0.0-20190801230047-ad7f989257ca.zip",
            "http://ats.apps.svc/gomod/github.com/etcd-io/gofail/com_github_etcd_io_gofail-v0.0.0-20190801230047-ad7f989257ca.zip",
            "https://cache.hawkingrei.com/gomod/github.com/etcd-io/gofail/com_github_etcd_io_gofail-v0.0.0-20190801230047-ad7f989257ca.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/etcd-io/gofail/com_github_etcd_io_gofail-v0.0.0-20190801230047-ad7f989257ca.zip",
        ],
    )
    go_repository(
        name = "com_github_ettle_strcase",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ettle/strcase",
        sha256 = "3b133509880af45108baefb655666e426e2a32adf6a4a660dac3eb06749df47b",
        strip_prefix = "github.com/ettle/strcase@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ettle/strcase/com_github_ettle_strcase-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/ettle/strcase/com_github_ettle_strcase-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ettle/strcase/com_github_ettle_strcase-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ettle/strcase/com_github_ettle_strcase-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_evanphx_json_patch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/evanphx/json-patch",
        sha256 = "1105c2dc020fe36fa8ac02ad52f64c64291d9639c7108b6fc3da77299efd13f3",
        strip_prefix = "github.com/evanphx/json-patch@v5.6.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/evanphx/json-patch/com_github_evanphx_json_patch-v5.6.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/evanphx/json-patch/com_github_evanphx_json_patch-v5.6.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/evanphx/json-patch/com_github_evanphx_json_patch-v5.6.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/evanphx/json-patch/com_github_evanphx_json_patch-v5.6.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_fasthttp_contrib_websocket",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fasthttp-contrib/websocket",
        sha256 = "9d11b15b5b6c4d0508bd6afad73ec4d33a90218068ff8a8283d7ea27c22ba9af",
        strip_prefix = "github.com/fasthttp-contrib/websocket@v0.0.0-20160511215533-1f3b11f56072",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fasthttp-contrib/websocket/com_github_fasthttp_contrib_websocket-v0.0.0-20160511215533-1f3b11f56072.zip",
            "http://ats.apps.svc/gomod/github.com/fasthttp-contrib/websocket/com_github_fasthttp_contrib_websocket-v0.0.0-20160511215533-1f3b11f56072.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fasthttp-contrib/websocket/com_github_fasthttp_contrib_websocket-v0.0.0-20160511215533-1f3b11f56072.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fasthttp-contrib/websocket/com_github_fasthttp_contrib_websocket-v0.0.0-20160511215533-1f3b11f56072.zip",
        ],
    )
    go_repository(
        name = "com_github_fatanugraha_noloopclosure",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatanugraha/noloopclosure",
        sha256 = "2fdc7dfcdee917b4e224c18f743e856a631a0dfac763f4f21c9a109f7411dc1e",
        strip_prefix = "github.com/fatanugraha/noloopclosure@v0.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fatanugraha/noloopclosure/com_github_fatanugraha_noloopclosure-v0.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/fatanugraha/noloopclosure/com_github_fatanugraha_noloopclosure-v0.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fatanugraha/noloopclosure/com_github_fatanugraha_noloopclosure-v0.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fatanugraha/noloopclosure/com_github_fatanugraha_noloopclosure-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_fatih_color",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatih/color",
        sha256 = "8719f3f5443d387546316e98105b5793d9c378dbdb9f4d60728ac4477d5aeadf",
        strip_prefix = "github.com/fatih/color@v1.16.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fatih/color/com_github_fatih_color-v1.16.0.zip",
            "http://ats.apps.svc/gomod/github.com/fatih/color/com_github_fatih_color-v1.16.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fatih/color/com_github_fatih_color-v1.16.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fatih/color/com_github_fatih_color-v1.16.0.zip",
        ],
    )
    go_repository(
        name = "com_github_fatih_structs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatih/structs",
        sha256 = "a361ecc95ad12000c66ee143d26b2aa0a4e5de3b045fd5d18a52564622a59148",
        strip_prefix = "github.com/fatih/structs@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fatih/structs/com_github_fatih_structs-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/fatih/structs/com_github_fatih_structs-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fatih/structs/com_github_fatih_structs-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fatih/structs/com_github_fatih_structs-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_fatih_structtag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatih/structtag",
        sha256 = "9fe0ed2128614a3c35c4149febde484cfae8c5ecb13c128957cfcdf2776dd1eb",
        strip_prefix = "github.com/fatih/structtag@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fatih/structtag/com_github_fatih_structtag-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/fatih/structtag/com_github_fatih_structtag-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fatih/structtag/com_github_fatih_structtag-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fatih/structtag/com_github_fatih_structtag-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_felixge_fgprof",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/felixge/fgprof",
        sha256 = "2c83268087acf8b767be69dbc37c099fa85856763c2e88fb99637d46eb6ac23c",
        strip_prefix = "github.com/felixge/fgprof@v0.9.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/felixge/fgprof/com_github_felixge_fgprof-v0.9.3.zip",
            "http://ats.apps.svc/gomod/github.com/felixge/fgprof/com_github_felixge_fgprof-v0.9.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/felixge/fgprof/com_github_felixge_fgprof-v0.9.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/felixge/fgprof/com_github_felixge_fgprof-v0.9.3.zip",
        ],
    )
    go_repository(
        name = "com_github_felixge_httpsnoop",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/felixge/httpsnoop",
        sha256 = "75aa471311265e9860df0e523400b4650ed0c1a33262786a421f07226792e494",
        strip_prefix = "github.com/felixge/httpsnoop@v1.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/felixge/httpsnoop/com_github_felixge_httpsnoop-v1.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/felixge/httpsnoop/com_github_felixge_httpsnoop-v1.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/felixge/httpsnoop/com_github_felixge_httpsnoop-v1.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/felixge/httpsnoop/com_github_felixge_httpsnoop-v1.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_firefart_nonamedreturns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/firefart/nonamedreturns",
        sha256 = "293f84c4e1737d2558e1d289f9ca6f7ca851276fb204bee9a21664da4ddd9cac",
        strip_prefix = "github.com/firefart/nonamedreturns@v1.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/firefart/nonamedreturns/com_github_firefart_nonamedreturns-v1.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/firefart/nonamedreturns/com_github_firefart_nonamedreturns-v1.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/firefart/nonamedreturns/com_github_firefart_nonamedreturns-v1.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/firefart/nonamedreturns/com_github_firefart_nonamedreturns-v1.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_flosch_pongo2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/flosch/pongo2",
        sha256 = "814b52f668d2e2528fe9af917506cda4894d22c927283cfb8aaf6857503dfc5a",
        strip_prefix = "github.com/flosch/pongo2@v0.0.0-20190707114632-bbf5a6c351f4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/flosch/pongo2/com_github_flosch_pongo2-v0.0.0-20190707114632-bbf5a6c351f4.zip",
            "http://ats.apps.svc/gomod/github.com/flosch/pongo2/com_github_flosch_pongo2-v0.0.0-20190707114632-bbf5a6c351f4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/flosch/pongo2/com_github_flosch_pongo2-v0.0.0-20190707114632-bbf5a6c351f4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/flosch/pongo2/com_github_flosch_pongo2-v0.0.0-20190707114632-bbf5a6c351f4.zip",
        ],
    )
    go_repository(
        name = "com_github_fogleman_gg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fogleman/gg",
        sha256 = "75b657490d88ac3bad9af07ec4acfe57a995944c50eeb1f167467cf82ff814c5",
        strip_prefix = "github.com/fogleman/gg@v1.2.1-0.20190220221249-0403632d5b90",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fogleman/gg/com_github_fogleman_gg-v1.2.1-0.20190220221249-0403632d5b90.zip",
            "http://ats.apps.svc/gomod/github.com/fogleman/gg/com_github_fogleman_gg-v1.2.1-0.20190220221249-0403632d5b90.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fogleman/gg/com_github_fogleman_gg-v1.2.1-0.20190220221249-0403632d5b90.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fogleman/gg/com_github_fogleman_gg-v1.2.1-0.20190220221249-0403632d5b90.zip",
        ],
    )
    go_repository(
        name = "com_github_fortytw2_leaktest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fortytw2/leaktest",
        sha256 = "867e6d131510751ba6055c51e7746b0056a6b3dcb1a1b2dfdc694251cd7eb8b3",
        strip_prefix = "github.com/fortytw2/leaktest@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fortytw2/leaktest/com_github_fortytw2_leaktest-v1.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/fortytw2/leaktest/com_github_fortytw2_leaktest-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fortytw2/leaktest/com_github_fortytw2_leaktest-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fortytw2/leaktest/com_github_fortytw2_leaktest-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_frankban_quicktest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/frankban/quicktest",
        sha256 = "35014be7acc79de33c58785d9372f48702556bf35fd89067c3ecbedf49c2e987",
        strip_prefix = "github.com/frankban/quicktest@v1.14.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/frankban/quicktest/com_github_frankban_quicktest-v1.14.3.zip",
            "http://ats.apps.svc/gomod/github.com/frankban/quicktest/com_github_frankban_quicktest-v1.14.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/frankban/quicktest/com_github_frankban_quicktest-v1.14.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/frankban/quicktest/com_github_frankban_quicktest-v1.14.3.zip",
        ],
    )
    go_repository(
        name = "com_github_fsnotify_fsnotify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fsnotify/fsnotify",
        sha256 = "f98f08a95224f2c7a77b62aa4840cefe4970f0ff00e0a027d7e457c3df752bb2",
        strip_prefix = "github.com/fsnotify/fsnotify@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fsnotify/fsnotify/com_github_fsnotify_fsnotify-v1.7.0.zip",
            "http://ats.apps.svc/gomod/github.com/fsnotify/fsnotify/com_github_fsnotify_fsnotify-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fsnotify/fsnotify/com_github_fsnotify_fsnotify-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fsnotify/fsnotify/com_github_fsnotify_fsnotify-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_fsouza_fake_gcs_server",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fsouza/fake-gcs-server",
        sha256 = "bd819fcc7642b82cc0f1cec60ad809208b8410d12f09fc442d16bb05eb7a7ffe",
        strip_prefix = "github.com/fsouza/fake-gcs-server@v1.44.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fsouza/fake-gcs-server/com_github_fsouza_fake_gcs_server-v1.44.0.zip",
            "http://ats.apps.svc/gomod/github.com/fsouza/fake-gcs-server/com_github_fsouza_fake_gcs_server-v1.44.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fsouza/fake-gcs-server/com_github_fsouza_fake_gcs_server-v1.44.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fsouza/fake-gcs-server/com_github_fsouza_fake_gcs_server-v1.44.0.zip",
        ],
    )
    go_repository(
        name = "com_github_fzipp_gocyclo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fzipp/gocyclo",
        sha256 = "91d60eb91f3a309711e46d44478293a558feb9657c6a043f0b404491c8afa8c1",
        strip_prefix = "github.com/fzipp/gocyclo@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fzipp/gocyclo/com_github_fzipp_gocyclo-v0.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/fzipp/gocyclo/com_github_fzipp_gocyclo-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fzipp/gocyclo/com_github_fzipp_gocyclo-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fzipp/gocyclo/com_github_fzipp_gocyclo-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gaijinentertainment_go_exhaustruct_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/GaijinEntertainment/go-exhaustruct/v3",
        sha256 = "ea73dfe7281f82b1fb955e087a865d9def560e7c149258ab251517271b1cb15f",
        strip_prefix = "github.com/GaijinEntertainment/go-exhaustruct/v3@v3.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/GaijinEntertainment/go-exhaustruct/v3/com_github_gaijinentertainment_go_exhaustruct_v3-v3.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/GaijinEntertainment/go-exhaustruct/v3/com_github_gaijinentertainment_go_exhaustruct_v3-v3.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/GaijinEntertainment/go-exhaustruct/v3/com_github_gaijinentertainment_go_exhaustruct_v3-v3.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/GaijinEntertainment/go-exhaustruct/v3/com_github_gaijinentertainment_go_exhaustruct_v3-v3.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gavv_httpexpect",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gavv/httpexpect",
        sha256 = "3db05c59a5c70d11b9452727c529be6934ddf8b42f4bfdc3138441055f1529b1",
        strip_prefix = "github.com/gavv/httpexpect@v2.0.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gavv/httpexpect/com_github_gavv_httpexpect-v2.0.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/gavv/httpexpect/com_github_gavv_httpexpect-v2.0.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gavv/httpexpect/com_github_gavv_httpexpect-v2.0.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gavv/httpexpect/com_github_gavv_httpexpect-v2.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_ghemawat_stream",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ghemawat/stream",
        sha256 = "9c0a42cacc8e22024b58db15127886a6f8ddbcfbf89d4d062bfdc43dc40d80d5",
        strip_prefix = "github.com/ghemawat/stream@v0.0.0-20171120220530-696b145b53b9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ghemawat/stream/com_github_ghemawat_stream-v0.0.0-20171120220530-696b145b53b9.zip",
            "http://ats.apps.svc/gomod/github.com/ghemawat/stream/com_github_ghemawat_stream-v0.0.0-20171120220530-696b145b53b9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ghemawat/stream/com_github_ghemawat_stream-v0.0.0-20171120220530-696b145b53b9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ghemawat/stream/com_github_ghemawat_stream-v0.0.0-20171120220530-696b145b53b9.zip",
        ],
    )
    go_repository(
        name = "com_github_ghodss_yaml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ghodss/yaml",
        sha256 = "c3f295d23c02c0b35e4d3b29053586e737cf9642df9615da99c0bda9bbacc624",
        strip_prefix = "github.com/ghodss/yaml@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ghodss/yaml/com_github_ghodss_yaml-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/ghodss/yaml/com_github_ghodss_yaml-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ghodss/yaml/com_github_ghodss_yaml-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ghodss/yaml/com_github_ghodss_yaml-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ghostiam_protogetter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ghostiam/protogetter",
        sha256 = "2e12dd7163387f2067544455f0288d931d2ba1e215fc38814028318895592dc5",
        strip_prefix = "github.com/ghostiam/protogetter@v0.3.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ghostiam/protogetter/com_github_ghostiam_protogetter-v0.3.4.zip",
            "http://ats.apps.svc/gomod/github.com/ghostiam/protogetter/com_github_ghostiam_protogetter-v0.3.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ghostiam/protogetter/com_github_ghostiam_protogetter-v0.3.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ghostiam/protogetter/com_github_ghostiam_protogetter-v0.3.4.zip",
        ],
    )
    go_repository(
        name = "com_github_gin_contrib_sse",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gin-contrib/sse",
        sha256 = "6acbc2849280488083f04df1114d260f91c6f675a501e008fb2daafa6c4da131",
        strip_prefix = "github.com/gin-contrib/sse@v0.0.0-20190301062529-5545eab6dad3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gin-contrib/sse/com_github_gin_contrib_sse-v0.0.0-20190301062529-5545eab6dad3.zip",
            "http://ats.apps.svc/gomod/github.com/gin-contrib/sse/com_github_gin_contrib_sse-v0.0.0-20190301062529-5545eab6dad3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gin-contrib/sse/com_github_gin_contrib_sse-v0.0.0-20190301062529-5545eab6dad3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gin-contrib/sse/com_github_gin_contrib_sse-v0.0.0-20190301062529-5545eab6dad3.zip",
        ],
    )
    go_repository(
        name = "com_github_gin_gonic_gin",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gin-gonic/gin",
        sha256 = "b9bc661bf658179d53fee9e7c587eba4df8326d0c26ad29f785739a78313fc4b",
        strip_prefix = "github.com/gin-gonic/gin@v1.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gin-gonic/gin/com_github_gin_gonic_gin-v1.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/gin-gonic/gin/com_github_gin_gonic_gin-v1.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gin-gonic/gin/com_github_gin_gonic_gin-v1.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gin-gonic/gin/com_github_gin_gonic_gin-v1.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_asn1_ber_asn1_ber",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-asn1-ber/asn1-ber",
        sha256 = "d0da40d84005074ccdcf352651f64f87a3525ac3bc0ff796139db9e08d1d0dd1",
        strip_prefix = "github.com/go-asn1-ber/asn1-ber@v1.5.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-asn1-ber/asn1-ber/com_github_go_asn1_ber_asn1_ber-v1.5.4.zip",
            "http://ats.apps.svc/gomod/github.com/go-asn1-ber/asn1-ber/com_github_go_asn1_ber_asn1_ber-v1.5.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-asn1-ber/asn1-ber/com_github_go_asn1_ber_asn1_ber-v1.5.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-asn1-ber/asn1-ber/com_github_go_asn1_ber_asn1_ber-v1.5.4.zip",
        ],
    )
    go_repository(
        name = "com_github_go_check_check",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-check/check",
        sha256 = "55ed8316526c1ba82e3e607d17aa98f3b8b0a139ca9c224ee2a3e9e1b582608e",
        strip_prefix = "github.com/go-check/check@v0.0.0-20180628173108-788fd7840127",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-check/check/com_github_go_check_check-v0.0.0-20180628173108-788fd7840127.zip",
            "http://ats.apps.svc/gomod/github.com/go-check/check/com_github_go_check_check-v0.0.0-20180628173108-788fd7840127.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-check/check/com_github_go_check_check-v0.0.0-20180628173108-788fd7840127.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-check/check/com_github_go_check_check-v0.0.0-20180628173108-788fd7840127.zip",
        ],
    )
    go_repository(
        name = "com_github_go_critic_go_critic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-critic/go-critic",
        sha256 = "e700c265717d422f92b60992f4cb9bd6e7df1f425fa4a233efb589ad9674fec9",
        strip_prefix = "github.com/go-critic/go-critic@v0.11.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-critic/go-critic/com_github_go_critic_go_critic-v0.11.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-critic/go-critic/com_github_go_critic_go_critic-v0.11.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-critic/go-critic/com_github_go_critic_go_critic-v0.11.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-critic/go-critic/com_github_go_critic_go_critic-v0.11.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_errors_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-errors/errors",
        sha256 = "bdbee3143e1798eadff4df919479c28ec2d3299a97d445917bc64d6eb6a3b95a",
        strip_prefix = "github.com/go-errors/errors@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-errors/errors/com_github_go_errors_errors-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-errors/errors/com_github_go_errors_errors-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-errors/errors/com_github_go_errors_errors-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-errors/errors/com_github_go_errors_errors-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_gl_glfw",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-gl/glfw",
        sha256 = "96c694c42e7b866ea8e26dc48b612c4daa8582ce61fdeefbe92c1a4c46163169",
        strip_prefix = "github.com/go-gl/glfw@v0.0.0-20190409004039-e6da0acd62b1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-gl/glfw/com_github_go_gl_glfw-v0.0.0-20190409004039-e6da0acd62b1.zip",
            "http://ats.apps.svc/gomod/github.com/go-gl/glfw/com_github_go_gl_glfw-v0.0.0-20190409004039-e6da0acd62b1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-gl/glfw/com_github_go_gl_glfw-v0.0.0-20190409004039-e6da0acd62b1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-gl/glfw/com_github_go_gl_glfw-v0.0.0-20190409004039-e6da0acd62b1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_gl_glfw_v3_3_glfw",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-gl/glfw/v3.3/glfw",
        sha256 = "2f6a1963397cb7c3df66257a45d75fae860aa9b9eec17825d8101c1e1313da5b",
        strip_prefix = "github.com/go-gl/glfw/v3.3/glfw@v0.0.0-20200222043503-6f7a984d4dc4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-gl/glfw/v3.3/glfw/com_github_go_gl_glfw_v3_3_glfw-v0.0.0-20200222043503-6f7a984d4dc4.zip",
            "http://ats.apps.svc/gomod/github.com/go-gl/glfw/v3.3/glfw/com_github_go_gl_glfw_v3_3_glfw-v0.0.0-20200222043503-6f7a984d4dc4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-gl/glfw/v3.3/glfw/com_github_go_gl_glfw_v3_3_glfw-v0.0.0-20200222043503-6f7a984d4dc4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-gl/glfw/v3.3/glfw/com_github_go_gl_glfw_v3_3_glfw-v0.0.0-20200222043503-6f7a984d4dc4.zip",
        ],
    )
    go_repository(
        name = "com_github_go_kit_kit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-kit/kit",
        sha256 = "2006e7fbfba4273d29042661e2c13749105ac430d85f06175359b520371e6c5a",
        strip_prefix = "github.com/go-kit/kit@v0.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-kit/kit/com_github_go_kit_kit-v0.12.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-kit/kit/com_github_go_kit_kit-v0.12.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-kit/kit/com_github_go_kit_kit-v0.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-kit/kit/com_github_go_kit_kit-v0.12.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_kit_log",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-kit/log",
        sha256 = "52634b502b9d0aa945833d93582cffc1bdd9bfa39810e7c70d0688e330b75198",
        strip_prefix = "github.com/go-kit/log@v0.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-kit/log/com_github_go_kit_log-v0.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-kit/log/com_github_go_kit_log-v0.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-kit/log/com_github_go_kit_log-v0.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-kit/log/com_github_go_kit_log-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_ldap_ldap_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-ldap/ldap/v3",
        sha256 = "217e899d6fc84f70eb3a7660ee383b660c21f6315b5bc4232c8ab7b568cc0bd0",
        strip_prefix = "github.com/YangKeao/ldap/v3@v3.4.5-0.20230421065457-369a3bab1117",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/YangKeao/ldap/v3/com_github_yangkeao_ldap_v3-v3.4.5-0.20230421065457-369a3bab1117.zip",
            "http://ats.apps.svc/gomod/github.com/YangKeao/ldap/v3/com_github_yangkeao_ldap_v3-v3.4.5-0.20230421065457-369a3bab1117.zip",
            "https://cache.hawkingrei.com/gomod/github.com/YangKeao/ldap/v3/com_github_yangkeao_ldap_v3-v3.4.5-0.20230421065457-369a3bab1117.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/YangKeao/ldap/v3/com_github_yangkeao_ldap_v3-v3.4.5-0.20230421065457-369a3bab1117.zip",
        ],
    )
    go_repository(
        name = "com_github_go_logfmt_logfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logfmt/logfmt",
        sha256 = "a49c00cff30c02d9c09a4974ce91215bfe37f528a74f129576697869a1b8c630",
        strip_prefix = "github.com/go-logfmt/logfmt@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-logfmt/logfmt/com_github_go_logfmt_logfmt-v0.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-logfmt/logfmt/com_github_go_logfmt_logfmt-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-logfmt/logfmt/com_github_go_logfmt_logfmt-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-logfmt/logfmt/com_github_go_logfmt_logfmt-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_logr_logr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logr/logr",
        sha256 = "27d1c8d411fd8e42dc6202991d70afa630089700f1d002de5454d6c26f93674c",
        strip_prefix = "github.com/go-logr/logr@v1.4.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-logr/logr/com_github_go_logr_logr-v1.4.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-logr/logr/com_github_go_logr_logr-v1.4.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-logr/logr/com_github_go_logr_logr-v1.4.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-logr/logr/com_github_go_logr_logr-v1.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_logr_stdr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logr/stdr",
        sha256 = "9dd6893bf700198485ae699640b49bc1efbc6c73b37cb5792a0476e1fd8f7fef",
        strip_prefix = "github.com/go-logr/stdr@v1.2.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-logr/stdr/com_github_go_logr_stdr-v1.2.2.zip",
            "http://ats.apps.svc/gomod/github.com/go-logr/stdr/com_github_go_logr_stdr-v1.2.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-logr/stdr/com_github_go_logr_stdr-v1.2.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-logr/stdr/com_github_go_logr_stdr-v1.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_go_martini_martini",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-martini/martini",
        sha256 = "0561a4dadd68dbc1b38c09ed95bbfc5073b0a7708b9a787d38533ebd48040ec2",
        strip_prefix = "github.com/go-martini/martini@v0.0.0-20170121215854-22fa46961aab",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-martini/martini/com_github_go_martini_martini-v0.0.0-20170121215854-22fa46961aab.zip",
            "http://ats.apps.svc/gomod/github.com/go-martini/martini/com_github_go_martini_martini-v0.0.0-20170121215854-22fa46961aab.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-martini/martini/com_github_go_martini_martini-v0.0.0-20170121215854-22fa46961aab.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-martini/martini/com_github_go_martini_martini-v0.0.0-20170121215854-22fa46961aab.zip",
        ],
    )
    go_repository(
        name = "com_github_go_ole_go_ole",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-ole/go-ole",
        sha256 = "bbf5b3bfa227a5daa06eb16ecdecccc0b20e08749bf103afb523fd72764e727a",
        strip_prefix = "github.com/go-ole/go-ole@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-ole/go-ole/com_github_go_ole_go_ole-v1.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-ole/go-ole/com_github_go_ole_go_ole-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-ole/go-ole/com_github_go_ole_go_ole-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-ole/go-ole/com_github_go_ole_go_ole-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_analysis",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/analysis",
        sha256 = "c38edc10742e5592847d0608ba13b1372a4a7ce1309fc521ea58842a0eb99d16",
        strip_prefix = "github.com/go-openapi/analysis@v0.21.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/analysis/com_github_go_openapi_analysis-v0.21.4.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/analysis/com_github_go_openapi_analysis-v0.21.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/analysis/com_github_go_openapi_analysis-v0.21.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/analysis/com_github_go_openapi_analysis-v0.21.4.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/errors",
        sha256 = "40b1b8d380b340602f760e050ca81fe3abfdd88d4e671ab5b9ca6d0361038eee",
        strip_prefix = "github.com/go-openapi/errors@v0.20.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/errors/com_github_go_openapi_errors-v0.20.4.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/errors/com_github_go_openapi_errors-v0.20.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/errors/com_github_go_openapi_errors-v0.20.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/errors/com_github_go_openapi_errors-v0.20.4.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_jsonpointer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonpointer",
        sha256 = "ff51a1ccbf148289e755c55f756fde4aa9626d5b6a79065f7592be868fc0ed74",
        strip_prefix = "github.com/go-openapi/jsonpointer@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/jsonpointer/com_github_go_openapi_jsonpointer-v0.20.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/jsonpointer/com_github_go_openapi_jsonpointer-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/jsonpointer/com_github_go_openapi_jsonpointer-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/jsonpointer/com_github_go_openapi_jsonpointer-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_jsonreference",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonreference",
        sha256 = "27afd0bef56453e463eba6093afb04dc08d97b5ad0e15b2266cac867d062ae1b",
        strip_prefix = "github.com/go-openapi/jsonreference@v0.20.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/jsonreference/com_github_go_openapi_jsonreference-v0.20.2.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/jsonreference/com_github_go_openapi_jsonreference-v0.20.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/jsonreference/com_github_go_openapi_jsonreference-v0.20.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/jsonreference/com_github_go_openapi_jsonreference-v0.20.2.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_loads",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/loads",
        sha256 = "a97ae476c31ad269ad3429186fab2fe08f38eeb5d4167215004194b19da9d1de",
        strip_prefix = "github.com/go-openapi/loads@v0.21.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/loads/com_github_go_openapi_loads-v0.21.2.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/loads/com_github_go_openapi_loads-v0.21.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/loads/com_github_go_openapi_loads-v0.21.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/loads/com_github_go_openapi_loads-v0.21.2.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_spec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/spec",
        sha256 = "06c843a4617b262b06f232c6fa380e732dea80cf77b9a80a09c0d1c83a0a8665",
        strip_prefix = "github.com/go-openapi/spec@v0.20.9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/spec/com_github_go_openapi_spec-v0.20.9.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/spec/com_github_go_openapi_spec-v0.20.9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/spec/com_github_go_openapi_spec-v0.20.9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/spec/com_github_go_openapi_spec-v0.20.9.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_strfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/strfmt",
        sha256 = "f2e876720e95509630e706a59d0d2ae735aae1f1afb4b2b8c89ea9231dc71084",
        strip_prefix = "github.com/go-openapi/strfmt@v0.21.9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/strfmt/com_github_go_openapi_strfmt-v0.21.9.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/strfmt/com_github_go_openapi_strfmt-v0.21.9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/strfmt/com_github_go_openapi_strfmt-v0.21.9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/strfmt/com_github_go_openapi_strfmt-v0.21.9.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_swag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/swag",
        sha256 = "ce8e7f82205e5c1949c99710f7d74be65d9a1353f38afe85338e9e4ba5981cb9",
        strip_prefix = "github.com/go-openapi/swag@v0.22.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/swag/com_github_go_openapi_swag-v0.22.4.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/swag/com_github_go_openapi_swag-v0.22.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/swag/com_github_go_openapi_swag-v0.22.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/swag/com_github_go_openapi_swag-v0.22.4.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_validate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/validate",
        sha256 = "7d528b3b728df6a721977532d838cec9a6699baf49959e195aa775e32909d0b2",
        strip_prefix = "github.com/go-openapi/validate@v0.22.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/validate/com_github_go_openapi_validate-v0.22.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/validate/com_github_go_openapi_validate-v0.22.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/validate/com_github_go_openapi_validate-v0.22.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/validate/com_github_go_openapi_validate-v0.22.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_resty_resty_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-resty/resty/v2",
        sha256 = "9e6212c6a90936edadf04754df8dfa1b0b154e013bdbf75f94a105d9fa54165e",
        strip_prefix = "github.com/go-resty/resty/v2@v2.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-resty/resty/v2/com_github_go_resty_resty_v2-v2.11.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-resty/resty/v2/com_github_go_resty_resty_v2-v2.11.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-resty/resty/v2/com_github_go_resty_resty_v2-v2.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-resty/resty/v2/com_github_go_resty_resty_v2-v2.11.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_sql_driver_mysql",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-sql-driver/mysql",
        sha256 = "5d3436cafe5d147d1f56cca6917f155b0e337b7d5df9f2f8b8be33584a7b1e2d",
        strip_prefix = "github.com/go-sql-driver/mysql@v1.7.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-sql-driver/mysql/com_github_go_sql_driver_mysql-v1.7.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-sql-driver/mysql/com_github_go_sql_driver_mysql-v1.7.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-sql-driver/mysql/com_github_go_sql_driver_mysql-v1.7.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-sql-driver/mysql/com_github_go_sql_driver_mysql-v1.7.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_stack_stack",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-stack/stack",
        sha256 = "78c2667c710f811307038634ffa43af442619acfeaf1efb593aa4e0ded9df48f",
        strip_prefix = "github.com/go-stack/stack@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-stack/stack/com_github_go_stack_stack-v1.8.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-stack/stack/com_github_go_stack_stack-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-stack/stack/com_github_go_stack_stack-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-stack/stack/com_github_go_stack_stack-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_task_slim_sprig",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-task/slim-sprig",
        sha256 = "25a036dc8eb9f6227c2df818916f76db93eebbac88cc24bad5c960b0c60d7a08",
        strip_prefix = "github.com/go-task/slim-sprig@v0.0.0-20230315185526-52ccab3ef572",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-task/slim-sprig/com_github_go_task_slim_sprig-v0.0.0-20230315185526-52ccab3ef572.zip",
            "http://ats.apps.svc/gomod/github.com/go-task/slim-sprig/com_github_go_task_slim_sprig-v0.0.0-20230315185526-52ccab3ef572.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-task/slim-sprig/com_github_go_task_slim_sprig-v0.0.0-20230315185526-52ccab3ef572.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-task/slim-sprig/com_github_go_task_slim_sprig-v0.0.0-20230315185526-52ccab3ef572.zip",
        ],
    )
    go_repository(
        name = "com_github_go_toolsmith_astcast",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astcast",
        sha256 = "c02cc24bf79fccc19edf826aff57a2f3c4db66abe9901345175abd46689b643a",
        strip_prefix = "github.com/go-toolsmith/astcast@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-toolsmith/astcast/com_github_go_toolsmith_astcast-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-toolsmith/astcast/com_github_go_toolsmith_astcast-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-toolsmith/astcast/com_github_go_toolsmith_astcast-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-toolsmith/astcast/com_github_go_toolsmith_astcast-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_toolsmith_astcopy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astcopy",
        sha256 = "941006b1e498d59d593f74ba2bf2f58f5aafc2dc29fba4e0b803394b6098b7eb",
        strip_prefix = "github.com/go-toolsmith/astcopy@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-toolsmith/astcopy/com_github_go_toolsmith_astcopy-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-toolsmith/astcopy/com_github_go_toolsmith_astcopy-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-toolsmith/astcopy/com_github_go_toolsmith_astcopy-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-toolsmith/astcopy/com_github_go_toolsmith_astcopy-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_toolsmith_astequal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astequal",
        sha256 = "9f646c10df79d73af47c98b7eca0f44ee3f49ec6da71597b2e4630114a11d59e",
        strip_prefix = "github.com/go-toolsmith/astequal@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-toolsmith/astequal/com_github_go_toolsmith_astequal-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-toolsmith/astequal/com_github_go_toolsmith_astequal-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-toolsmith/astequal/com_github_go_toolsmith_astequal-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-toolsmith/astequal/com_github_go_toolsmith_astequal-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_toolsmith_astfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astfmt",
        sha256 = "6e21f3ed75bba0460be9448e575ac342b75b128dbd273e568252780f18608b60",
        strip_prefix = "github.com/go-toolsmith/astfmt@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-toolsmith/astfmt/com_github_go_toolsmith_astfmt-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-toolsmith/astfmt/com_github_go_toolsmith_astfmt-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-toolsmith/astfmt/com_github_go_toolsmith_astfmt-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-toolsmith/astfmt/com_github_go_toolsmith_astfmt-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_toolsmith_astp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astp",
        sha256 = "e594ad39bd1a4235cef8bfc6c7c530707c4b366cc667ed0af76e397ce89689d7",
        strip_prefix = "github.com/go-toolsmith/astp@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-toolsmith/astp/com_github_go_toolsmith_astp-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-toolsmith/astp/com_github_go_toolsmith_astp-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-toolsmith/astp/com_github_go_toolsmith_astp-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-toolsmith/astp/com_github_go_toolsmith_astp-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_toolsmith_strparse",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/strparse",
        sha256 = "4d1b8d6b53b0595942cbd7f49f33690e7d673785d151d8aade02d9baa5e5cd6a",
        strip_prefix = "github.com/go-toolsmith/strparse@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-toolsmith/strparse/com_github_go_toolsmith_strparse-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-toolsmith/strparse/com_github_go_toolsmith_strparse-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-toolsmith/strparse/com_github_go_toolsmith_strparse-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-toolsmith/strparse/com_github_go_toolsmith_strparse-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_toolsmith_typep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/typep",
        sha256 = "48a1c09f9968b29b0d62029988db0a242869df138553c5d9235f61d51d80ba48",
        strip_prefix = "github.com/go-toolsmith/typep@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-toolsmith/typep/com_github_go_toolsmith_typep-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-toolsmith/typep/com_github_go_toolsmith_typep-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-toolsmith/typep/com_github_go_toolsmith_typep-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-toolsmith/typep/com_github_go_toolsmith_typep-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_viper_mapstructure_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-viper/mapstructure/v2",
        sha256 = "3b8336eed67f93b0d9c0e087d2f6e08e56fb01fc5c8d518c9b1fdda0fbcb5c62",
        strip_prefix = "github.com/go-viper/mapstructure/v2@v2.0.0-alpha.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-viper/mapstructure/v2/com_github_go_viper_mapstructure_v2-v2.0.0-alpha.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-viper/mapstructure/v2/com_github_go_viper_mapstructure_v2-v2.0.0-alpha.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-viper/mapstructure/v2/com_github_go_viper_mapstructure_v2-v2.0.0-alpha.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-viper/mapstructure/v2/com_github_go_viper_mapstructure_v2-v2.0.0-alpha.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_xmlfmt_xmlfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-xmlfmt/xmlfmt",
        sha256 = "f2b5cb3c797696f8c3868628d818ce2b55ef93a0ab5d9ada4c4a5088177d7ccf",
        strip_prefix = "github.com/go-xmlfmt/xmlfmt@v1.1.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-xmlfmt/xmlfmt/com_github_go_xmlfmt_xmlfmt-v1.1.2.zip",
            "http://ats.apps.svc/gomod/github.com/go-xmlfmt/xmlfmt/com_github_go_xmlfmt_xmlfmt-v1.1.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-xmlfmt/xmlfmt/com_github_go_xmlfmt_xmlfmt-v1.1.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-xmlfmt/xmlfmt/com_github_go_xmlfmt_xmlfmt-v1.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_go_zookeeper_zk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-zookeeper/zk",
        sha256 = "5577b9e7924ff73c19e2c62fb6fddb9621d05f0720d0994ce8dc4be625399ca3",
        strip_prefix = "github.com/go-zookeeper/zk@v1.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-zookeeper/zk/com_github_go_zookeeper_zk-v1.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/go-zookeeper/zk/com_github_go_zookeeper_zk-v1.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-zookeeper/zk/com_github_go_zookeeper_zk-v1.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-zookeeper/zk/com_github_go_zookeeper_zk-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_gobwas_glob",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/glob",
        sha256 = "0cfe486cd63d45ed4cb5863ff1cbd14b15e4b9380dcbf80ff26991b4049f4fdf",
        strip_prefix = "github.com/gobwas/glob@v0.2.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gobwas/glob/com_github_gobwas_glob-v0.2.3.zip",
            "http://ats.apps.svc/gomod/github.com/gobwas/glob/com_github_gobwas_glob-v0.2.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gobwas/glob/com_github_gobwas_glob-v0.2.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gobwas/glob/com_github_gobwas_glob-v0.2.3.zip",
        ],
    )
    go_repository(
        name = "com_github_gobwas_httphead",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/httphead",
        sha256 = "a4646f1d12786fee639c489219e7c667b10f7dc19578a4e7222bd17c5d9bdf8a",
        strip_prefix = "github.com/gobwas/httphead@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gobwas/httphead/com_github_gobwas_httphead-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/gobwas/httphead/com_github_gobwas_httphead-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gobwas/httphead/com_github_gobwas_httphead-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gobwas/httphead/com_github_gobwas_httphead-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gobwas_pool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/pool",
        sha256 = "79b505a9f42b141affca1eedd2edc87ae922482d052e16e3b6e5e3c9dcec89e1",
        strip_prefix = "github.com/gobwas/pool@v0.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gobwas/pool/com_github_gobwas_pool-v0.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/gobwas/pool/com_github_gobwas_pool-v0.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gobwas/pool/com_github_gobwas_pool-v0.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gobwas/pool/com_github_gobwas_pool-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gobwas_ws",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/ws",
        sha256 = "423d7d8b1364e1d9b0c4418905f7dfc29c092dc2db4c80fb66b695d4a002daca",
        strip_prefix = "github.com/gobwas/ws@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gobwas/ws/com_github_gobwas_ws-v1.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/gobwas/ws/com_github_gobwas_ws-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gobwas/ws/com_github_gobwas_ws-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gobwas/ws/com_github_gobwas_ws-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_goccy_go_json",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/goccy/go-json",
        sha256 = "ed9043ee01cc46557c74bcecc625db37ffe3a5c7af219f390a287f44a40c2520",
        strip_prefix = "github.com/goccy/go-json@v0.10.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/goccy/go-json/com_github_goccy_go_json-v0.10.2.zip",
            "http://ats.apps.svc/gomod/github.com/goccy/go-json/com_github_goccy_go_json-v0.10.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/goccy/go-json/com_github_goccy_go_json-v0.10.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/goccy/go-json/com_github_goccy_go_json-v0.10.2.zip",
        ],
    )
    go_repository(
        name = "com_github_godbus_dbus_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/godbus/dbus/v5",
        sha256 = "23a23f08bea48e6e49a46a4015b64adbb1692dc6ddf0d83c2f0c2027cb8e31c8",
        strip_prefix = "github.com/godbus/dbus/v5@v5.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/godbus/dbus/v5/com_github_godbus_dbus_v5-v5.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/godbus/dbus/v5/com_github_godbus_dbus_v5-v5.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/godbus/dbus/v5/com_github_godbus_dbus_v5-v5.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/godbus/dbus/v5/com_github_godbus_dbus_v5-v5.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_gofrs_flock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gofrs/flock",
        sha256 = "9ace5b0a05672937904fba1fcb86cb45e7f701e508faeb5f612e243340351dfa",
        strip_prefix = "github.com/gofrs/flock@v0.8.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gofrs/flock/com_github_gofrs_flock-v0.8.1.zip",
            "http://ats.apps.svc/gomod/github.com/gofrs/flock/com_github_gofrs_flock-v0.8.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gofrs/flock/com_github_gofrs_flock-v0.8.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gofrs/flock/com_github_gofrs_flock-v0.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gogo_googleapis",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/googleapis",
        sha256 = "4933f2a2ffadf09e6fc167743c07d44ddbe2f5748da66d48cbc0af7726702d8b",
        strip_prefix = "github.com/gogo/googleapis@v0.0.0-20180223154316-0cd9801be74a",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gogo/googleapis/com_github_gogo_googleapis-v0.0.0-20180223154316-0cd9801be74a.zip",
            "http://ats.apps.svc/gomod/github.com/gogo/googleapis/com_github_gogo_googleapis-v0.0.0-20180223154316-0cd9801be74a.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gogo/googleapis/com_github_gogo_googleapis-v0.0.0-20180223154316-0cd9801be74a.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gogo/googleapis/com_github_gogo_googleapis-v0.0.0-20180223154316-0cd9801be74a.zip",
        ],
    )
    go_repository(
        name = "com_github_gogo_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/protobuf",
        sha256 = "dd2b73f163c8183941626360196c8f844addd95423d341a0412e1b22d0104ff7",
        strip_prefix = "github.com/gogo/protobuf@v1.3.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gogo/protobuf/com_github_gogo_protobuf-v1.3.2.zip",
            "http://ats.apps.svc/gomod/github.com/gogo/protobuf/com_github_gogo_protobuf-v1.3.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gogo/protobuf/com_github_gogo_protobuf-v1.3.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gogo/protobuf/com_github_gogo_protobuf-v1.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_gogo_status",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/status",
        sha256 = "c042d3555c9f490a75d44ad4c3dff367f9512e6d189252f8765f4837b11b12b1",
        strip_prefix = "github.com/gogo/status@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gogo/status/com_github_gogo_status-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/gogo/status/com_github_gogo_status-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gogo/status/com_github_gogo_status-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gogo/status/com_github_gogo_status-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_goji_httpauth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/goji/httpauth",
        sha256 = "8467ed1df8ffba8da7ead144b656b6281469ab4d122adf3edf496175ad870192",
        strip_prefix = "github.com/goji/httpauth@v0.0.0-20160601135302-2da839ab0f4d",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/goji/httpauth/com_github_goji_httpauth-v0.0.0-20160601135302-2da839ab0f4d.zip",
            "http://ats.apps.svc/gomod/github.com/goji/httpauth/com_github_goji_httpauth-v0.0.0-20160601135302-2da839ab0f4d.zip",
            "https://cache.hawkingrei.com/gomod/github.com/goji/httpauth/com_github_goji_httpauth-v0.0.0-20160601135302-2da839ab0f4d.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/goji/httpauth/com_github_goji_httpauth-v0.0.0-20160601135302-2da839ab0f4d.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_freetype",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/freetype",
        sha256 = "cdcb9e6a14933dcbf167b44dcd5083fc6a2e52c4fae8fb79747c691efeb7d84e",
        strip_prefix = "github.com/golang/freetype@v0.0.0-20170609003504-e2365dfdc4a0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang/freetype/com_github_golang_freetype-v0.0.0-20170609003504-e2365dfdc4a0.zip",
            "http://ats.apps.svc/gomod/github.com/golang/freetype/com_github_golang_freetype-v0.0.0-20170609003504-e2365dfdc4a0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang/freetype/com_github_golang_freetype-v0.0.0-20170609003504-e2365dfdc4a0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang/freetype/com_github_golang_freetype-v0.0.0-20170609003504-e2365dfdc4a0.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_glog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/glog",
        sha256 = "07688d418628ff30ffd40fde44956d1fb6bae4436003d7fcca40c85236b9484a",
        strip_prefix = "github.com/golang/glog@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang/glog/com_github_golang_glog-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/golang/glog/com_github_golang_glog-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang/glog/com_github_golang_glog-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang/glog/com_github_golang_glog-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_groupcache",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/groupcache",
        sha256 = "b27034e8fc013627543e1ad098cfc65329f2896df3da5cf3266cc9166f93f3a5",
        strip_prefix = "github.com/golang/groupcache@v0.0.0-20210331224755-41bb18bfe9da",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang/groupcache/com_github_golang_groupcache-v0.0.0-20210331224755-41bb18bfe9da.zip",
            "http://ats.apps.svc/gomod/github.com/golang/groupcache/com_github_golang_groupcache-v0.0.0-20210331224755-41bb18bfe9da.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang/groupcache/com_github_golang_groupcache-v0.0.0-20210331224755-41bb18bfe9da.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang/groupcache/com_github_golang_groupcache-v0.0.0-20210331224755-41bb18bfe9da.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_jwt_jwt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-jwt/jwt",
        sha256 = "1fedba05e152177f8de04cafe8d30200b03e657f70ac667b2fa8e04fb3d9109d",
        strip_prefix = "github.com/golang-jwt/jwt@v3.2.1+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang-jwt/jwt/com_github_golang_jwt_jwt-v3.2.1+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/golang-jwt/jwt/com_github_golang_jwt_jwt-v3.2.1+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang-jwt/jwt/com_github_golang_jwt_jwt-v3.2.1+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang-jwt/jwt/com_github_golang_jwt_jwt-v3.2.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_jwt_jwt_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-jwt/jwt/v4",
        sha256 = "fdb3b9c078eba9a2bd437c1b3acdf98ee09d276121a97b4bc7f6d870eb5ff75b",
        strip_prefix = "github.com/golang-jwt/jwt/v4@v4.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang-jwt/jwt/v4/com_github_golang_jwt_jwt_v4-v4.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/golang-jwt/jwt/v4/com_github_golang_jwt_jwt_v4-v4.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang-jwt/jwt/v4/com_github_golang_jwt_jwt_v4-v4.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang-jwt/jwt/v4/com_github_golang_jwt_jwt_v4-v4.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_jwt_jwt_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-jwt/jwt/v5",
        sha256 = "d7d763fe73d36361b7a005a3fa3e7bc908ac395c490e1b5b0fdbc4a65272f0b8",
        strip_prefix = "github.com/golang-jwt/jwt/v5@v5.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang-jwt/jwt/v5/com_github_golang_jwt_jwt_v5-v5.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/golang-jwt/jwt/v5/com_github_golang_jwt_jwt_v5-v5.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang-jwt/jwt/v5/com_github_golang_jwt_jwt_v5-v5.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang-jwt/jwt/v5/com_github_golang_jwt_jwt_v5-v5.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_mock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/mock",
        sha256 = "fa25916b546f90da49418f436e3a61e4c5dae898cf3c82b0007b5a6fab74261b",
        strip_prefix = "github.com/golang/mock@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang/mock/com_github_golang_mock-v1.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/golang/mock/com_github_golang_mock-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang/mock/com_github_golang_mock-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang/mock/com_github_golang_mock-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/protobuf",
        patch_args = ["-p1"],
        patches = [
            "//build/patches:com_github_golang_protobuf.patch",
        ],
        sha256 = "9a2f43d3eac8ceda506ebbeb4f229254b87235ce90346692a0e233614182190b",
        strip_prefix = "github.com/golang/protobuf@v1.5.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang/protobuf/com_github_golang_protobuf-v1.5.4.zip",
            "http://ats.apps.svc/gomod/github.com/golang/protobuf/com_github_golang_protobuf-v1.5.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang/protobuf/com_github_golang_protobuf-v1.5.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang/protobuf/com_github_golang_protobuf-v1.5.4.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_snappy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/snappy",
        sha256 = "ea4545ca44ee990554094df6de440386a440a5bd99106e048939409d63beb423",
        strip_prefix = "github.com/golang/snappy@v0.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang/snappy/com_github_golang_snappy-v0.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/golang/snappy/com_github_golang_snappy-v0.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang/snappy/com_github_golang_snappy-v0.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang/snappy/com_github_golang_snappy-v0.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_check",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/check",
        sha256 = "5c7fb283a9c9c3cc3f1d8cb4795d2482bc969db30723441fd2478e937bb32ad4",
        strip_prefix = "github.com/golangci/check@v0.0.0-20180506172741-cfe4005ccda2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/check/com_github_golangci_check-v0.0.0-20180506172741-cfe4005ccda2.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/check/com_github_golangci_check-v0.0.0-20180506172741-cfe4005ccda2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/check/com_github_golangci_check-v0.0.0-20180506172741-cfe4005ccda2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/check/com_github_golangci_check-v0.0.0-20180506172741-cfe4005ccda2.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_dupl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/dupl",
        sha256 = "51e235bdd12ec48cb72afc3ddd4ca0f065554b53dce02fdce5c6434d84b3fc8d",
        strip_prefix = "github.com/golangci/dupl@v0.0.0-20180902072040-3e9179ac440a",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/dupl/com_github_golangci_dupl-v0.0.0-20180902072040-3e9179ac440a.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/dupl/com_github_golangci_dupl-v0.0.0-20180902072040-3e9179ac440a.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/dupl/com_github_golangci_dupl-v0.0.0-20180902072040-3e9179ac440a.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/dupl/com_github_golangci_dupl-v0.0.0-20180902072040-3e9179ac440a.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_go_misc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/go-misc",
        sha256 = "9df3692a0de7e030aa3dceac7b06664856a049e9cd87b0d2c21bcec26cd1333c",
        strip_prefix = "github.com/golangci/go-misc@v0.0.0-20220329215616-d24fe342adfe",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/go-misc/com_github_golangci_go_misc-v0.0.0-20220329215616-d24fe342adfe.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/go-misc/com_github_golangci_go_misc-v0.0.0-20220329215616-d24fe342adfe.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/go-misc/com_github_golangci_go_misc-v0.0.0-20220329215616-d24fe342adfe.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/go-misc/com_github_golangci_go_misc-v0.0.0-20220329215616-d24fe342adfe.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_gofmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/gofmt",
        sha256 = "ffca3283ab68db353f1e8e312c9d178cc6384db4179c5f214e553fe60380f68d",
        strip_prefix = "github.com/golangci/gofmt@v0.0.0-20231019111953-be8c47862aaa",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/gofmt/com_github_golangci_gofmt-v0.0.0-20231019111953-be8c47862aaa.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/gofmt/com_github_golangci_gofmt-v0.0.0-20231019111953-be8c47862aaa.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/gofmt/com_github_golangci_gofmt-v0.0.0-20231019111953-be8c47862aaa.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/gofmt/com_github_golangci_gofmt-v0.0.0-20231019111953-be8c47862aaa.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_golangci_lint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/golangci-lint",
        sha256 = "b7505fdd1857ec9ed3c4ced1c53a4bfd3b4de36722a8025a22a1cdb0f18fa00e",
        strip_prefix = "github.com/golangci/golangci-lint@v1.56.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/golangci-lint/com_github_golangci_golangci_lint-v1.56.2.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/golangci-lint/com_github_golangci_golangci_lint-v1.56.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/golangci-lint/com_github_golangci_golangci_lint-v1.56.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/golangci-lint/com_github_golangci_golangci_lint-v1.56.2.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_gosec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/gosec",
        sha256 = "04e556925885db7957b6429bc90361bd0d3d82bcc64eb1de88c4bb07375fa161",
        strip_prefix = "github.com/golangci/gosec@v0.0.0-20180901114220-8afd9cbb6cfb",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/gosec/com_github_golangci_gosec-v0.0.0-20180901114220-8afd9cbb6cfb.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/gosec/com_github_golangci_gosec-v0.0.0-20180901114220-8afd9cbb6cfb.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/gosec/com_github_golangci_gosec-v0.0.0-20180901114220-8afd9cbb6cfb.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/gosec/com_github_golangci_gosec-v0.0.0-20180901114220-8afd9cbb6cfb.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_lint_1",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/lint-1",
        sha256 = "c8be48c9286072c9cdad2f275435fdd093f5a5d61fad42ccb7367e42b7fdfaf4",
        strip_prefix = "github.com/golangci/lint-1@v0.0.0-20191013205115-297bf364a8e0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/lint-1/com_github_golangci_lint_1-v0.0.0-20191013205115-297bf364a8e0.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/lint-1/com_github_golangci_lint_1-v0.0.0-20191013205115-297bf364a8e0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/lint-1/com_github_golangci_lint_1-v0.0.0-20191013205115-297bf364a8e0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/lint-1/com_github_golangci_lint_1-v0.0.0-20191013205115-297bf364a8e0.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_maligned",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/maligned",
        sha256 = "ade4c35cd67dfba8411847fd7d9b8f7912e71d5091d10a1b09e903260352d31a",
        strip_prefix = "github.com/golangci/maligned@v0.0.0-20180506175553-b1d89398deca",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/maligned/com_github_golangci_maligned-v0.0.0-20180506175553-b1d89398deca.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/maligned/com_github_golangci_maligned-v0.0.0-20180506175553-b1d89398deca.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/maligned/com_github_golangci_maligned-v0.0.0-20180506175553-b1d89398deca.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/maligned/com_github_golangci_maligned-v0.0.0-20180506175553-b1d89398deca.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_misspell",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/misspell",
        sha256 = "5792fe3dd490249e6288020ff82c72a716ebaf52a8e99fe787b908423587fba3",
        strip_prefix = "github.com/golangci/misspell@v0.4.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/misspell/com_github_golangci_misspell-v0.4.1.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/misspell/com_github_golangci_misspell-v0.4.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/misspell/com_github_golangci_misspell-v0.4.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/misspell/com_github_golangci_misspell-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_prealloc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/prealloc",
        sha256 = "f2e1ea148d92af46aa61dbb777aa525a9792575a3282fd37af6b5d380dd5bca8",
        strip_prefix = "github.com/golangci/prealloc@v0.0.0-20180630174525-215b22d4de21",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/prealloc/com_github_golangci_prealloc-v0.0.0-20180630174525-215b22d4de21.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/prealloc/com_github_golangci_prealloc-v0.0.0-20180630174525-215b22d4de21.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/prealloc/com_github_golangci_prealloc-v0.0.0-20180630174525-215b22d4de21.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/prealloc/com_github_golangci_prealloc-v0.0.0-20180630174525-215b22d4de21.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_revgrep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/revgrep",
        sha256 = "0806458bba9e33b3a3566e5f246ed4be0f356695606c5987974effbc9081f750",
        strip_prefix = "github.com/golangci/revgrep@v0.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/revgrep/com_github_golangci_revgrep-v0.5.2.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/revgrep/com_github_golangci_revgrep-v0.5.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/revgrep/com_github_golangci_revgrep-v0.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/revgrep/com_github_golangci_revgrep-v0.5.2.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_unconvert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/unconvert",
        sha256 = "97baf7ee25f7532e1d634e6e0d0cc572c83a761a781dce2f59544762878af685",
        strip_prefix = "github.com/golangci/unconvert@v0.0.0-20180507085042-28b1c447d1f4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/unconvert/com_github_golangci_unconvert-v0.0.0-20180507085042-28b1c447d1f4.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/unconvert/com_github_golangci_unconvert-v0.0.0-20180507085042-28b1c447d1f4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/unconvert/com_github_golangci_unconvert-v0.0.0-20180507085042-28b1c447d1f4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/unconvert/com_github_golangci_unconvert-v0.0.0-20180507085042-28b1c447d1f4.zip",
        ],
    )
    go_repository(
        name = "com_github_gomodule_redigo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gomodule/redigo",
        sha256 = "f665942b590c65e87284d681ea2784d0b9873c644756f4716a9972dc0d8e804e",
        strip_prefix = "github.com/gomodule/redigo@v1.7.1-0.20190724094224-574c33c3df38",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gomodule/redigo/com_github_gomodule_redigo-v1.7.1-0.20190724094224-574c33c3df38.zip",
            "http://ats.apps.svc/gomod/github.com/gomodule/redigo/com_github_gomodule_redigo-v1.7.1-0.20190724094224-574c33c3df38.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gomodule/redigo/com_github_gomodule_redigo-v1.7.1-0.20190724094224-574c33c3df38.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gomodule/redigo/com_github_gomodule_redigo-v1.7.1-0.20190724094224-574c33c3df38.zip",
        ],
    )
    go_repository(
        name = "com_github_google_btree",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/btree",
        sha256 = "faee8550c5fffb4ae1dadde5ccaccb13298726f9fad226bb4eed0c03c90a481d",
        strip_prefix = "github.com/google/btree@v1.1.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/btree/com_github_google_btree-v1.1.2.zip",
            "http://ats.apps.svc/gomod/github.com/google/btree/com_github_google_btree-v1.1.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/btree/com_github_google_btree-v1.1.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/btree/com_github_google_btree-v1.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_google_gnostic_models",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/gnostic-models",
        sha256 = "5276180bd184f64676867fc2f64a583175968c507d404be6b7f1261ead229484",
        strip_prefix = "github.com/google/gnostic-models@v0.6.8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/gnostic-models/com_github_google_gnostic_models-v0.6.8.zip",
            "http://ats.apps.svc/gomod/github.com/google/gnostic-models/com_github_google_gnostic_models-v0.6.8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/gnostic-models/com_github_google_gnostic_models-v0.6.8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/gnostic-models/com_github_google_gnostic_models-v0.6.8.zip",
        ],
    )
    go_repository(
        name = "com_github_google_go_cmp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-cmp",
        sha256 = "4b4e9bf6c48211080651b491dfb48d68b736c66a305bcf94605606e1ba2eaa4a",
        strip_prefix = "github.com/google/go-cmp@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/go-cmp/com_github_google_go_cmp-v0.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/google/go-cmp/com_github_google_go_cmp-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/go-cmp/com_github_google_go_cmp-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/go-cmp/com_github_google_go_cmp-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_google_go_github_v33",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-github/v33",
        sha256 = "16649a598ad8c271509c5967778ea322eb3d5046d68f0ff770b326786e77f4bc",
        strip_prefix = "github.com/google/go-github/v33@v33.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/go-github/v33/com_github_google_go_github_v33-v33.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/google/go-github/v33/com_github_google_go_github_v33-v33.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/go-github/v33/com_github_google_go_github_v33-v33.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/go-github/v33/com_github_google_go_github_v33-v33.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_google_go_pkcs11",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-pkcs11",
        sha256 = "b9bf12c2450efa77c8b27134d5f206633057fcf0c324883797d7fde5bc3a4887",
        strip_prefix = "github.com/google/go-pkcs11@v0.2.1-0.20230907215043-c6f79328ddf9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/go-pkcs11/com_github_google_go_pkcs11-v0.2.1-0.20230907215043-c6f79328ddf9.zip",
            "http://ats.apps.svc/gomod/github.com/google/go-pkcs11/com_github_google_go_pkcs11-v0.2.1-0.20230907215043-c6f79328ddf9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/go-pkcs11/com_github_google_go_pkcs11-v0.2.1-0.20230907215043-c6f79328ddf9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/go-pkcs11/com_github_google_go_pkcs11-v0.2.1-0.20230907215043-c6f79328ddf9.zip",
        ],
    )
    go_repository(
        name = "com_github_google_go_querystring",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-querystring",
        sha256 = "a6aafc01f5602e6177928751074e325792a654e1d92f0e238b8e8739656dd72b",
        strip_prefix = "github.com/google/go-querystring@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/go-querystring/com_github_google_go_querystring-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/google/go-querystring/com_github_google_go_querystring-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/go-querystring/com_github_google_go_querystring-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/go-querystring/com_github_google_go_querystring-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_google_gofuzz",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/gofuzz",
        sha256 = "5948f40af1923d8f98dc1d4191311030e40e0057fb255df19ebc0360f2faac16",
        strip_prefix = "github.com/google/gofuzz@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/gofuzz/com_github_google_gofuzz-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/google/gofuzz/com_github_google_gofuzz-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/gofuzz/com_github_google_gofuzz-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/gofuzz/com_github_google_gofuzz-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_google_licensecheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/licensecheck",
        sha256 = "0df7b4ca172de6ee28c525815e21fb5c2014f1e8dbe8879d099b1e019691ca7c",
        strip_prefix = "github.com/google/licensecheck@v0.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/licensecheck/com_github_google_licensecheck-v0.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/google/licensecheck/com_github_google_licensecheck-v0.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/licensecheck/com_github_google_licensecheck-v0.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/licensecheck/com_github_google_licensecheck-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_google_martian",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/martian",
        sha256 = "5bdd2ebd37dda1c0cf786db27707966c8624b288641da704b0e31c96b393ce70",
        strip_prefix = "github.com/google/martian@v2.1.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/martian/com_github_google_martian-v2.1.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/google/martian/com_github_google_martian-v2.1.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/martian/com_github_google_martian-v2.1.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/martian/com_github_google_martian-v2.1.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_google_martian_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/martian/v3",
        sha256 = "aa691c18a36d986d0505aab68925985faba03d72e15729ee1b97f919af8e628c",
        strip_prefix = "github.com/google/martian/v3@v3.3.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/martian/v3/com_github_google_martian_v3-v3.3.2.zip",
            "http://ats.apps.svc/gomod/github.com/google/martian/v3/com_github_google_martian_v3-v3.3.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/martian/v3/com_github_google_martian_v3-v3.3.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/martian/v3/com_github_google_martian_v3-v3.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_google_pprof",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/pprof",
        sha256 = "587c3a10510505eb10369aa89f5648eee6632187f9a83b11f96be9e567284791",
        strip_prefix = "github.com/google/pprof@v0.0.0-20240117000934-35fc243c5815",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/pprof/com_github_google_pprof-v0.0.0-20240117000934-35fc243c5815.zip",
            "http://ats.apps.svc/gomod/github.com/google/pprof/com_github_google_pprof-v0.0.0-20240117000934-35fc243c5815.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/pprof/com_github_google_pprof-v0.0.0-20240117000934-35fc243c5815.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/pprof/com_github_google_pprof-v0.0.0-20240117000934-35fc243c5815.zip",
        ],
    )
    go_repository(
        name = "com_github_google_renameio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/renameio",
        sha256 = "b8510bb34078691a20b8e4902d371afe0eb171b2daf953f67cb3960d1926ccf3",
        strip_prefix = "github.com/google/renameio@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/renameio/com_github_google_renameio-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/google/renameio/com_github_google_renameio-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/renameio/com_github_google_renameio-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/renameio/com_github_google_renameio-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_google_renameio_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/renameio/v2",
        sha256 = "6e2b3ddcedd6bb5ff669b8e294befc9a35c01ede30fd00a183fc637ce7c9fd8e",
        strip_prefix = "github.com/google/renameio/v2@v2.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/renameio/v2/com_github_google_renameio_v2-v2.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/google/renameio/v2/com_github_google_renameio_v2-v2.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/renameio/v2/com_github_google_renameio_v2-v2.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/renameio/v2/com_github_google_renameio_v2-v2.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_google_s2a_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/s2a-go",
        sha256 = "4392e675b6f0ff0b90f970c0280d63e34b32d077e1f8c0abd1006ad0dbeb2f2e",
        strip_prefix = "github.com/google/s2a-go@v0.1.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/s2a-go/com_github_google_s2a_go-v0.1.7.zip",
            "http://ats.apps.svc/gomod/github.com/google/s2a-go/com_github_google_s2a_go-v0.1.7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/s2a-go/com_github_google_s2a_go-v0.1.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/s2a-go/com_github_google_s2a_go-v0.1.7.zip",
        ],
    )
    go_repository(
        name = "com_github_google_skylark",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/skylark",
        sha256 = "401bbeea49fb3939c4a7246da4154d411d4612881b510657cae4a5bfa05f8c21",
        strip_prefix = "github.com/google/skylark@v0.0.0-20181101142754-a5f7082aabed",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/skylark/com_github_google_skylark-v0.0.0-20181101142754-a5f7082aabed.zip",
            "http://ats.apps.svc/gomod/github.com/google/skylark/com_github_google_skylark-v0.0.0-20181101142754-a5f7082aabed.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/skylark/com_github_google_skylark-v0.0.0-20181101142754-a5f7082aabed.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/skylark/com_github_google_skylark-v0.0.0-20181101142754-a5f7082aabed.zip",
        ],
    )
    go_repository(
        name = "com_github_google_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/uuid",
        sha256 = "d0f02f377217f42702e259684e06441edbf5140dddcc34ba9bea56038b38a6ed",
        strip_prefix = "github.com/google/uuid@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/uuid/com_github_google_uuid-v1.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/google/uuid/com_github_google_uuid-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/uuid/com_github_google_uuid-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/uuid/com_github_google_uuid-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_googleapis_enterprise_certificate_proxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/enterprise-certificate-proxy",
        sha256 = "56127cb8bea94f438c4e867f9217bdfc55865282953e54c74eee019575c1020e",
        strip_prefix = "github.com/googleapis/enterprise-certificate-proxy@v0.3.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/googleapis/enterprise-certificate-proxy/com_github_googleapis_enterprise_certificate_proxy-v0.3.2.zip",
            "http://ats.apps.svc/gomod/github.com/googleapis/enterprise-certificate-proxy/com_github_googleapis_enterprise_certificate_proxy-v0.3.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/googleapis/enterprise-certificate-proxy/com_github_googleapis_enterprise_certificate_proxy-v0.3.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/googleapis/enterprise-certificate-proxy/com_github_googleapis_enterprise_certificate_proxy-v0.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/gax-go/v2",
        sha256 = "10ad5944b8bcce3f2cb9a215a0dda163de5b1f092e61b74a4e162d1eb8f7f7a2",
        strip_prefix = "github.com/googleapis/gax-go/v2@v2.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/googleapis/gax-go/v2/com_github_googleapis_gax_go_v2-v2.12.0.zip",
            "http://ats.apps.svc/gomod/github.com/googleapis/gax-go/v2/com_github_googleapis_gax_go_v2-v2.12.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/googleapis/gax-go/v2/com_github_googleapis_gax_go_v2-v2.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/googleapis/gax-go/v2/com_github_googleapis_gax_go_v2-v2.12.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gophercloud_gophercloud",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gophercloud/gophercloud",
        sha256 = "89d7a90a3bd552caa46946be9bc031d3231898423724c4ec2b97ce7a25923a58",
        strip_prefix = "github.com/gophercloud/gophercloud@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gophercloud/gophercloud/com_github_gophercloud_gophercloud-v1.8.0.zip",
            "http://ats.apps.svc/gomod/github.com/gophercloud/gophercloud/com_github_gophercloud_gophercloud-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gophercloud/gophercloud/com_github_gophercloud_gophercloud-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gophercloud/gophercloud/com_github_gophercloud_gophercloud-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gopherjs_gopherjs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gopherjs/gopherjs",
        sha256 = "9b9ccc9606dfeae2fb533f768b437025797dc4aa59ac3f8f091b64dc14bf5db7",
        strip_prefix = "github.com/gopherjs/gopherjs@v0.0.0-20181017120253-0766667cb4d1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gopherjs/gopherjs/com_github_gopherjs_gopherjs-v0.0.0-20181017120253-0766667cb4d1.zip",
            "http://ats.apps.svc/gomod/github.com/gopherjs/gopherjs/com_github_gopherjs_gopherjs-v0.0.0-20181017120253-0766667cb4d1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gopherjs/gopherjs/com_github_gopherjs_gopherjs-v0.0.0-20181017120253-0766667cb4d1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gopherjs/gopherjs/com_github_gopherjs_gopherjs-v0.0.0-20181017120253-0766667cb4d1.zip",
        ],
    )
    go_repository(
        name = "com_github_gordonklaus_ineffassign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gordonklaus/ineffassign",
        sha256 = "75704a7f180532c5b8c274da14f6e9dddd3687a526a956ad39e0da3cc36b7b3e",
        strip_prefix = "github.com/gordonklaus/ineffassign@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gordonklaus/ineffassign/com_github_gordonklaus_ineffassign-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/gordonklaus/ineffassign/com_github_gordonklaus_ineffassign-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gordonklaus/ineffassign/com_github_gordonklaus_ineffassign-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gordonklaus/ineffassign/com_github_gordonklaus_ineffassign-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_handlers",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/handlers",
        sha256 = "700cb5572cef0b4c251fc63550d3a656d53b91cec845f19b6a16bdbc6795beec",
        strip_prefix = "github.com/gorilla/handlers@v1.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gorilla/handlers/com_github_gorilla_handlers-v1.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/gorilla/handlers/com_github_gorilla_handlers-v1.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gorilla/handlers/com_github_gorilla_handlers-v1.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gorilla/handlers/com_github_gorilla_handlers-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_mux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/mux",
        sha256 = "7641911e00af9c91f089868333067c9cb9a58702d2c9ea821ee374940091c385",
        strip_prefix = "github.com/gorilla/mux@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gorilla/mux/com_github_gorilla_mux-v1.8.0.zip",
            "http://ats.apps.svc/gomod/github.com/gorilla/mux/com_github_gorilla_mux-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gorilla/mux/com_github_gorilla_mux-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gorilla/mux/com_github_gorilla_mux-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_securecookie",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/securecookie",
        sha256 = "dd83a4230e11568159756bbea4d343c88df0cd1415bbbc7cd5badad6cd2ed903",
        strip_prefix = "github.com/gorilla/securecookie@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gorilla/securecookie/com_github_gorilla_securecookie-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/gorilla/securecookie/com_github_gorilla_securecookie-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gorilla/securecookie/com_github_gorilla_securecookie-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gorilla/securecookie/com_github_gorilla_securecookie-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_sessions",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/sessions",
        sha256 = "2c6aeebfef8062537fd7778067e5e99d4c13f79ac63114e905c97040a6e6b523",
        strip_prefix = "github.com/gorilla/sessions@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gorilla/sessions/com_github_gorilla_sessions-v1.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/gorilla/sessions/com_github_gorilla_sessions-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gorilla/sessions/com_github_gorilla_sessions-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gorilla/sessions/com_github_gorilla_sessions-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_websocket",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/websocket",
        sha256 = "e0183e81bbc710dbbcb9b2b206a89614dd3540ddfbbd59b52861edf953eda753",
        strip_prefix = "github.com/gorilla/websocket@v1.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gorilla/websocket/com_github_gorilla_websocket-v1.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/gorilla/websocket/com_github_gorilla_websocket-v1.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gorilla/websocket/com_github_gorilla_websocket-v1.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gorilla/websocket/com_github_gorilla_websocket-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gostaticanalysis_analysisutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/analysisutil",
        sha256 = "f372dd7390227402df610bb50bc0e278bb1fd34c893b2298c78801ea010c8849",
        strip_prefix = "github.com/gostaticanalysis/analysisutil@v0.7.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gostaticanalysis/analysisutil/com_github_gostaticanalysis_analysisutil-v0.7.1.zip",
            "http://ats.apps.svc/gomod/github.com/gostaticanalysis/analysisutil/com_github_gostaticanalysis_analysisutil-v0.7.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gostaticanalysis/analysisutil/com_github_gostaticanalysis_analysisutil-v0.7.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gostaticanalysis/analysisutil/com_github_gostaticanalysis_analysisutil-v0.7.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gostaticanalysis_comment",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/comment",
        sha256 = "53242816ebfcfcf63febae65c37c94a15d0838245b11fde3ccf8e05b979d40ab",
        strip_prefix = "github.com/gostaticanalysis/comment@v1.4.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gostaticanalysis/comment/com_github_gostaticanalysis_comment-v1.4.2.zip",
            "http://ats.apps.svc/gomod/github.com/gostaticanalysis/comment/com_github_gostaticanalysis_comment-v1.4.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gostaticanalysis/comment/com_github_gostaticanalysis_comment-v1.4.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gostaticanalysis/comment/com_github_gostaticanalysis_comment-v1.4.2.zip",
        ],
    )
    go_repository(
        name = "com_github_gostaticanalysis_forcetypeassert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/forcetypeassert",
        sha256 = "69841e0bb1c695ccb942ae3f0eac805b62d8d0905a776d7f9022e4cc2de15367",
        strip_prefix = "github.com/gostaticanalysis/forcetypeassert@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gostaticanalysis/forcetypeassert/com_github_gostaticanalysis_forcetypeassert-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/gostaticanalysis/forcetypeassert/com_github_gostaticanalysis_forcetypeassert-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gostaticanalysis/forcetypeassert/com_github_gostaticanalysis_forcetypeassert-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gostaticanalysis/forcetypeassert/com_github_gostaticanalysis_forcetypeassert-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gostaticanalysis_nilerr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/nilerr",
        sha256 = "8c02f73130b5d0a3649d2b4cc35bce7930d294a8cbd676db29023e6bc20c6316",
        strip_prefix = "github.com/gostaticanalysis/nilerr@v0.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gostaticanalysis/nilerr/com_github_gostaticanalysis_nilerr-v0.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/gostaticanalysis/nilerr/com_github_gostaticanalysis_nilerr-v0.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gostaticanalysis/nilerr/com_github_gostaticanalysis_nilerr-v0.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gostaticanalysis/nilerr/com_github_gostaticanalysis_nilerr-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gostaticanalysis_testutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/testutil",
        sha256 = "c20a660c72175ae026ee2c0488037babd93da54f5d8bed0fa9252f268f2a518b",
        strip_prefix = "github.com/gostaticanalysis/testutil@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gostaticanalysis/testutil/com_github_gostaticanalysis_testutil-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/gostaticanalysis/testutil/com_github_gostaticanalysis_testutil-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gostaticanalysis/testutil/com_github_gostaticanalysis_testutil-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gostaticanalysis/testutil/com_github_gostaticanalysis_testutil-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_grafana_regexp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grafana/regexp",
        sha256 = "32777ad2e39bac06b359b0d93460530a41a1e0cb7cfd92faac82feb364ce8c91",
        strip_prefix = "github.com/grafana/regexp@v0.0.0-20221122212121-6b5c0a4cb7fd",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/grafana/regexp/com_github_grafana_regexp-v0.0.0-20221122212121-6b5c0a4cb7fd.zip",
            "http://ats.apps.svc/gomod/github.com/grafana/regexp/com_github_grafana_regexp-v0.0.0-20221122212121-6b5c0a4cb7fd.zip",
            "https://cache.hawkingrei.com/gomod/github.com/grafana/regexp/com_github_grafana_regexp-v0.0.0-20221122212121-6b5c0a4cb7fd.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/grafana/regexp/com_github_grafana_regexp-v0.0.0-20221122212121-6b5c0a4cb7fd.zip",
        ],
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_middleware",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grpc-ecosystem/go-grpc-middleware",
        sha256 = "e4e1845280f93ea81648d0e48d3c17e6610c7916a49171f73c150fbde8fa9bc0",
        strip_prefix = "github.com/grpc-ecosystem/go-grpc-middleware@v1.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/grpc-ecosystem/go-grpc-middleware/com_github_grpc_ecosystem_go_grpc_middleware-v1.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/grpc-ecosystem/go-grpc-middleware/com_github_grpc_ecosystem_go_grpc_middleware-v1.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/grpc-ecosystem/go-grpc-middleware/com_github_grpc_ecosystem_go_grpc_middleware-v1.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/grpc-ecosystem/go-grpc-middleware/com_github_grpc_ecosystem_go_grpc_middleware-v1.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_prometheus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
        sha256 = "124dfc63aa52611a2882417e685c0452d4d99d64c13836a6a6747675e911fc17",
        strip_prefix = "github.com/grpc-ecosystem/go-grpc-prometheus@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/grpc-ecosystem/go-grpc-prometheus/com_github_grpc_ecosystem_go_grpc_prometheus-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/grpc-ecosystem/go-grpc-prometheus/com_github_grpc_ecosystem_go_grpc_prometheus-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/grpc-ecosystem/go-grpc-prometheus/com_github_grpc_ecosystem_go_grpc_prometheus-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/grpc-ecosystem/go-grpc-prometheus/com_github_grpc_ecosystem_go_grpc_prometheus-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_grpc_ecosystem_grpc_gateway",
        build_file_proto_mode = "disable_global",
        build_naming_convention = "go_default_library",
        importpath = "github.com/grpc-ecosystem/grpc-gateway",
        patch_args = ["-p1"],
        patches = [
            "//build/patches:com_github_grpc_ecosystem_grpc_gateway.patch",
        ],
        sha256 = "377b03aef288b34ed894449d3ddba40d525dd7fb55de6e79045cdf499e7fe565",
        strip_prefix = "github.com/grpc-ecosystem/grpc-gateway@v1.16.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/grpc-ecosystem/grpc-gateway/com_github_grpc_ecosystem_grpc_gateway-v1.16.0.zip",
            "http://ats.apps.svc/gomod/github.com/grpc-ecosystem/grpc-gateway/com_github_grpc_ecosystem_grpc_gateway-v1.16.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/grpc-ecosystem/grpc-gateway/com_github_grpc_ecosystem_grpc_gateway-v1.16.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/grpc-ecosystem/grpc-gateway/com_github_grpc_ecosystem_grpc_gateway-v1.16.0.zip",
        ],
    )
    go_repository(
        name = "com_github_grpc_ecosystem_grpc_gateway_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grpc-ecosystem/grpc-gateway/v2",
        sha256 = "31c467951356be11a0c646299cbe2155b3254a68b643888a1ef7a7511cf1b1cf",
        strip_prefix = "github.com/grpc-ecosystem/grpc-gateway/v2@v2.19.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/grpc-ecosystem/grpc-gateway/v2/com_github_grpc_ecosystem_grpc_gateway_v2-v2.19.1.zip",
            "http://ats.apps.svc/gomod/github.com/grpc-ecosystem/grpc-gateway/v2/com_github_grpc_ecosystem_grpc_gateway_v2-v2.19.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/grpc-ecosystem/grpc-gateway/v2/com_github_grpc_ecosystem_grpc_gateway_v2-v2.19.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/grpc-ecosystem/grpc-gateway/v2/com_github_grpc_ecosystem_grpc_gateway_v2-v2.19.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_consul_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/consul/api",
        sha256 = "0a944611bc32f1206642e7aeb40433cea5e98fd084290e7e44b74973d1c184e7",
        strip_prefix = "github.com/hashicorp/consul/api@v1.26.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/consul/api/com_github_hashicorp_consul_api-v1.26.1.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/consul/api/com_github_hashicorp_consul_api-v1.26.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/consul/api/com_github_hashicorp_consul_api-v1.26.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/consul/api/com_github_hashicorp_consul_api-v1.26.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_cronexpr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/cronexpr",
        sha256 = "d4a26ea051d2e1c3518ae9bae405db83f91b4b3bf2cb9fec903aff10e447cfa7",
        strip_prefix = "github.com/hashicorp/cronexpr@v1.1.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/cronexpr/com_github_hashicorp_cronexpr-v1.1.2.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/cronexpr/com_github_hashicorp_cronexpr-v1.1.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/cronexpr/com_github_hashicorp_cronexpr-v1.1.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/cronexpr/com_github_hashicorp_cronexpr-v1.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_errwrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/errwrap",
        sha256 = "209ae99bc039443e28e4d6bb66517d1756d9468b7578d31f1b63a28103d8e18c",
        strip_prefix = "github.com/hashicorp/errwrap@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/errwrap/com_github_hashicorp_errwrap-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/errwrap/com_github_hashicorp_errwrap-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/errwrap/com_github_hashicorp_errwrap-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/errwrap/com_github_hashicorp_errwrap-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_cleanhttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-cleanhttp",
        sha256 = "e9f3dcfcb33172ba499b4f8e888169252d7f1e072082182124a6e2053523f7df",
        strip_prefix = "github.com/hashicorp/go-cleanhttp@v0.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-cleanhttp/com_github_hashicorp_go_cleanhttp-v0.5.2.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-cleanhttp/com_github_hashicorp_go_cleanhttp-v0.5.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-cleanhttp/com_github_hashicorp_go_cleanhttp-v0.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-cleanhttp/com_github_hashicorp_go_cleanhttp-v0.5.2.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_hclog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-hclog",
        sha256 = "37eae99309c542b32aa7e28bcd0236e1ded8acce4aadc25d8e5a8ab03066482d",
        strip_prefix = "github.com/hashicorp/go-hclog@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-hclog/com_github_hashicorp_go_hclog-v1.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-hclog/com_github_hashicorp_go_hclog-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-hclog/com_github_hashicorp_go_hclog-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-hclog/com_github_hashicorp_go_hclog-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_immutable_radix",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-immutable-radix",
        sha256 = "47f3d79b57082d5db3f966547ad4de2a00544dfb362790fbf2cef1a161b4de3f",
        strip_prefix = "github.com/hashicorp/go-immutable-radix@v1.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-immutable-radix/com_github_hashicorp_go_immutable_radix-v1.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-immutable-radix/com_github_hashicorp_go_immutable_radix-v1.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-immutable-radix/com_github_hashicorp_go_immutable_radix-v1.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-immutable-radix/com_github_hashicorp_go_immutable_radix-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_multierror",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-multierror",
        sha256 = "972cd841ee51fdeac69c5a301e57f8ea27aebf15fddd7f621d5c240f28c3000c",
        strip_prefix = "github.com/hashicorp/go-multierror@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-multierror/com_github_hashicorp_go_multierror-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-multierror/com_github_hashicorp_go_multierror-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-multierror/com_github_hashicorp_go_multierror-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-multierror/com_github_hashicorp_go_multierror-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_retryablehttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-retryablehttp",
        sha256 = "00f6d85c5c8b327f56d49ad48ef1d2df94affea340ca46ce827415ba75db4712",
        strip_prefix = "github.com/hashicorp/go-retryablehttp@v0.7.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-retryablehttp/com_github_hashicorp_go_retryablehttp-v0.7.4.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-retryablehttp/com_github_hashicorp_go_retryablehttp-v0.7.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-retryablehttp/com_github_hashicorp_go_retryablehttp-v0.7.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-retryablehttp/com_github_hashicorp_go_retryablehttp-v0.7.4.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_rootcerts",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-rootcerts",
        sha256 = "864a48e642e87a273fb5ef60bb3575bd74a7090510f93143163fa6700be31948",
        strip_prefix = "github.com/hashicorp/go-rootcerts@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-rootcerts/com_github_hashicorp_go_rootcerts-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-rootcerts/com_github_hashicorp_go_rootcerts-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-rootcerts/com_github_hashicorp_go_rootcerts-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-rootcerts/com_github_hashicorp_go_rootcerts-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-uuid",
        sha256 = "30e419ebb4658e789be8ef1f5629faccc15d6571c6914a51afdcbaf74a5862b8",
        strip_prefix = "github.com/hashicorp/go-uuid@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-uuid/com_github_hashicorp_go_uuid-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-uuid/com_github_hashicorp_go_uuid-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-uuid/com_github_hashicorp_go_uuid-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-uuid/com_github_hashicorp_go_uuid-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_version",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-version",
        sha256 = "bf1d96bda50abf5e2d111bf99d220d978314907d815fd58f4bd4770dc7959b9e",
        strip_prefix = "github.com/hashicorp/go-version@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-version/com_github_hashicorp_go_version-v1.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-version/com_github_hashicorp_go_version-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-version/com_github_hashicorp_go_version-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-version/com_github_hashicorp_go_version-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_golang_lru",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/golang-lru",
        sha256 = "75a21bee633745563dc3161386b2245fc126f882d2e5d2d97c0c6899511a5faf",
        strip_prefix = "github.com/hashicorp/golang-lru@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/golang-lru/com_github_hashicorp_golang_lru-v0.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/golang-lru/com_github_hashicorp_golang_lru-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/golang-lru/com_github_hashicorp_golang_lru-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/golang-lru/com_github_hashicorp_golang_lru-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_hcl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/hcl",
        sha256 = "54149a2e5121b3e81f961c79210e63d6798eb63de28d2599ee59ade1fa76c82b",
        strip_prefix = "github.com/hashicorp/hcl@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/hcl/com_github_hashicorp_hcl-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/hcl/com_github_hashicorp_hcl-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/hcl/com_github_hashicorp_hcl-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/hcl/com_github_hashicorp_hcl-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_nomad_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/nomad/api",
        sha256 = "84f0a19132b5bc9b3694d113d0bd551a605717728f6b13a474db9bfbec502bc6",
        strip_prefix = "github.com/hashicorp/nomad/api@v0.0.0-20230721134942-515895c7690c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/nomad/api/com_github_hashicorp_nomad_api-v0.0.0-20230721134942-515895c7690c.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/nomad/api/com_github_hashicorp_nomad_api-v0.0.0-20230721134942-515895c7690c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/nomad/api/com_github_hashicorp_nomad_api-v0.0.0-20230721134942-515895c7690c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/nomad/api/com_github_hashicorp_nomad_api-v0.0.0-20230721134942-515895c7690c.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_serf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/serf",
        sha256 = "661b6ad5df497dcda0f581607b003e40646ef9f3ca09d12bdeec7cb3d16ad370",
        strip_prefix = "github.com/hashicorp/serf@v0.10.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/serf/com_github_hashicorp_serf-v0.10.1.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/serf/com_github_hashicorp_serf-v0.10.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/serf/com_github_hashicorp_serf-v0.10.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/serf/com_github_hashicorp_serf-v0.10.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hdrhistogram_hdrhistogram_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/HdrHistogram/hdrhistogram-go",
        sha256 = "bbc1d64d3179248c78ffa3729ad2ab696ed1ff14874f37d8d4fc4a5a235fa77f",
        strip_prefix = "github.com/HdrHistogram/hdrhistogram-go@v1.1.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/HdrHistogram/hdrhistogram-go/com_github_hdrhistogram_hdrhistogram_go-v1.1.2.zip",
            "http://ats.apps.svc/gomod/github.com/HdrHistogram/hdrhistogram-go/com_github_hdrhistogram_hdrhistogram_go-v1.1.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/HdrHistogram/hdrhistogram-go/com_github_hdrhistogram_hdrhistogram_go-v1.1.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/HdrHistogram/hdrhistogram-go/com_github_hdrhistogram_hdrhistogram_go-v1.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_hetznercloud_hcloud_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hetznercloud/hcloud-go/v2",
        sha256 = "71e2f7c3acd1b9b8838ce91b16baf302bb39684b03af90f1f710d4917d754ca2",
        strip_prefix = "github.com/hetznercloud/hcloud-go/v2@v2.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hetznercloud/hcloud-go/v2/com_github_hetznercloud_hcloud_go_v2-v2.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/hetznercloud/hcloud-go/v2/com_github_hetznercloud_hcloud_go_v2-v2.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hetznercloud/hcloud-go/v2/com_github_hetznercloud_hcloud_go_v2-v2.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hetznercloud/hcloud-go/v2/com_github_hetznercloud_hcloud_go_v2-v2.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hexops_gotextdiff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hexops/gotextdiff",
        sha256 = "a10c3942f09bc5132268d22d4bb9d0c1849122d533fe8cdf65ea69da05cebbaf",
        strip_prefix = "github.com/hexops/gotextdiff@v1.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hexops/gotextdiff/com_github_hexops_gotextdiff-v1.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/hexops/gotextdiff/com_github_hexops_gotextdiff-v1.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hexops/gotextdiff/com_github_hexops_gotextdiff-v1.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hexops/gotextdiff/com_github_hexops_gotextdiff-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_hpcloud_tail",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hpcloud/tail",
        sha256 = "3cba484748e2e2919d72663599b8cc6454058976fbca96f9ac78d84f195b922a",
        strip_prefix = "github.com/hpcloud/tail@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hpcloud/tail/com_github_hpcloud_tail-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/hpcloud/tail/com_github_hpcloud_tail-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hpcloud/tail/com_github_hpcloud_tail-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hpcloud/tail/com_github_hpcloud_tail-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_huandu_xstrings",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/huandu/xstrings",
        sha256 = "20b20f552a0eba0c3cf6aa1c9ed109fe0ab894a966477491267f21150856c6fc",
        strip_prefix = "github.com/huandu/xstrings@v1.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/huandu/xstrings/com_github_huandu_xstrings-v1.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/huandu/xstrings/com_github_huandu_xstrings-v1.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/huandu/xstrings/com_github_huandu_xstrings-v1.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/huandu/xstrings/com_github_huandu_xstrings-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hydrogen18_memlistener",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hydrogen18/memlistener",
        sha256 = "6e4ca24d3d49677a6f6378f7a6052f22f6defd45fb0f1f89ac17193993a3964b",
        strip_prefix = "github.com/hydrogen18/memlistener@v0.0.0-20141126152155-54553eb933fb",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hydrogen18/memlistener/com_github_hydrogen18_memlistener-v0.0.0-20141126152155-54553eb933fb.zip",
            "http://ats.apps.svc/gomod/github.com/hydrogen18/memlistener/com_github_hydrogen18_memlistener-v0.0.0-20141126152155-54553eb933fb.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hydrogen18/memlistener/com_github_hydrogen18_memlistener-v0.0.0-20141126152155-54553eb933fb.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hydrogen18/memlistener/com_github_hydrogen18_memlistener-v0.0.0-20141126152155-54553eb933fb.zip",
        ],
    )
    go_repository(
        name = "com_github_ianlancetaylor_demangle",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ianlancetaylor/demangle",
        sha256 = "b6426a32f7d0525c6a6012a5be7b14ba57a59810d949fadb3bfec22f66604cac",
        strip_prefix = "github.com/ianlancetaylor/demangle@v0.0.0-20230524184225-eabc099b10ab",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ianlancetaylor/demangle/com_github_ianlancetaylor_demangle-v0.0.0-20230524184225-eabc099b10ab.zip",
            "http://ats.apps.svc/gomod/github.com/ianlancetaylor/demangle/com_github_ianlancetaylor_demangle-v0.0.0-20230524184225-eabc099b10ab.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ianlancetaylor/demangle/com_github_ianlancetaylor_demangle-v0.0.0-20230524184225-eabc099b10ab.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ianlancetaylor/demangle/com_github_ianlancetaylor_demangle-v0.0.0-20230524184225-eabc099b10ab.zip",
        ],
    )
    go_repository(
        name = "com_github_imdario_mergo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/imdario/mergo",
        sha256 = "536b0b87ec2b9f02d759a3a01604043b538e15e62924a29e34cfc2b16a1cf580",
        strip_prefix = "github.com/imdario/mergo@v0.3.16",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/imdario/mergo/com_github_imdario_mergo-v0.3.16.zip",
            "http://ats.apps.svc/gomod/github.com/imdario/mergo/com_github_imdario_mergo-v0.3.16.zip",
            "https://cache.hawkingrei.com/gomod/github.com/imdario/mergo/com_github_imdario_mergo-v0.3.16.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/imdario/mergo/com_github_imdario_mergo-v0.3.16.zip",
        ],
    )
    go_repository(
        name = "com_github_imkira_go_interpol",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/imkira/go-interpol",
        sha256 = "de5111f7694700ea056beeb7c1ca1a827075d423422f251076ee17bd869477d9",
        strip_prefix = "github.com/imkira/go-interpol@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/imkira/go-interpol/com_github_imkira_go_interpol-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/imkira/go-interpol/com_github_imkira_go_interpol-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/imkira/go-interpol/com_github_imkira_go_interpol-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/imkira/go-interpol/com_github_imkira_go_interpol-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_inconshreveable_mousetrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/inconshreveable/mousetrap",
        sha256 = "526674de624d7db108cfe7653ef110ccdfd97bc85026254224815567928ed243",
        strip_prefix = "github.com/inconshreveable/mousetrap@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/inconshreveable/mousetrap/com_github_inconshreveable_mousetrap-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/inconshreveable/mousetrap/com_github_inconshreveable_mousetrap-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/inconshreveable/mousetrap/com_github_inconshreveable_mousetrap-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/inconshreveable/mousetrap/com_github_inconshreveable_mousetrap-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_tdigest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/tdigest",
        sha256 = "849177b840452dee7b1986b962c5612f75a56036af4cb42cbf227113c50b3dc4",
        strip_prefix = "github.com/influxdata/tdigest@v0.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/influxdata/tdigest/com_github_influxdata_tdigest-v0.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/influxdata/tdigest/com_github_influxdata_tdigest-v0.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/influxdata/tdigest/com_github_influxdata_tdigest-v0.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/influxdata/tdigest/com_github_influxdata_tdigest-v0.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_ionos_cloud_sdk_go_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ionos-cloud/sdk-go/v6",
        sha256 = "6ffa828ff99194d52be83c7b6a62453c9189c0ed8e4543589253d8ccfdc94f1e",
        strip_prefix = "github.com/ionos-cloud/sdk-go/v6@v6.1.10",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ionos-cloud/sdk-go/v6/com_github_ionos_cloud_sdk_go_v6-v6.1.10.zip",
            "http://ats.apps.svc/gomod/github.com/ionos-cloud/sdk-go/v6/com_github_ionos_cloud_sdk_go_v6-v6.1.10.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ionos-cloud/sdk-go/v6/com_github_ionos_cloud_sdk_go_v6-v6.1.10.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ionos-cloud/sdk-go/v6/com_github_ionos_cloud_sdk_go_v6-v6.1.10.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_blackfriday",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/blackfriday",
        sha256 = "936679f49251da75fde84b8f38884dbce89747b96f8206f7a4675bfcc7dd165d",
        strip_prefix = "github.com/iris-contrib/blackfriday@v2.0.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/iris-contrib/blackfriday/com_github_iris_contrib_blackfriday-v2.0.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/iris-contrib/blackfriday/com_github_iris_contrib_blackfriday-v2.0.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/iris-contrib/blackfriday/com_github_iris_contrib_blackfriday-v2.0.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/iris-contrib/blackfriday/com_github_iris_contrib_blackfriday-v2.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_go_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/go.uuid",
        sha256 = "c6bae86643c2d6047c68c25226a1e75c5331c03466532ee6c943705743949bd9",
        strip_prefix = "github.com/iris-contrib/go.uuid@v2.0.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/iris-contrib/go.uuid/com_github_iris_contrib_go_uuid-v2.0.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/iris-contrib/go.uuid/com_github_iris_contrib_go_uuid-v2.0.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/iris-contrib/go.uuid/com_github_iris_contrib_go_uuid-v2.0.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/iris-contrib/go.uuid/com_github_iris_contrib_go_uuid-v2.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_i18n",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/i18n",
        sha256 = "f9d637c5c887210f906f1228682239d98312b99e6d5192bec64faf597a3bab9c",
        strip_prefix = "github.com/iris-contrib/i18n@v0.0.0-20171121225848-987a633949d0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/iris-contrib/i18n/com_github_iris_contrib_i18n-v0.0.0-20171121225848-987a633949d0.zip",
            "http://ats.apps.svc/gomod/github.com/iris-contrib/i18n/com_github_iris_contrib_i18n-v0.0.0-20171121225848-987a633949d0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/iris-contrib/i18n/com_github_iris_contrib_i18n-v0.0.0-20171121225848-987a633949d0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/iris-contrib/i18n/com_github_iris_contrib_i18n-v0.0.0-20171121225848-987a633949d0.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_schema",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/schema",
        sha256 = "d0887d45474f3aa30ff0fd329e98341e795be2c6e861bd92c30a7f97f6e57385",
        strip_prefix = "github.com/iris-contrib/schema@v0.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/iris-contrib/schema/com_github_iris_contrib_schema-v0.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/iris-contrib/schema/com_github_iris_contrib_schema-v0.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/iris-contrib/schema/com_github_iris_contrib_schema-v0.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/iris-contrib/schema/com_github_iris_contrib_schema-v0.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_aescts_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/aescts/v2",
        sha256 = "717a211ad4aac248cf33cadde73059c13f8e9462123a0ab2fed5c5e61f7739d7",
        strip_prefix = "github.com/jcmturner/aescts/v2@v2.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jcmturner/aescts/v2/com_github_jcmturner_aescts_v2-v2.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/jcmturner/aescts/v2/com_github_jcmturner_aescts_v2-v2.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jcmturner/aescts/v2/com_github_jcmturner_aescts_v2-v2.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jcmturner/aescts/v2/com_github_jcmturner_aescts_v2-v2.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_dnsutils_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/dnsutils/v2",
        sha256 = "f9188186b672e547cfaef66107aa62d65054c5d4f10d4dcd1ff157d6bf8c275d",
        strip_prefix = "github.com/jcmturner/dnsutils/v2@v2.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jcmturner/dnsutils/v2/com_github_jcmturner_dnsutils_v2-v2.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/jcmturner/dnsutils/v2/com_github_jcmturner_dnsutils_v2-v2.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jcmturner/dnsutils/v2/com_github_jcmturner_dnsutils_v2-v2.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jcmturner/dnsutils/v2/com_github_jcmturner_dnsutils_v2-v2.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_gofork",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/gofork",
        sha256 = "5e015dd9b038f1dded0b2ded77e529d2f6ba0bed228a98831af5a3610eefcb52",
        strip_prefix = "github.com/jcmturner/gofork@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jcmturner/gofork/com_github_jcmturner_gofork-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/jcmturner/gofork/com_github_jcmturner_gofork-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jcmturner/gofork/com_github_jcmturner_gofork-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jcmturner/gofork/com_github_jcmturner_gofork-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_goidentity_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/goidentity/v6",
        sha256 = "243e6fd6ea9f3094eea32c55febade6d8aaa1b563db655b0c5327940e4719beb",
        strip_prefix = "github.com/jcmturner/goidentity/v6@v6.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jcmturner/goidentity/v6/com_github_jcmturner_goidentity_v6-v6.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/jcmturner/goidentity/v6/com_github_jcmturner_goidentity_v6-v6.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jcmturner/goidentity/v6/com_github_jcmturner_goidentity_v6-v6.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jcmturner/goidentity/v6/com_github_jcmturner_goidentity_v6-v6.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_gokrb5_v8",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/gokrb5/v8",
        sha256 = "eecd7120363321bb6b58b015395089958720271b3211659d802447d417af5970",
        strip_prefix = "github.com/jcmturner/gokrb5/v8@v8.4.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jcmturner/gokrb5/v8/com_github_jcmturner_gokrb5_v8-v8.4.2.zip",
            "http://ats.apps.svc/gomod/github.com/jcmturner/gokrb5/v8/com_github_jcmturner_gokrb5_v8-v8.4.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jcmturner/gokrb5/v8/com_github_jcmturner_gokrb5_v8-v8.4.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jcmturner/gokrb5/v8/com_github_jcmturner_gokrb5_v8-v8.4.2.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_rpc_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/rpc/v2",
        sha256 = "90c595355e5e2c9dc1e1ae71a88491a04c34d8791180098da103217cbf5f5574",
        strip_prefix = "github.com/jcmturner/rpc/v2@v2.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jcmturner/rpc/v2/com_github_jcmturner_rpc_v2-v2.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/jcmturner/rpc/v2/com_github_jcmturner_rpc_v2-v2.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jcmturner/rpc/v2/com_github_jcmturner_rpc_v2-v2.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jcmturner/rpc/v2/com_github_jcmturner_rpc_v2-v2.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_jedib0t_go_pretty_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jedib0t/go-pretty/v6",
        sha256 = "4d1f6a514d7efa48c0fae2d70ee0a5510fe2b73cf7e3460f3f75f545bff6374c",
        strip_prefix = "github.com/jedib0t/go-pretty/v6@v6.2.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jedib0t/go-pretty/v6/com_github_jedib0t_go_pretty_v6-v6.2.2.zip",
            "http://ats.apps.svc/gomod/github.com/jedib0t/go-pretty/v6/com_github_jedib0t_go_pretty_v6-v6.2.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jedib0t/go-pretty/v6/com_github_jedib0t_go_pretty_v6-v6.2.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jedib0t/go-pretty/v6/com_github_jedib0t_go_pretty_v6-v6.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_jellydator_ttlcache_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jellydator/ttlcache/v3",
        sha256 = "75cabcc118414bc9e42cef6769fffc0c500954f2ef1988a3797aee0f4351f306",
        strip_prefix = "github.com/jellydator/ttlcache/v3@v3.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jellydator/ttlcache/v3/com_github_jellydator_ttlcache_v3-v3.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/jellydator/ttlcache/v3/com_github_jellydator_ttlcache_v3-v3.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jellydator/ttlcache/v3/com_github_jellydator_ttlcache_v3-v3.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jellydator/ttlcache/v3/com_github_jellydator_ttlcache_v3-v3.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jfcg_opt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jfcg/opt",
        sha256 = "d774e375f6827f16be051e177407bb3217e051cf4285449788abff3ebf01a468",
        strip_prefix = "github.com/jfcg/opt@v0.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jfcg/opt/com_github_jfcg_opt-v0.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/jfcg/opt/com_github_jfcg_opt-v0.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jfcg/opt/com_github_jfcg_opt-v0.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jfcg/opt/com_github_jfcg_opt-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jfcg_rng",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jfcg/rng",
        sha256 = "8c450d237ea8ba22b6bc6337dcd11519fd6d04c2b31b9b38e5c26ae04ed7cae8",
        strip_prefix = "github.com/jfcg/rng@v1.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jfcg/rng/com_github_jfcg_rng-v1.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/jfcg/rng/com_github_jfcg_rng-v1.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jfcg/rng/com_github_jfcg_rng-v1.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jfcg/rng/com_github_jfcg_rng-v1.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_jfcg_sixb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jfcg/sixb",
        sha256 = "5eda29ec69dff767e5353325eab7b2edac5911fcbbaf84f42c0513ab50b76952",
        strip_prefix = "github.com/jfcg/sixb@v1.3.8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jfcg/sixb/com_github_jfcg_sixb-v1.3.8.zip",
            "http://ats.apps.svc/gomod/github.com/jfcg/sixb/com_github_jfcg_sixb-v1.3.8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jfcg/sixb/com_github_jfcg_sixb-v1.3.8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jfcg/sixb/com_github_jfcg_sixb-v1.3.8.zip",
        ],
    )
    go_repository(
        name = "com_github_jfcg_sorty_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jfcg/sorty/v2",
        sha256 = "4a126a66ee9237c696a038eff39710b55d92a846f807a5005dde35f4a0b869e3",
        strip_prefix = "github.com/jfcg/sorty/v2@v2.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jfcg/sorty/v2/com_github_jfcg_sorty_v2-v2.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/jfcg/sorty/v2/com_github_jfcg_sorty_v2-v2.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jfcg/sorty/v2/com_github_jfcg_sorty_v2-v2.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jfcg/sorty/v2/com_github_jfcg_sorty_v2-v2.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jgautheron_goconst",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jgautheron/goconst",
        sha256 = "4b79cb5259fdd5a7a7288b19ca9d45133bca338d4b9edd3ca142bd17fd252b02",
        strip_prefix = "github.com/jgautheron/goconst@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jgautheron/goconst/com_github_jgautheron_goconst-v1.7.0.zip",
            "http://ats.apps.svc/gomod/github.com/jgautheron/goconst/com_github_jgautheron_goconst-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jgautheron/goconst/com_github_jgautheron_goconst-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jgautheron/goconst/com_github_jgautheron_goconst-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jingyugao_rowserrcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jingyugao/rowserrcheck",
        sha256 = "500e58a8a78797fd1c470f397d6c23116861bd38d7a66fdbfe7e3fee7a7f8a6c",
        strip_prefix = "github.com/jingyugao/rowserrcheck@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jingyugao/rowserrcheck/com_github_jingyugao_rowserrcheck-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/jingyugao/rowserrcheck/com_github_jingyugao_rowserrcheck-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jingyugao/rowserrcheck/com_github_jingyugao_rowserrcheck-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jingyugao/rowserrcheck/com_github_jingyugao_rowserrcheck-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jirfag_go_printf_func_name",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jirfag/go-printf-func-name",
        sha256 = "013b28c5c829165bd768e75784d7a8bbdfd0b0bb6ca1549539f94b9d6a6000fe",
        strip_prefix = "github.com/jirfag/go-printf-func-name@v0.0.0-20200119135958-7558a9eaa5af",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jirfag/go-printf-func-name/com_github_jirfag_go_printf_func_name-v0.0.0-20200119135958-7558a9eaa5af.zip",
            "http://ats.apps.svc/gomod/github.com/jirfag/go-printf-func-name/com_github_jirfag_go_printf_func_name-v0.0.0-20200119135958-7558a9eaa5af.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jirfag/go-printf-func-name/com_github_jirfag_go_printf_func_name-v0.0.0-20200119135958-7558a9eaa5af.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jirfag/go-printf-func-name/com_github_jirfag_go_printf_func_name-v0.0.0-20200119135958-7558a9eaa5af.zip",
        ],
    )
    go_repository(
        name = "com_github_jjti_go_spancheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jjti/go-spancheck",
        sha256 = "76f1a87e6bb98dcce77f89109fe71aec1c701e5a87b788bdad8f172244a9c15c",
        strip_prefix = "github.com/jjti/go-spancheck@v0.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jjti/go-spancheck/com_github_jjti_go_spancheck-v0.5.2.zip",
            "http://ats.apps.svc/gomod/github.com/jjti/go-spancheck/com_github_jjti_go_spancheck-v0.5.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jjti/go-spancheck/com_github_jjti_go_spancheck-v0.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jjti/go-spancheck/com_github_jjti_go_spancheck-v0.5.2.zip",
        ],
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jmespath/go-jmespath",
        sha256 = "d1f77b6790d7c4321a74260f3675683d3ac06b0a614b5f83e870beae0a8b2867",
        strip_prefix = "github.com/jmespath/go-jmespath@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jmespath/go-jmespath/com_github_jmespath_go_jmespath-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/jmespath/go-jmespath/com_github_jmespath_go_jmespath-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jmespath/go-jmespath/com_github_jmespath_go_jmespath-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jmespath/go-jmespath/com_github_jmespath_go_jmespath-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath_internal_testify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jmespath/go-jmespath/internal/testify",
        sha256 = "338f73832eb2a63ab0c912197e653c7b62426fc4387e0a76ab0d43c65e29b3e1",
        strip_prefix = "github.com/jmespath/go-jmespath/internal/testify@v1.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jmespath/go-jmespath/internal/testify/com_github_jmespath_go_jmespath_internal_testify-v1.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/jmespath/go-jmespath/internal/testify/com_github_jmespath_go_jmespath_internal_testify-v1.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jmespath/go-jmespath/internal/testify/com_github_jmespath_go_jmespath_internal_testify-v1.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jmespath/go-jmespath/internal/testify/com_github_jmespath_go_jmespath_internal_testify-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_johannesboyne_gofakes3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/johannesboyne/gofakes3",
        sha256 = "b0ba2f7ee1765c24d88f2c5c3d478992f03d40c72531d3725696baa5fdad4a73",
        strip_prefix = "github.com/johannesboyne/gofakes3@v0.0.0-20230506070712-04da935ef877",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/johannesboyne/gofakes3/com_github_johannesboyne_gofakes3-v0.0.0-20230506070712-04da935ef877.zip",
            "http://ats.apps.svc/gomod/github.com/johannesboyne/gofakes3/com_github_johannesboyne_gofakes3-v0.0.0-20230506070712-04da935ef877.zip",
            "https://cache.hawkingrei.com/gomod/github.com/johannesboyne/gofakes3/com_github_johannesboyne_gofakes3-v0.0.0-20230506070712-04da935ef877.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/johannesboyne/gofakes3/com_github_johannesboyne_gofakes3-v0.0.0-20230506070712-04da935ef877.zip",
        ],
    )
    go_repository(
        name = "com_github_joho_sqltocsv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/joho/sqltocsv",
        sha256 = "62bb4ce0bd45a58c294aecdbfe256437747102de4bbe684c84322091661f1122",
        strip_prefix = "github.com/joho/sqltocsv@v0.0.0-20210428211105-a6d6801d59df",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/joho/sqltocsv/com_github_joho_sqltocsv-v0.0.0-20210428211105-a6d6801d59df.zip",
            "http://ats.apps.svc/gomod/github.com/joho/sqltocsv/com_github_joho_sqltocsv-v0.0.0-20210428211105-a6d6801d59df.zip",
            "https://cache.hawkingrei.com/gomod/github.com/joho/sqltocsv/com_github_joho_sqltocsv-v0.0.0-20210428211105-a6d6801d59df.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/joho/sqltocsv/com_github_joho_sqltocsv-v0.0.0-20210428211105-a6d6801d59df.zip",
        ],
    )
    go_repository(
        name = "com_github_joker_hpp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Joker/hpp",
        sha256 = "790dc3cfb8e51ff22f29d74b5b58782999e267e86290bc2b52485ccf9c8d2792",
        strip_prefix = "github.com/Joker/hpp@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Joker/hpp/com_github_joker_hpp-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/Joker/hpp/com_github_joker_hpp-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Joker/hpp/com_github_joker_hpp-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Joker/hpp/com_github_joker_hpp-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_joker_jade",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Joker/jade",
        sha256 = "3fc31c80e93cb053cd4fce60a7288e3760f9fe5c571ec4c2d32c4f9bf6c487e7",
        strip_prefix = "github.com/Joker/jade@v1.0.1-0.20190614124447-d475f43051e7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Joker/jade/com_github_joker_jade-v1.0.1-0.20190614124447-d475f43051e7.zip",
            "http://ats.apps.svc/gomod/github.com/Joker/jade/com_github_joker_jade-v1.0.1-0.20190614124447-d475f43051e7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Joker/jade/com_github_joker_jade-v1.0.1-0.20190614124447-d475f43051e7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Joker/jade/com_github_joker_jade-v1.0.1-0.20190614124447-d475f43051e7.zip",
        ],
    )
    go_repository(
        name = "com_github_jonboulle_clockwork",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jonboulle/clockwork",
        sha256 = "20b3f45f363d05fce53618c059c9251860a5f3411f8d71b8b85b4e35b9060294",
        strip_prefix = "github.com/jonboulle/clockwork@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jonboulle/clockwork/com_github_jonboulle_clockwork-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/jonboulle/clockwork/com_github_jonboulle_clockwork-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jonboulle/clockwork/com_github_jonboulle_clockwork-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jonboulle/clockwork/com_github_jonboulle_clockwork-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_josharian_intern",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/josharian/intern",
        sha256 = "5679bfd11c14adccdb45bd1a0f9cf4b445b95caeed6fb507ba96ecced11c248d",
        strip_prefix = "github.com/josharian/intern@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/josharian/intern/com_github_josharian_intern-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/josharian/intern/com_github_josharian_intern-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/josharian/intern/com_github_josharian_intern-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/josharian/intern/com_github_josharian_intern-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jpillora_backoff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jpillora/backoff",
        sha256 = "f856692c725143c49b9cceabfbca8bc93d3dbde84a0aaa53fb26ed3774c220cc",
        strip_prefix = "github.com/jpillora/backoff@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jpillora/backoff/com_github_jpillora_backoff-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/jpillora/backoff/com_github_jpillora_backoff-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jpillora/backoff/com_github_jpillora_backoff-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jpillora/backoff/com_github_jpillora_backoff-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_json_iterator_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/json-iterator/go",
        sha256 = "d001ea57081afd0e378467c8f4a9b6a51259996bb8bb763f78107eaf12f99501",
        strip_prefix = "github.com/json-iterator/go@v1.1.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/json-iterator/go/com_github_json_iterator_go-v1.1.12.zip",
            "http://ats.apps.svc/gomod/github.com/json-iterator/go/com_github_json_iterator_go-v1.1.12.zip",
            "https://cache.hawkingrei.com/gomod/github.com/json-iterator/go/com_github_json_iterator_go-v1.1.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/json-iterator/go/com_github_json_iterator_go-v1.1.12.zip",
        ],
    )
    go_repository(
        name = "com_github_jstemmer_go_junit_report",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jstemmer/go-junit-report",
        sha256 = "fbd2196e4a50a88f8c352f76325f4ba72338ecec7b6cb7535317ce9e3aa40284",
        strip_prefix = "github.com/jstemmer/go-junit-report@v0.9.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jstemmer/go-junit-report/com_github_jstemmer_go_junit_report-v0.9.1.zip",
            "http://ats.apps.svc/gomod/github.com/jstemmer/go-junit-report/com_github_jstemmer_go_junit_report-v0.9.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jstemmer/go-junit-report/com_github_jstemmer_go_junit_report-v0.9.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jstemmer/go-junit-report/com_github_jstemmer_go_junit_report-v0.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jtolds_gls",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jtolds/gls",
        sha256 = "2f51f8cb610e846dc4bd9b3c0fbf6bebab24bb06d866db7804e123a61b0bd9ec",
        strip_prefix = "github.com/jtolds/gls@v4.20.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jtolds/gls/com_github_jtolds_gls-v4.20.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/jtolds/gls/com_github_jtolds_gls-v4.20.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jtolds/gls/com_github_jtolds_gls-v4.20.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jtolds/gls/com_github_jtolds_gls-v4.20.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_juju_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/juju/errors",
        sha256 = "b97a8b6ca1e8cc6fba715b4187a25d9ae53122edbdcaf1154e36249e6d297393",
        strip_prefix = "github.com/juju/errors@v0.0.0-20181118221551-089d3ea4e4d5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/juju/errors/com_github_juju_errors-v0.0.0-20181118221551-089d3ea4e4d5.zip",
            "http://ats.apps.svc/gomod/github.com/juju/errors/com_github_juju_errors-v0.0.0-20181118221551-089d3ea4e4d5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/juju/errors/com_github_juju_errors-v0.0.0-20181118221551-089d3ea4e4d5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/juju/errors/com_github_juju_errors-v0.0.0-20181118221551-089d3ea4e4d5.zip",
        ],
    )
    go_repository(
        name = "com_github_juju_loggo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/juju/loggo",
        sha256 = "64c21c4a3810a5d0e940fd11a46daa08bde2f951b59fb330ff06ab9634cc4863",
        strip_prefix = "github.com/juju/loggo@v0.0.0-20180524022052-584905176618",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/juju/loggo/com_github_juju_loggo-v0.0.0-20180524022052-584905176618.zip",
            "http://ats.apps.svc/gomod/github.com/juju/loggo/com_github_juju_loggo-v0.0.0-20180524022052-584905176618.zip",
            "https://cache.hawkingrei.com/gomod/github.com/juju/loggo/com_github_juju_loggo-v0.0.0-20180524022052-584905176618.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/juju/loggo/com_github_juju_loggo-v0.0.0-20180524022052-584905176618.zip",
        ],
    )
    go_repository(
        name = "com_github_juju_testing",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/juju/testing",
        sha256 = "a66b521a6b60dd443b86a42d0274209e385d7f3e71db775b8c2000bcfd0c6649",
        strip_prefix = "github.com/juju/testing@v0.0.0-20180920084828-472a3e8b2073",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/juju/testing/com_github_juju_testing-v0.0.0-20180920084828-472a3e8b2073.zip",
            "http://ats.apps.svc/gomod/github.com/juju/testing/com_github_juju_testing-v0.0.0-20180920084828-472a3e8b2073.zip",
            "https://cache.hawkingrei.com/gomod/github.com/juju/testing/com_github_juju_testing-v0.0.0-20180920084828-472a3e8b2073.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/juju/testing/com_github_juju_testing-v0.0.0-20180920084828-472a3e8b2073.zip",
        ],
    )
    go_repository(
        name = "com_github_julienschmidt_httprouter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/julienschmidt/httprouter",
        sha256 = "e457dccd7015f340664e3b8cfd41997471382da2f4a743ee55be539abc6ca1f9",
        strip_prefix = "github.com/julienschmidt/httprouter@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/julienschmidt/httprouter/com_github_julienschmidt_httprouter-v1.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/julienschmidt/httprouter/com_github_julienschmidt_httprouter-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/julienschmidt/httprouter/com_github_julienschmidt_httprouter-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/julienschmidt/httprouter/com_github_julienschmidt_httprouter-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_julz_importas",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/julz/importas",
        sha256 = "7039982a695bc0b40961257409aae243f9bb4aac256bea606166a3f7b6852d64",
        strip_prefix = "github.com/julz/importas@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/julz/importas/com_github_julz_importas-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/julz/importas/com_github_julz_importas-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/julz/importas/com_github_julz_importas-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/julz/importas/com_github_julz_importas-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jung_kurt_gofpdf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jung-kurt/gofpdf",
        sha256 = "f0fa70ade137185bbff2f016831a2a456eaadc8d14bc7bf24f0229211820c078",
        strip_prefix = "github.com/jung-kurt/gofpdf@v1.0.3-0.20190309125859-24315acbbda5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jung-kurt/gofpdf/com_github_jung_kurt_gofpdf-v1.0.3-0.20190309125859-24315acbbda5.zip",
            "http://ats.apps.svc/gomod/github.com/jung-kurt/gofpdf/com_github_jung_kurt_gofpdf-v1.0.3-0.20190309125859-24315acbbda5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jung-kurt/gofpdf/com_github_jung_kurt_gofpdf-v1.0.3-0.20190309125859-24315acbbda5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jung-kurt/gofpdf/com_github_jung_kurt_gofpdf-v1.0.3-0.20190309125859-24315acbbda5.zip",
        ],
    )
    go_repository(
        name = "com_github_k0kubun_colorstring",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/k0kubun/colorstring",
        sha256 = "32a2eac0ffb69c6882b32ccfcdd76968cb9dfee9d9dc3d469fc405775399167c",
        strip_prefix = "github.com/k0kubun/colorstring@v0.0.0-20150214042306-9440f1994b88",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/k0kubun/colorstring/com_github_k0kubun_colorstring-v0.0.0-20150214042306-9440f1994b88.zip",
            "http://ats.apps.svc/gomod/github.com/k0kubun/colorstring/com_github_k0kubun_colorstring-v0.0.0-20150214042306-9440f1994b88.zip",
            "https://cache.hawkingrei.com/gomod/github.com/k0kubun/colorstring/com_github_k0kubun_colorstring-v0.0.0-20150214042306-9440f1994b88.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/k0kubun/colorstring/com_github_k0kubun_colorstring-v0.0.0-20150214042306-9440f1994b88.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_golog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/golog",
        sha256 = "bb4d1476d5cbe33088190116a5af7b355fd62858127a8ea9d30d77701279350e",
        strip_prefix = "github.com/kataras/golog@v0.0.9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kataras/golog/com_github_kataras_golog-v0.0.9.zip",
            "http://ats.apps.svc/gomod/github.com/kataras/golog/com_github_kataras_golog-v0.0.9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kataras/golog/com_github_kataras_golog-v0.0.9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kataras/golog/com_github_kataras_golog-v0.0.9.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_iris_v12",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/iris/v12",
        sha256 = "0e51cdc209b22eeabde80d429051032f0599933a99534a123e5234e566a58d73",
        strip_prefix = "github.com/kataras/iris/v12@v12.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kataras/iris/v12/com_github_kataras_iris_v12-v12.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/kataras/iris/v12/com_github_kataras_iris_v12-v12.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kataras/iris/v12/com_github_kataras_iris_v12-v12.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kataras/iris/v12/com_github_kataras_iris_v12-v12.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_neffos",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/neffos",
        sha256 = "8eaa49fadd1994c1992cc95da15db3ea2a9651bf4faadc6b7706eb3c3313c758",
        strip_prefix = "github.com/kataras/neffos@v0.0.10",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kataras/neffos/com_github_kataras_neffos-v0.0.10.zip",
            "http://ats.apps.svc/gomod/github.com/kataras/neffos/com_github_kataras_neffos-v0.0.10.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kataras/neffos/com_github_kataras_neffos-v0.0.10.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kataras/neffos/com_github_kataras_neffos-v0.0.10.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_pio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/pio",
        sha256 = "70a50855f07ff59d96db9633a0cf729280a8b9f7af72b936fe8a28e48406432f",
        strip_prefix = "github.com/kataras/pio@v0.0.0-20190103105442-ea782b38602d",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kataras/pio/com_github_kataras_pio-v0.0.0-20190103105442-ea782b38602d.zip",
            "http://ats.apps.svc/gomod/github.com/kataras/pio/com_github_kataras_pio-v0.0.0-20190103105442-ea782b38602d.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kataras/pio/com_github_kataras_pio-v0.0.0-20190103105442-ea782b38602d.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kataras/pio/com_github_kataras_pio-v0.0.0-20190103105442-ea782b38602d.zip",
        ],
    )
    go_repository(
        name = "com_github_kisielk_errcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kisielk/errcheck",
        patch_args = ["-p1"],
        patches = [
            "//build/patches:com_github_kisielk_errcheck.patch",
        ],
        sha256 = "f394d1df1f2332387ce142d98734c5c44fb94e9a8a2af2a9b75aa4ec4a64b963",
        strip_prefix = "github.com/kisielk/errcheck@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kisielk/errcheck/com_github_kisielk_errcheck-v1.7.0.zip",
            "http://ats.apps.svc/gomod/github.com/kisielk/errcheck/com_github_kisielk_errcheck-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kisielk/errcheck/com_github_kisielk_errcheck-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kisielk/errcheck/com_github_kisielk_errcheck-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_kisielk_gotool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kisielk/gotool",
        sha256 = "089dbba6e3aa09944fdb40d72acc86694e8bdde01cfc0f40fe0248309eb80a3f",
        strip_prefix = "github.com/kisielk/gotool@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kisielk/gotool/com_github_kisielk_gotool-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/kisielk/gotool/com_github_kisielk_gotool-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kisielk/gotool/com_github_kisielk_gotool-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kisielk/gotool/com_github_kisielk_gotool-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_kkhaike_contextcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kkHAIKE/contextcheck",
        sha256 = "fe8eb6fa48a052d726deee01d2d05506a7cf653d52229b4970b3bdf7eac3aae6",
        strip_prefix = "github.com/kkHAIKE/contextcheck@v1.1.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kkHAIKE/contextcheck/com_github_kkhaike_contextcheck-v1.1.4.zip",
            "http://ats.apps.svc/gomod/github.com/kkHAIKE/contextcheck/com_github_kkhaike_contextcheck-v1.1.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kkHAIKE/contextcheck/com_github_kkhaike_contextcheck-v1.1.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kkHAIKE/contextcheck/com_github_kkhaike_contextcheck-v1.1.4.zip",
        ],
    )
    go_repository(
        name = "com_github_klauspost_compress",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/compress",
        sha256 = "dd1acc63c40bf36ccfb2a7a7dd46579ea67585e37f1d2dbb06026b56ef625903",
        strip_prefix = "github.com/klauspost/compress@v1.17.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/klauspost/compress/com_github_klauspost_compress-v1.17.4.zip",
            "http://ats.apps.svc/gomod/github.com/klauspost/compress/com_github_klauspost_compress-v1.17.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/klauspost/compress/com_github_klauspost_compress-v1.17.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/klauspost/compress/com_github_klauspost_compress-v1.17.4.zip",
        ],
    )
    go_repository(
        name = "com_github_klauspost_cpuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/cpuid",
        sha256 = "f61266e43d5c247fdb55d843e2d93974717c1052cba9f331b181f60c4cf687d9",
        strip_prefix = "github.com/klauspost/cpuid@v1.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/klauspost/cpuid/com_github_klauspost_cpuid-v1.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/klauspost/cpuid/com_github_klauspost_cpuid-v1.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/klauspost/cpuid/com_github_klauspost_cpuid-v1.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/klauspost/cpuid/com_github_klauspost_cpuid-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_kolo_xmlrpc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kolo/xmlrpc",
        sha256 = "310742360a864798a1bfce6db8604263574c0be502670c8bfedeab8fcbe9d191",
        strip_prefix = "github.com/kolo/xmlrpc@v0.0.0-20220921171641-a4b6fa1dd06b",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kolo/xmlrpc/com_github_kolo_xmlrpc-v0.0.0-20220921171641-a4b6fa1dd06b.zip",
            "http://ats.apps.svc/gomod/github.com/kolo/xmlrpc/com_github_kolo_xmlrpc-v0.0.0-20220921171641-a4b6fa1dd06b.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kolo/xmlrpc/com_github_kolo_xmlrpc-v0.0.0-20220921171641-a4b6fa1dd06b.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kolo/xmlrpc/com_github_kolo_xmlrpc-v0.0.0-20220921171641-a4b6fa1dd06b.zip",
        ],
    )
    go_repository(
        name = "com_github_konsorten_go_windows_terminal_sequences",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/konsorten/go-windows-terminal-sequences",
        sha256 = "7fd0273fc0855ed08172c150f756e708d6e43c4a6d52ca4939a8b43d03356091",
        strip_prefix = "github.com/konsorten/go-windows-terminal-sequences@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/konsorten/go-windows-terminal-sequences/com_github_konsorten_go_windows_terminal_sequences-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/konsorten/go-windows-terminal-sequences/com_github_konsorten_go_windows_terminal_sequences-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/konsorten/go-windows-terminal-sequences/com_github_konsorten_go_windows_terminal_sequences-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/konsorten/go-windows-terminal-sequences/com_github_konsorten_go_windows_terminal_sequences-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_kr_pretty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/pretty",
        sha256 = "ecf5a4af24826c3ad758ce06410ca08e2d58e4d95053be3b9dde2e14852c0cdc",
        strip_prefix = "github.com/kr/pretty@v0.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kr/pretty/com_github_kr_pretty-v0.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/kr/pretty/com_github_kr_pretty-v0.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kr/pretty/com_github_kr_pretty-v0.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kr/pretty/com_github_kr_pretty-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_kr_pty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/pty",
        sha256 = "10474d7a875cbd2b9d74c9bb8fb99264b7863f204c7610607797ff18d580bf00",
        strip_prefix = "github.com/kr/pty@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kr/pty/com_github_kr_pty-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/kr/pty/com_github_kr_pty-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kr/pty/com_github_kr_pty-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kr/pty/com_github_kr_pty-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_kr_text",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/text",
        sha256 = "368eb318f91a5b67be905c47032ab5c31a1d49a97848b1011a0d0a2122b30ba4",
        strip_prefix = "github.com/kr/text@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kr/text/com_github_kr_text-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/kr/text/com_github_kr_text-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kr/text/com_github_kr_text-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kr/text/com_github_kr_text-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ks3sdklib_aws_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ks3sdklib/aws-sdk-go",
        sha256 = "1edfac4a072a0180b308ddc1a9e96d51407e2e66573938e14e056ba6fef5bddb",
        strip_prefix = "github.com/ks3sdklib/aws-sdk-go@v1.2.9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ks3sdklib/aws-sdk-go/com_github_ks3sdklib_aws_sdk_go-v1.2.9.zip",
            "http://ats.apps.svc/gomod/github.com/ks3sdklib/aws-sdk-go/com_github_ks3sdklib_aws_sdk_go-v1.2.9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ks3sdklib/aws-sdk-go/com_github_ks3sdklib_aws_sdk_go-v1.2.9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ks3sdklib/aws-sdk-go/com_github_ks3sdklib_aws_sdk_go-v1.2.9.zip",
        ],
    )
    go_repository(
        name = "com_github_kulti_thelper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kulti/thelper",
        sha256 = "df0f6ef115c192c9949fd671f05a9660b6c3f6b1bb8de3fb4a5fc74632c92676",
        strip_prefix = "github.com/kulti/thelper@v0.6.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kulti/thelper/com_github_kulti_thelper-v0.6.3.zip",
            "http://ats.apps.svc/gomod/github.com/kulti/thelper/com_github_kulti_thelper-v0.6.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kulti/thelper/com_github_kulti_thelper-v0.6.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kulti/thelper/com_github_kulti_thelper-v0.6.3.zip",
        ],
    )
    go_repository(
        name = "com_github_kunwardeep_paralleltest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kunwardeep/paralleltest",
        sha256 = "6055b96c280348681b8096df4936d14a389bc6f4a8f60601c396b17c75c120b9",
        strip_prefix = "github.com/kunwardeep/paralleltest@v1.0.9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kunwardeep/paralleltest/com_github_kunwardeep_paralleltest-v1.0.9.zip",
            "http://ats.apps.svc/gomod/github.com/kunwardeep/paralleltest/com_github_kunwardeep_paralleltest-v1.0.9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kunwardeep/paralleltest/com_github_kunwardeep_paralleltest-v1.0.9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kunwardeep/paralleltest/com_github_kunwardeep_paralleltest-v1.0.9.zip",
        ],
    )
    go_repository(
        name = "com_github_kylelemons_godebug",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kylelemons/godebug",
        sha256 = "dbbd0ce8c2f4932bb03704d73026b21af12bd68d5b8f4798dbf10a487a2b6d13",
        strip_prefix = "github.com/kylelemons/godebug@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kylelemons/godebug/com_github_kylelemons_godebug-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/kylelemons/godebug/com_github_kylelemons_godebug-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kylelemons/godebug/com_github_kylelemons_godebug-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kylelemons/godebug/com_github_kylelemons_godebug-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_kyoh86_exportloopref",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kyoh86/exportloopref",
        sha256 = "b020464bc6aa6a0bfcb8ee69ff2a836596208d41b6a638c014e5937a3611dad0",
        strip_prefix = "github.com/kyoh86/exportloopref@v0.1.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kyoh86/exportloopref/com_github_kyoh86_exportloopref-v0.1.11.zip",
            "http://ats.apps.svc/gomod/github.com/kyoh86/exportloopref/com_github_kyoh86_exportloopref-v0.1.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kyoh86/exportloopref/com_github_kyoh86_exportloopref-v0.1.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kyoh86/exportloopref/com_github_kyoh86_exportloopref-v0.1.11.zip",
        ],
    )
    go_repository(
        name = "com_github_labstack_echo_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/labstack/echo/v4",
        sha256 = "5c540fbbe5ddd5e99153d17aee615f952865a8d8304074235c7b84a6ec8a2981",
        strip_prefix = "github.com/labstack/echo/v4@v4.1.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/labstack/echo/v4/com_github_labstack_echo_v4-v4.1.11.zip",
            "http://ats.apps.svc/gomod/github.com/labstack/echo/v4/com_github_labstack_echo_v4-v4.1.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/labstack/echo/v4/com_github_labstack_echo_v4-v4.1.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/labstack/echo/v4/com_github_labstack_echo_v4-v4.1.11.zip",
        ],
    )
    go_repository(
        name = "com_github_labstack_gommon",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/labstack/gommon",
        sha256 = "2783ed1c24d09a5539bc35954f71f41d270d78dc656be256c98a8ede2cbbe451",
        strip_prefix = "github.com/labstack/gommon@v0.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/labstack/gommon/com_github_labstack_gommon-v0.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/labstack/gommon/com_github_labstack_gommon-v0.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/labstack/gommon/com_github_labstack_gommon-v0.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/labstack/gommon/com_github_labstack_gommon-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ldez_gomoddirectives",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ldez/gomoddirectives",
        sha256 = "69ce0919a09cc6f1adb05f3fb9c22dada43d685bebabf3d03ae19f6fd752b8e1",
        strip_prefix = "github.com/ldez/gomoddirectives@v0.2.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ldez/gomoddirectives/com_github_ldez_gomoddirectives-v0.2.3.zip",
            "http://ats.apps.svc/gomod/github.com/ldez/gomoddirectives/com_github_ldez_gomoddirectives-v0.2.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ldez/gomoddirectives/com_github_ldez_gomoddirectives-v0.2.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ldez/gomoddirectives/com_github_ldez_gomoddirectives-v0.2.3.zip",
        ],
    )
    go_repository(
        name = "com_github_ldez_tagliatelle",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ldez/tagliatelle",
        sha256 = "9feaf58a15a1b93d2d93ed07fb3ec480fc01b5d75676eb0af5a0f5c6f36128c3",
        strip_prefix = "github.com/ldez/tagliatelle@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ldez/tagliatelle/com_github_ldez_tagliatelle-v0.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/ldez/tagliatelle/com_github_ldez_tagliatelle-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ldez/tagliatelle/com_github_ldez_tagliatelle-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ldez/tagliatelle/com_github_ldez_tagliatelle-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_leonklingele_grouper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/leonklingele/grouper",
        sha256 = "bfc82b49e2d6a73b3df108ae164fc2b72030a16f94de4204d38d96d2bc06fb60",
        strip_prefix = "github.com/leonklingele/grouper@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/leonklingele/grouper/com_github_leonklingele_grouper-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/leonklingele/grouper/com_github_leonklingele_grouper-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/leonklingele/grouper/com_github_leonklingele_grouper-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/leonklingele/grouper/com_github_leonklingele_grouper-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_lestrrat_go_blackmagic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/blackmagic",
        sha256 = "2baa5f21e1db4781a11d0ba2fbe8e71323c78875034da61687d80f47ae9c78ce",
        strip_prefix = "github.com/lestrrat-go/blackmagic@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lestrrat-go/blackmagic/com_github_lestrrat_go_blackmagic-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/lestrrat-go/blackmagic/com_github_lestrrat_go_blackmagic-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lestrrat-go/blackmagic/com_github_lestrrat_go_blackmagic-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lestrrat-go/blackmagic/com_github_lestrrat_go_blackmagic-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_lestrrat_go_httpcc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/httpcc",
        sha256 = "d75132f805ea5cf6275d9af02a5ff3c116ad92ac7fc28e2a22b8fd2e029a3f4c",
        strip_prefix = "github.com/lestrrat-go/httpcc@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lestrrat-go/httpcc/com_github_lestrrat_go_httpcc-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/lestrrat-go/httpcc/com_github_lestrrat_go_httpcc-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lestrrat-go/httpcc/com_github_lestrrat_go_httpcc-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lestrrat-go/httpcc/com_github_lestrrat_go_httpcc-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_lestrrat_go_httprc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/httprc",
        sha256 = "b5ec122596da8970869d3b41a1bc901a440c66a906bbd2fcbe2a19e8728787d7",
        strip_prefix = "github.com/lestrrat-go/httprc@v1.0.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lestrrat-go/httprc/com_github_lestrrat_go_httprc-v1.0.5.zip",
            "http://ats.apps.svc/gomod/github.com/lestrrat-go/httprc/com_github_lestrrat_go_httprc-v1.0.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lestrrat-go/httprc/com_github_lestrrat_go_httprc-v1.0.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lestrrat-go/httprc/com_github_lestrrat_go_httprc-v1.0.5.zip",
        ],
    )
    go_repository(
        name = "com_github_lestrrat_go_iter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/iter",
        sha256 = "991bf0aee428fc1a2c01d548e2c7996dc26871dd0b359c062dfc07b1fb137572",
        strip_prefix = "github.com/lestrrat-go/iter@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lestrrat-go/iter/com_github_lestrrat_go_iter-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/lestrrat-go/iter/com_github_lestrrat_go_iter-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lestrrat-go/iter/com_github_lestrrat_go_iter-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lestrrat-go/iter/com_github_lestrrat_go_iter-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_lestrrat_go_jwx_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/jwx/v2",
        sha256 = "f49d9cb1482cbd4ed113d8fa1c3f197df5ba498dd461641123cff0337e030af2",
        strip_prefix = "github.com/lestrrat-go/jwx/v2@v2.0.21",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lestrrat-go/jwx/v2/com_github_lestrrat_go_jwx_v2-v2.0.21.zip",
            "http://ats.apps.svc/gomod/github.com/lestrrat-go/jwx/v2/com_github_lestrrat_go_jwx_v2-v2.0.21.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lestrrat-go/jwx/v2/com_github_lestrrat_go_jwx_v2-v2.0.21.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lestrrat-go/jwx/v2/com_github_lestrrat_go_jwx_v2-v2.0.21.zip",
        ],
    )
    go_repository(
        name = "com_github_lestrrat_go_option",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/option",
        sha256 = "3e5614e160680053e07e4970e825e694c2a917741e735ab4d435a396b739ae78",
        strip_prefix = "github.com/lestrrat-go/option@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lestrrat-go/option/com_github_lestrrat_go_option-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/lestrrat-go/option/com_github_lestrrat_go_option-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lestrrat-go/option/com_github_lestrrat_go_option-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lestrrat-go/option/com_github_lestrrat_go_option-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_linode_linodego",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/linode/linodego",
        sha256 = "06b5687b4acf4511965b37bf08aec25fb606dc289803e463dfa450ee19518a93",
        strip_prefix = "github.com/linode/linodego@v1.25.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/linode/linodego/com_github_linode_linodego-v1.25.0.zip",
            "http://ats.apps.svc/gomod/github.com/linode/linodego/com_github_linode_linodego-v1.25.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/linode/linodego/com_github_linode_linodego-v1.25.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/linode/linodego/com_github_linode_linodego-v1.25.0.zip",
        ],
    )
    go_repository(
        name = "com_github_lufeee_execinquery",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lufeee/execinquery",
        sha256 = "040a3d96d8ca1bb8240a9c8beaf914e71a1c73c2a44358e290b4969de560225f",
        strip_prefix = "github.com/lufeee/execinquery@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lufeee/execinquery/com_github_lufeee_execinquery-v1.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/lufeee/execinquery/com_github_lufeee_execinquery-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lufeee/execinquery/com_github_lufeee_execinquery-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lufeee/execinquery/com_github_lufeee_execinquery-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_lufia_plan9stats",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lufia/plan9stats",
        sha256 = "7163852e02f12aff5db9b5250690f3a177cdcdb514f2afc8cfb38a6396a950c1",
        strip_prefix = "github.com/lufia/plan9stats@v0.0.0-20230326075908-cb1d2100619a",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lufia/plan9stats/com_github_lufia_plan9stats-v0.0.0-20230326075908-cb1d2100619a.zip",
            "http://ats.apps.svc/gomod/github.com/lufia/plan9stats/com_github_lufia_plan9stats-v0.0.0-20230326075908-cb1d2100619a.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lufia/plan9stats/com_github_lufia_plan9stats-v0.0.0-20230326075908-cb1d2100619a.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lufia/plan9stats/com_github_lufia_plan9stats-v0.0.0-20230326075908-cb1d2100619a.zip",
        ],
    )
    go_repository(
        name = "com_github_macabu_inamedparam",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/macabu/inamedparam",
        sha256 = "7d14759754c4269cbc25cd7ef39f72369566e81e2b2dd6c5ce2e10aeff7084d9",
        strip_prefix = "github.com/macabu/inamedparam@v0.1.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/macabu/inamedparam/com_github_macabu_inamedparam-v0.1.3.zip",
            "http://ats.apps.svc/gomod/github.com/macabu/inamedparam/com_github_macabu_inamedparam-v0.1.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/macabu/inamedparam/com_github_macabu_inamedparam-v0.1.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/macabu/inamedparam/com_github_macabu_inamedparam-v0.1.3.zip",
        ],
    )
    go_repository(
        name = "com_github_magiconair_properties",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/magiconair/properties",
        sha256 = "649dd0dac8fa6d7f2d5e6d1e7fe4a57ecb6c05346c8f6f15968dd66ebaf7212a",
        strip_prefix = "github.com/magiconair/properties@v1.8.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/magiconair/properties/com_github_magiconair_properties-v1.8.6.zip",
            "http://ats.apps.svc/gomod/github.com/magiconair/properties/com_github_magiconair_properties-v1.8.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/magiconair/properties/com_github_magiconair_properties-v1.8.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/magiconair/properties/com_github_magiconair_properties-v1.8.6.zip",
        ],
    )
    go_repository(
        name = "com_github_mailru_easyjson",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mailru/easyjson",
        sha256 = "139387981a220d499c9f47cece42a2002f105e4ee3ab9c74188a7fb8a9be711e",
        strip_prefix = "github.com/mailru/easyjson@v0.7.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mailru/easyjson/com_github_mailru_easyjson-v0.7.7.zip",
            "http://ats.apps.svc/gomod/github.com/mailru/easyjson/com_github_mailru_easyjson-v0.7.7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mailru/easyjson/com_github_mailru_easyjson-v0.7.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mailru/easyjson/com_github_mailru_easyjson-v0.7.7.zip",
        ],
    )
    go_repository(
        name = "com_github_maratori_testableexamples",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/maratori/testableexamples",
        sha256 = "c4605f4f40f71448ab16bdd914a8c35903f3e6a65f7578b66e07456111f9f433",
        strip_prefix = "github.com/maratori/testableexamples@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/maratori/testableexamples/com_github_maratori_testableexamples-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/maratori/testableexamples/com_github_maratori_testableexamples-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/maratori/testableexamples/com_github_maratori_testableexamples-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/maratori/testableexamples/com_github_maratori_testableexamples-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_maratori_testpackage",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/maratori/testpackage",
        sha256 = "72931ea874f81055da8999ab8f383967a18c705d3b93259a35fe4a9dc4feb21c",
        strip_prefix = "github.com/maratori/testpackage@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/maratori/testpackage/com_github_maratori_testpackage-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/maratori/testpackage/com_github_maratori_testpackage-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/maratori/testpackage/com_github_maratori_testpackage-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/maratori/testpackage/com_github_maratori_testpackage-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_goutils",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/goutils",
        sha256 = "ef8778a20c37e98a92e3b1db5ab027cc201743a2f5bfb26ba228bf0515e20b48",
        strip_prefix = "github.com/Masterminds/goutils@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Masterminds/goutils/com_github_masterminds_goutils-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/Masterminds/goutils/com_github_masterminds_goutils-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Masterminds/goutils/com_github_masterminds_goutils-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Masterminds/goutils/com_github_masterminds_goutils-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_semver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/semver",
        sha256 = "15f6b54a695c15ffb205d5719e5ed50fab9ba9a739e1b4bdf3a0a319f51a7202",
        strip_prefix = "github.com/Masterminds/semver@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Masterminds/semver/com_github_masterminds_semver-v1.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/Masterminds/semver/com_github_masterminds_semver-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Masterminds/semver/com_github_masterminds_semver-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Masterminds/semver/com_github_masterminds_semver-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_semver_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/semver/v3",
        sha256 = "0a46c7403dfeda09b0821e851f8e1cec8f1ea4276281e42ea399da5bc5bf0704",
        strip_prefix = "github.com/Masterminds/semver/v3@v3.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Masterminds/semver/v3/com_github_masterminds_semver_v3-v3.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/Masterminds/semver/v3/com_github_masterminds_semver_v3-v3.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Masterminds/semver/v3/com_github_masterminds_semver_v3-v3.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Masterminds/semver/v3/com_github_masterminds_semver_v3-v3.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_sprig_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/sprig/v3",
        sha256 = "be8dcfe2b278d11b946caee75661e0ce3c2592733963029fb9950e67dcd92579",
        strip_prefix = "github.com/Masterminds/sprig/v3@v3.2.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Masterminds/sprig/v3/com_github_masterminds_sprig_v3-v3.2.2.zip",
            "http://ats.apps.svc/gomod/github.com/Masterminds/sprig/v3/com_github_masterminds_sprig_v3-v3.2.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Masterminds/sprig/v3/com_github_masterminds_sprig_v3-v3.2.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Masterminds/sprig/v3/com_github_masterminds_sprig_v3-v3.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_matoous_godox",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/matoous/godox",
        sha256 = "10c2ba3fdd10df1c263c709208868cab6c8b0d07a91689708a21efe9c98e4f62",
        strip_prefix = "github.com/matoous/godox@v0.0.0-20230222163458-006bad1f9d26",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/matoous/godox/com_github_matoous_godox-v0.0.0-20230222163458-006bad1f9d26.zip",
            "http://ats.apps.svc/gomod/github.com/matoous/godox/com_github_matoous_godox-v0.0.0-20230222163458-006bad1f9d26.zip",
            "https://cache.hawkingrei.com/gomod/github.com/matoous/godox/com_github_matoous_godox-v0.0.0-20230222163458-006bad1f9d26.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/matoous/godox/com_github_matoous_godox-v0.0.0-20230222163458-006bad1f9d26.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_colorable",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-colorable",
        sha256 = "08be322dcc584a9fcfde5caf0cf878b4e11cd98f252e32bc704e92c5a4ba9d15",
        strip_prefix = "github.com/mattn/go-colorable@v0.1.13",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mattn/go-colorable/com_github_mattn_go_colorable-v0.1.13.zip",
            "http://ats.apps.svc/gomod/github.com/mattn/go-colorable/com_github_mattn_go_colorable-v0.1.13.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mattn/go-colorable/com_github_mattn_go_colorable-v0.1.13.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mattn/go-colorable/com_github_mattn_go_colorable-v0.1.13.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_isatty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-isatty",
        sha256 = "f2d5f89ca451577e17464b9bb596dc0d0ecececb5eaa63622c41b57cd0b7b8cc",
        strip_prefix = "github.com/mattn/go-isatty@v0.0.20",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mattn/go-isatty/com_github_mattn_go_isatty-v0.0.20.zip",
            "http://ats.apps.svc/gomod/github.com/mattn/go-isatty/com_github_mattn_go_isatty-v0.0.20.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mattn/go-isatty/com_github_mattn_go_isatty-v0.0.20.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mattn/go-isatty/com_github_mattn_go_isatty-v0.0.20.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_runewidth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-runewidth",
        sha256 = "d97c4f0667a14957569c932a8e2488f1c43757b4dcce313897aa001f07d149b0",
        strip_prefix = "github.com/mattn/go-runewidth@v0.0.15",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mattn/go-runewidth/com_github_mattn_go_runewidth-v0.0.15.zip",
            "http://ats.apps.svc/gomod/github.com/mattn/go-runewidth/com_github_mattn_go_runewidth-v0.0.15.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mattn/go-runewidth/com_github_mattn_go_runewidth-v0.0.15.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mattn/go-runewidth/com_github_mattn_go_runewidth-v0.0.15.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_goveralls",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/goveralls",
        sha256 = "3df5b7ebfb61edd9a098895aae7009a927a2fe91f73f38f48467a7b9e6c006f7",
        strip_prefix = "github.com/mattn/goveralls@v0.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mattn/goveralls/com_github_mattn_goveralls-v0.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/mattn/goveralls/com_github_mattn_goveralls-v0.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mattn/goveralls/com_github_mattn_goveralls-v0.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mattn/goveralls/com_github_mattn_goveralls-v0.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_matttproud_golang_protobuf_extensions",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/matttproud/golang_protobuf_extensions",
        sha256 = "e64dc58023f4b8c4472d05a44f2719b84d6c2cc364cc682820c9f72b233c9cdc",
        strip_prefix = "github.com/matttproud/golang_protobuf_extensions@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/matttproud/golang_protobuf_extensions/com_github_matttproud_golang_protobuf_extensions-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/matttproud/golang_protobuf_extensions/com_github_matttproud_golang_protobuf_extensions-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/matttproud/golang_protobuf_extensions/com_github_matttproud_golang_protobuf_extensions-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/matttproud/golang_protobuf_extensions/com_github_matttproud_golang_protobuf_extensions-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_matttproud_golang_protobuf_extensions_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/matttproud/golang_protobuf_extensions/v2",
        sha256 = "999b014a892da09d7cdd84e4f7117ff034075d74658b162b35eb61bebf29a14f",
        strip_prefix = "github.com/matttproud/golang_protobuf_extensions/v2@v2.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/matttproud/golang_protobuf_extensions/v2/com_github_matttproud_golang_protobuf_extensions_v2-v2.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/matttproud/golang_protobuf_extensions/v2/com_github_matttproud_golang_protobuf_extensions_v2-v2.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/matttproud/golang_protobuf_extensions/v2/com_github_matttproud_golang_protobuf_extensions_v2-v2.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/matttproud/golang_protobuf_extensions/v2/com_github_matttproud_golang_protobuf_extensions_v2-v2.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mbilski_exhaustivestruct",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mbilski/exhaustivestruct",
        sha256 = "9c1396a64b322467fc591289fe966c87ef4c976e3f70aab678cf25387a3c9b0c",
        strip_prefix = "github.com/mbilski/exhaustivestruct@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mbilski/exhaustivestruct/com_github_mbilski_exhaustivestruct-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/mbilski/exhaustivestruct/com_github_mbilski_exhaustivestruct-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mbilski/exhaustivestruct/com_github_mbilski_exhaustivestruct-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mbilski/exhaustivestruct/com_github_mbilski_exhaustivestruct-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mediocregopher_mediocre_go_lib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mediocregopher/mediocre-go-lib",
        sha256 = "6b9950c36810c23dfe38c9de790da689af70811f520f161fc9325b202c71fab3",
        strip_prefix = "github.com/mediocregopher/mediocre-go-lib@v0.0.0-20181029021733-cb65787f37ed",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mediocregopher/mediocre-go-lib/com_github_mediocregopher_mediocre_go_lib-v0.0.0-20181029021733-cb65787f37ed.zip",
            "http://ats.apps.svc/gomod/github.com/mediocregopher/mediocre-go-lib/com_github_mediocregopher_mediocre_go_lib-v0.0.0-20181029021733-cb65787f37ed.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mediocregopher/mediocre-go-lib/com_github_mediocregopher_mediocre_go_lib-v0.0.0-20181029021733-cb65787f37ed.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mediocregopher/mediocre-go-lib/com_github_mediocregopher_mediocre_go_lib-v0.0.0-20181029021733-cb65787f37ed.zip",
        ],
    )
    go_repository(
        name = "com_github_mediocregopher_radix_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mediocregopher/radix/v3",
        sha256 = "c9d5413d739e2254b611da4fe4abc2de0aea552ab3a95032ffe107c341144b04",
        strip_prefix = "github.com/mediocregopher/radix/v3@v3.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mediocregopher/radix/v3/com_github_mediocregopher_radix_v3-v3.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/mediocregopher/radix/v3/com_github_mediocregopher_radix_v3-v3.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mediocregopher/radix/v3/com_github_mediocregopher_radix_v3-v3.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mediocregopher/radix/v3/com_github_mediocregopher_radix_v3-v3.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mgechev_dots",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mgechev/dots",
        sha256 = "4c7dd3e110685eb3e5955032bf2beaa0b062bcebaaa06a1d4a097c3aef83af17",
        strip_prefix = "github.com/mgechev/dots@v0.0.0-20210922191527-e955255bf517",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mgechev/dots/com_github_mgechev_dots-v0.0.0-20210922191527-e955255bf517.zip",
            "http://ats.apps.svc/gomod/github.com/mgechev/dots/com_github_mgechev_dots-v0.0.0-20210922191527-e955255bf517.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mgechev/dots/com_github_mgechev_dots-v0.0.0-20210922191527-e955255bf517.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mgechev/dots/com_github_mgechev_dots-v0.0.0-20210922191527-e955255bf517.zip",
        ],
    )
    go_repository(
        name = "com_github_mgechev_revive",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mgechev/revive",
        sha256 = "bebbe64597e76c9d7219de964b05d3f0659e4a31344d9dff2b5ec3fad50f7e3a",
        strip_prefix = "github.com/mgechev/revive@v1.3.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mgechev/revive/com_github_mgechev_revive-v1.3.7.zip",
            "http://ats.apps.svc/gomod/github.com/mgechev/revive/com_github_mgechev_revive-v1.3.7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mgechev/revive/com_github_mgechev_revive-v1.3.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mgechev/revive/com_github_mgechev_revive-v1.3.7.zip",
        ],
    )
    go_repository(
        name = "com_github_microcosm_cc_bluemonday",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/microcosm-cc/bluemonday",
        sha256 = "9cfac37098da75ab1c278740e8f0f7741891d8843e14afb256574596ad786f83",
        strip_prefix = "github.com/microcosm-cc/bluemonday@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/microcosm-cc/bluemonday/com_github_microcosm_cc_bluemonday-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/microcosm-cc/bluemonday/com_github_microcosm_cc_bluemonday-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/microcosm-cc/bluemonday/com_github_microcosm_cc_bluemonday-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/microcosm-cc/bluemonday/com_github_microcosm_cc_bluemonday-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_microsoft_go_winio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Microsoft/go-winio",
        sha256 = "fdfec88b9eb61895ab39ed3a6181d99d78366638f86a609170d76417ba018f53",
        strip_prefix = "github.com/Microsoft/go-winio@v0.6.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Microsoft/go-winio/com_github_microsoft_go_winio-v0.6.1.zip",
            "http://ats.apps.svc/gomod/github.com/Microsoft/go-winio/com_github_microsoft_go_winio-v0.6.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Microsoft/go-winio/com_github_microsoft_go_winio-v0.6.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Microsoft/go-winio/com_github_microsoft_go_winio-v0.6.1.zip",
        ],
    )
    go_repository(
        name = "com_github_miekg_dns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/miekg/dns",
        sha256 = "50750ca3ebe181f8c16a3d5ccdbc4f7fe864ba6c731a8013d4df8678f901e1f6",
        strip_prefix = "github.com/miekg/dns@v1.1.57",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/miekg/dns/com_github_miekg_dns-v1.1.57.zip",
            "http://ats.apps.svc/gomod/github.com/miekg/dns/com_github_miekg_dns-v1.1.57.zip",
            "https://cache.hawkingrei.com/gomod/github.com/miekg/dns/com_github_miekg_dns-v1.1.57.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/miekg/dns/com_github_miekg_dns-v1.1.57.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_copystructure",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/copystructure",
        sha256 = "4a2c9eb367a7781864e8edbd3b11781897766bcf6120f77a717d54a575392eee",
        strip_prefix = "github.com/mitchellh/copystructure@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/copystructure/com_github_mitchellh_copystructure-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/copystructure/com_github_mitchellh_copystructure-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/copystructure/com_github_mitchellh_copystructure-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/copystructure/com_github_mitchellh_copystructure-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_go_homedir",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-homedir",
        sha256 = "fffec361fc7e776bb71433560c285ee2982d2c140b8f5bfba0db6033c0ade184",
        strip_prefix = "github.com/mitchellh/go-homedir@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/go-homedir/com_github_mitchellh_go_homedir-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/go-homedir/com_github_mitchellh_go_homedir-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/go-homedir/com_github_mitchellh_go_homedir-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/go-homedir/com_github_mitchellh_go_homedir-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_go_ps",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-ps",
        sha256 = "f2f0400b1d5e136419daed275c27a930b0f5447ac12bb8acd3ddbe39547b2834",
        strip_prefix = "github.com/mitchellh/go-ps@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/go-ps/com_github_mitchellh_go_ps-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/go-ps/com_github_mitchellh_go_ps-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/go-ps/com_github_mitchellh_go_ps-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/go-ps/com_github_mitchellh_go_ps-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_mapstructure",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/mapstructure",
        sha256 = "118d5b2cb65c50dba967fb6d708f450a9caf93f321f8fc99080675b2ee374199",
        strip_prefix = "github.com/mitchellh/mapstructure@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/mapstructure/com_github_mitchellh_mapstructure-v1.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/mapstructure/com_github_mitchellh_mapstructure-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/mapstructure/com_github_mitchellh_mapstructure-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/mapstructure/com_github_mitchellh_mapstructure-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_reflectwalk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/reflectwalk",
        sha256 = "bf1d4540bf05ea244e65fca3e9f859d8129c381adaeebe7f22703959aadc4210",
        strip_prefix = "github.com/mitchellh/reflectwalk@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/reflectwalk/com_github_mitchellh_reflectwalk-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/reflectwalk/com_github_mitchellh_reflectwalk-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/reflectwalk/com_github_mitchellh_reflectwalk-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/reflectwalk/com_github_mitchellh_reflectwalk-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_moby_spdystream",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moby/spdystream",
        sha256 = "9db6d001a80f4c3cb332bb8a1bb9260908e1ffa9a20491e9bc05358263eed278",
        strip_prefix = "github.com/moby/spdystream@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/moby/spdystream/com_github_moby_spdystream-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/moby/spdystream/com_github_moby_spdystream-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/moby/spdystream/com_github_moby_spdystream-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/moby/spdystream/com_github_moby_spdystream-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_moby_term",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moby/term",
        sha256 = "0d2e2ce8280f803a14d9c2af23a79cf854e06d47f2e6b7d455291ffd47c11e2f",
        strip_prefix = "github.com/moby/term@v0.0.0-20210619224110-3f7ff695adc6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/moby/term/com_github_moby_term-v0.0.0-20210619224110-3f7ff695adc6.zip",
            "http://ats.apps.svc/gomod/github.com/moby/term/com_github_moby_term-v0.0.0-20210619224110-3f7ff695adc6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/moby/term/com_github_moby_term-v0.0.0-20210619224110-3f7ff695adc6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/moby/term/com_github_moby_term-v0.0.0-20210619224110-3f7ff695adc6.zip",
        ],
    )
    go_repository(
        name = "com_github_modern_go_concurrent",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/modern-go/concurrent",
        sha256 = "91ef49599bec459869d94ff3dec128871ab66bd2dfa61041f1e1169f9b4a8073",
        strip_prefix = "github.com/modern-go/concurrent@v0.0.0-20180306012644-bacd9c7ef1dd",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/modern-go/concurrent/com_github_modern_go_concurrent-v0.0.0-20180306012644-bacd9c7ef1dd.zip",
            "http://ats.apps.svc/gomod/github.com/modern-go/concurrent/com_github_modern_go_concurrent-v0.0.0-20180306012644-bacd9c7ef1dd.zip",
            "https://cache.hawkingrei.com/gomod/github.com/modern-go/concurrent/com_github_modern_go_concurrent-v0.0.0-20180306012644-bacd9c7ef1dd.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/modern-go/concurrent/com_github_modern_go_concurrent-v0.0.0-20180306012644-bacd9c7ef1dd.zip",
        ],
    )
    go_repository(
        name = "com_github_modern_go_reflect2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/modern-go/reflect2",
        sha256 = "f46f41409c2e74293f82cfe6c70b5d582bff8ada0106a7d3ff5706520c50c21c",
        strip_prefix = "github.com/modern-go/reflect2@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/modern-go/reflect2/com_github_modern_go_reflect2-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/modern-go/reflect2/com_github_modern_go_reflect2-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/modern-go/reflect2/com_github_modern_go_reflect2-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/modern-go/reflect2/com_github_modern_go_reflect2-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_montanaflynn_stats",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/montanaflynn/stats",
        sha256 = "661546beb7c49f92a2c798709323f5cb175251bc359c061e5933071679f9b2ef",
        strip_prefix = "github.com/montanaflynn/stats@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/montanaflynn/stats/com_github_montanaflynn_stats-v0.7.0.zip",
            "http://ats.apps.svc/gomod/github.com/montanaflynn/stats/com_github_montanaflynn_stats-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/montanaflynn/stats/com_github_montanaflynn_stats-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/montanaflynn/stats/com_github_montanaflynn_stats-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_moricho_tparallel",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moricho/tparallel",
        sha256 = "338b0f9ea839d5b8663a45b8b094bcfc22b5347cd3771cbe872de326e4d8ea9e",
        strip_prefix = "github.com/moricho/tparallel@v0.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/moricho/tparallel/com_github_moricho_tparallel-v0.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/moricho/tparallel/com_github_moricho_tparallel-v0.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/moricho/tparallel/com_github_moricho_tparallel-v0.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/moricho/tparallel/com_github_moricho_tparallel-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_morikuni_aec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/morikuni/aec",
        sha256 = "c14eeff6945b854edd8b91a83ac760fbd95068f33dc17d102c18f2e8e86bcced",
        strip_prefix = "github.com/morikuni/aec@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/morikuni/aec/com_github_morikuni_aec-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/morikuni/aec/com_github_morikuni_aec-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/morikuni/aec/com_github_morikuni_aec-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/morikuni/aec/com_github_morikuni_aec-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_moul_http2curl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moul/http2curl",
        sha256 = "3600be3621038727f856bf7403d3ef0ffcc2a6729716bab67b592dcd19b3fee2",
        strip_prefix = "github.com/moul/http2curl@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/moul/http2curl/com_github_moul_http2curl-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/moul/http2curl/com_github_moul_http2curl-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/moul/http2curl/com_github_moul_http2curl-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/moul/http2curl/com_github_moul_http2curl-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_munnerz_goautoneg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/munnerz/goautoneg",
        sha256 = "3d7ce17916779890be02ea6b3dd6345c3c30c1df502ad9d8b5b9b310e636afd9",
        strip_prefix = "github.com/munnerz/goautoneg@v0.0.0-20191010083416-a7dc8b61c822",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/munnerz/goautoneg/com_github_munnerz_goautoneg-v0.0.0-20191010083416-a7dc8b61c822.zip",
            "http://ats.apps.svc/gomod/github.com/munnerz/goautoneg/com_github_munnerz_goautoneg-v0.0.0-20191010083416-a7dc8b61c822.zip",
            "https://cache.hawkingrei.com/gomod/github.com/munnerz/goautoneg/com_github_munnerz_goautoneg-v0.0.0-20191010083416-a7dc8b61c822.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/munnerz/goautoneg/com_github_munnerz_goautoneg-v0.0.0-20191010083416-a7dc8b61c822.zip",
        ],
    )
    go_repository(
        name = "com_github_mwitkow_go_conntrack",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mwitkow/go-conntrack",
        sha256 = "d6fc513490d5c73e3f64ede3cf18ba973a4f8ef4c39c9816cc6080e39c8c480a",
        strip_prefix = "github.com/mwitkow/go-conntrack@v0.0.0-20190716064945-2f068394615f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mwitkow/go-conntrack/com_github_mwitkow_go_conntrack-v0.0.0-20190716064945-2f068394615f.zip",
            "http://ats.apps.svc/gomod/github.com/mwitkow/go-conntrack/com_github_mwitkow_go_conntrack-v0.0.0-20190716064945-2f068394615f.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mwitkow/go-conntrack/com_github_mwitkow_go_conntrack-v0.0.0-20190716064945-2f068394615f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mwitkow/go-conntrack/com_github_mwitkow_go_conntrack-v0.0.0-20190716064945-2f068394615f.zip",
        ],
    )
    go_repository(
        name = "com_github_mxk_go_flowrate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mxk/go-flowrate",
        sha256 = "bd0701ef9115469a661c07a3e9c2e572114126eb2d098b01eda34ebf62548492",
        strip_prefix = "github.com/mxk/go-flowrate@v0.0.0-20140419014527-cca7078d478f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mxk/go-flowrate/com_github_mxk_go_flowrate-v0.0.0-20140419014527-cca7078d478f.zip",
            "http://ats.apps.svc/gomod/github.com/mxk/go-flowrate/com_github_mxk_go_flowrate-v0.0.0-20140419014527-cca7078d478f.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mxk/go-flowrate/com_github_mxk_go_flowrate-v0.0.0-20140419014527-cca7078d478f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mxk/go-flowrate/com_github_mxk_go_flowrate-v0.0.0-20140419014527-cca7078d478f.zip",
        ],
    )
    go_repository(
        name = "com_github_nakabonne_nestif",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nakabonne/nestif",
        sha256 = "7c0a39bd8577b7b158e9213f70f8d92a704d19d74900eee4f5da0e9f233fa7c7",
        strip_prefix = "github.com/nakabonne/nestif@v0.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nakabonne/nestif/com_github_nakabonne_nestif-v0.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/nakabonne/nestif/com_github_nakabonne_nestif-v0.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nakabonne/nestif/com_github_nakabonne_nestif-v0.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nakabonne/nestif/com_github_nakabonne_nestif-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_nats_io_nats_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nats-io/nats.go",
        sha256 = "42a3892acc5cd1d41e449825e71ecd97d5bc973e718d9eca2d9ccdf1d0560266",
        strip_prefix = "github.com/nats-io/nats.go@v1.8.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nats-io/nats.go/com_github_nats_io_nats_go-v1.8.1.zip",
            "http://ats.apps.svc/gomod/github.com/nats-io/nats.go/com_github_nats_io_nats_go-v1.8.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nats-io/nats.go/com_github_nats_io_nats_go-v1.8.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nats-io/nats.go/com_github_nats_io_nats_go-v1.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_nats_io_nkeys",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nats-io/nkeys",
        sha256 = "5ac686325cdc67ca417c61f55ab8736643fcfafcba9b29aa6e632b96d725b2df",
        strip_prefix = "github.com/nats-io/nkeys@v0.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nats-io/nkeys/com_github_nats_io_nkeys-v0.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/nats-io/nkeys/com_github_nats_io_nkeys-v0.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nats-io/nkeys/com_github_nats_io_nkeys-v0.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nats-io/nkeys/com_github_nats_io_nkeys-v0.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_nats_io_nuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nats-io/nuid",
        sha256 = "809d144fbd16f91651a433e28d2008d339e19dafc450c5995e2ed92f1c17c1f3",
        strip_prefix = "github.com/nats-io/nuid@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nats-io/nuid/com_github_nats_io_nuid-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/nats-io/nuid/com_github_nats_io_nuid-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nats-io/nuid/com_github_nats_io_nuid-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nats-io/nuid/com_github_nats_io_nuid-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_nbutton23_zxcvbn_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nbutton23/zxcvbn-go",
        sha256 = "ceffa831914e8b648effbc6c937c00c1c0287f99b1f0bc039218100c20242f2d",
        strip_prefix = "github.com/nbutton23/zxcvbn-go@v0.0.0-20210217022336-fa2cb2858354",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nbutton23/zxcvbn-go/com_github_nbutton23_zxcvbn_go-v0.0.0-20210217022336-fa2cb2858354.zip",
            "http://ats.apps.svc/gomod/github.com/nbutton23/zxcvbn-go/com_github_nbutton23_zxcvbn_go-v0.0.0-20210217022336-fa2cb2858354.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nbutton23/zxcvbn-go/com_github_nbutton23_zxcvbn_go-v0.0.0-20210217022336-fa2cb2858354.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nbutton23/zxcvbn-go/com_github_nbutton23_zxcvbn_go-v0.0.0-20210217022336-fa2cb2858354.zip",
        ],
    )
    go_repository(
        name = "com_github_ncw_directio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ncw/directio",
        sha256 = "15266d0977e1466c6a3d9d436b069df02b8593d7901dbe18a60dd1ac851420f8",
        strip_prefix = "github.com/ncw/directio@v1.0.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ncw/directio/com_github_ncw_directio-v1.0.5.zip",
            "http://ats.apps.svc/gomod/github.com/ncw/directio/com_github_ncw_directio-v1.0.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ncw/directio/com_github_ncw_directio-v1.0.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ncw/directio/com_github_ncw_directio-v1.0.5.zip",
        ],
    )
    go_repository(
        name = "com_github_ngaut_pools",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ngaut/pools",
        sha256 = "26342833d7a5b91a52f8451e8e34bc9ffc5069d342666ab0b478628c41a86d44",
        strip_prefix = "github.com/ngaut/pools@v0.0.0-20180318154953-b7bc8c42aac7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ngaut/pools/com_github_ngaut_pools-v0.0.0-20180318154953-b7bc8c42aac7.zip",
            "http://ats.apps.svc/gomod/github.com/ngaut/pools/com_github_ngaut_pools-v0.0.0-20180318154953-b7bc8c42aac7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ngaut/pools/com_github_ngaut_pools-v0.0.0-20180318154953-b7bc8c42aac7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ngaut/pools/com_github_ngaut_pools-v0.0.0-20180318154953-b7bc8c42aac7.zip",
        ],
    )
    go_repository(
        name = "com_github_ngaut_sync2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ngaut/sync2",
        sha256 = "2635d6120b6172c190f84b57b5fc878f9158b768b4bd6bd4468bfa98a73061a4",
        strip_prefix = "github.com/ngaut/sync2@v0.0.0-20141008032647-7a24ed77b2ef",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ngaut/sync2/com_github_ngaut_sync2-v0.0.0-20141008032647-7a24ed77b2ef.zip",
            "http://ats.apps.svc/gomod/github.com/ngaut/sync2/com_github_ngaut_sync2-v0.0.0-20141008032647-7a24ed77b2ef.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ngaut/sync2/com_github_ngaut_sync2-v0.0.0-20141008032647-7a24ed77b2ef.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ngaut/sync2/com_github_ngaut_sync2-v0.0.0-20141008032647-7a24ed77b2ef.zip",
        ],
    )
    go_repository(
        name = "com_github_niemeyer_pretty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/niemeyer/pretty",
        sha256 = "2dcb7053faf11c28cad7d84fcfa3dd7f93e3d236b39d83cff0934f691f860d7a",
        strip_prefix = "github.com/niemeyer/pretty@v0.0.0-20200227124842-a10e7caefd8e",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/niemeyer/pretty/com_github_niemeyer_pretty-v0.0.0-20200227124842-a10e7caefd8e.zip",
            "http://ats.apps.svc/gomod/github.com/niemeyer/pretty/com_github_niemeyer_pretty-v0.0.0-20200227124842-a10e7caefd8e.zip",
            "https://cache.hawkingrei.com/gomod/github.com/niemeyer/pretty/com_github_niemeyer_pretty-v0.0.0-20200227124842-a10e7caefd8e.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/niemeyer/pretty/com_github_niemeyer_pretty-v0.0.0-20200227124842-a10e7caefd8e.zip",
        ],
    )
    go_repository(
        name = "com_github_nishanths_exhaustive",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nishanths/exhaustive",
        sha256 = "2b60661abebe145c072c3ca54956d182999f6cca074e4b2f144f559f70c8e4bd",
        strip_prefix = "github.com/nishanths/exhaustive@v0.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nishanths/exhaustive/com_github_nishanths_exhaustive-v0.12.0.zip",
            "http://ats.apps.svc/gomod/github.com/nishanths/exhaustive/com_github_nishanths_exhaustive-v0.12.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nishanths/exhaustive/com_github_nishanths_exhaustive-v0.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nishanths/exhaustive/com_github_nishanths_exhaustive-v0.12.0.zip",
        ],
    )
    go_repository(
        name = "com_github_nishanths_predeclared",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nishanths/predeclared",
        sha256 = "8ab7ff9f539ec50902647a9be76d7408e9f501958efd14973891ac4be87a4486",
        strip_prefix = "github.com/nishanths/predeclared@v0.2.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nishanths/predeclared/com_github_nishanths_predeclared-v0.2.2.zip",
            "http://ats.apps.svc/gomod/github.com/nishanths/predeclared/com_github_nishanths_predeclared-v0.2.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nishanths/predeclared/com_github_nishanths_predeclared-v0.2.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nishanths/predeclared/com_github_nishanths_predeclared-v0.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_nunnatsa_ginkgolinter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nunnatsa/ginkgolinter",
        sha256 = "18297165d1d36b7b32e56498631d06c4ff4a4c9dc469d420e2e8aaf27c4caa24",
        strip_prefix = "github.com/nunnatsa/ginkgolinter@v0.15.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.15.2.zip",
            "http://ats.apps.svc/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.15.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.15.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.15.2.zip",
        ],
    )
    go_repository(
        name = "com_github_nxadm_tail",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nxadm/tail",
        sha256 = "70bf6e142f90694059792f7d5b31a915df989e8a6a554a836de36fa075377ff9",
        strip_prefix = "github.com/nxadm/tail@v1.4.8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nxadm/tail/com_github_nxadm_tail-v1.4.8.zip",
            "http://ats.apps.svc/gomod/github.com/nxadm/tail/com_github_nxadm_tail-v1.4.8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nxadm/tail/com_github_nxadm_tail-v1.4.8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nxadm/tail/com_github_nxadm_tail-v1.4.8.zip",
        ],
    )
    go_repository(
        name = "com_github_oklog_run",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/oklog/run",
        sha256 = "d6f69fc71aa155043f926c2a98fc1e5b3a8ebab422f2f36d785cfba38a7ebee4",
        strip_prefix = "github.com/oklog/run@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/oklog/run/com_github_oklog_run-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/oklog/run/com_github_oklog_run-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/oklog/run/com_github_oklog_run-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/oklog/run/com_github_oklog_run-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_oklog_ulid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/oklog/ulid",
        sha256 = "40e502c064a922d5eb7f2bc2cda9c6a2a929ec0fc76c9aae4db54fb7b6b611ae",
        strip_prefix = "github.com/oklog/ulid@v1.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/oklog/ulid/com_github_oklog_ulid-v1.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/oklog/ulid/com_github_oklog_ulid-v1.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/oklog/ulid/com_github_oklog_ulid-v1.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/oklog/ulid/com_github_oklog_ulid-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_olekukonko_tablewriter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/olekukonko/tablewriter",
        sha256 = "ba678c0fddd0645293afc2ac50a5943730d755e31059f588f4b4a8c581b65dad",
        strip_prefix = "github.com/olekukonko/tablewriter@v0.0.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/olekukonko/tablewriter/com_github_olekukonko_tablewriter-v0.0.5.zip",
            "http://ats.apps.svc/gomod/github.com/olekukonko/tablewriter/com_github_olekukonko_tablewriter-v0.0.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/olekukonko/tablewriter/com_github_olekukonko_tablewriter-v0.0.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/olekukonko/tablewriter/com_github_olekukonko_tablewriter-v0.0.5.zip",
        ],
    )
    go_repository(
        name = "com_github_onsi_ginkgo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/onsi/ginkgo",
        sha256 = "e23fc33b0affa73a4f4c63410af931bf1f8d5b9db266b3461177036d725eacc5",
        strip_prefix = "github.com/onsi/ginkgo@v1.16.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/onsi/ginkgo/com_github_onsi_ginkgo-v1.16.5.zip",
            "http://ats.apps.svc/gomod/github.com/onsi/ginkgo/com_github_onsi_ginkgo-v1.16.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/onsi/ginkgo/com_github_onsi_ginkgo-v1.16.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/onsi/ginkgo/com_github_onsi_ginkgo-v1.16.5.zip",
        ],
    )
    go_repository(
        name = "com_github_onsi_ginkgo_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/onsi/ginkgo/v2",
        sha256 = "f41e92baa52ec53d482603e4585c0906ca0c02e05004dca78a62bf1de88833ad",
        strip_prefix = "github.com/onsi/ginkgo/v2@v2.9.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/onsi/ginkgo/v2/com_github_onsi_ginkgo_v2-v2.9.4.zip",
            "http://ats.apps.svc/gomod/github.com/onsi/ginkgo/v2/com_github_onsi_ginkgo_v2-v2.9.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/onsi/ginkgo/v2/com_github_onsi_ginkgo_v2-v2.9.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/onsi/ginkgo/v2/com_github_onsi_ginkgo_v2-v2.9.4.zip",
        ],
    )
    go_repository(
        name = "com_github_onsi_gomega",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/onsi/gomega",
        sha256 = "ea2b22782cc15569645dfdfc066a651e1335626677ad92d7ba4358a0885bf369",
        strip_prefix = "github.com/onsi/gomega@v1.20.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/onsi/gomega/com_github_onsi_gomega-v1.20.1.zip",
            "http://ats.apps.svc/gomod/github.com/onsi/gomega/com_github_onsi_gomega-v1.20.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/onsi/gomega/com_github_onsi_gomega-v1.20.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/onsi/gomega/com_github_onsi_gomega-v1.20.1.zip",
        ],
    )
    go_repository(
        name = "com_github_opencontainers_go_digest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/go-digest",
        sha256 = "615efb31ff6cd71035b8aa38c3659d8b4da46f3cd92ac807cb50449adfe37c86",
        strip_prefix = "github.com/opencontainers/go-digest@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/opencontainers/go-digest/com_github_opencontainers_go_digest-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/opencontainers/go-digest/com_github_opencontainers_go_digest-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/opencontainers/go-digest/com_github_opencontainers_go_digest-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/opencontainers/go-digest/com_github_opencontainers_go_digest-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_opencontainers_image_spec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/image-spec",
        sha256 = "d842127b6038c1a74c2bb609d75bdde0ac9c7cde5c354ac82c4f953ce08d0c08",
        strip_prefix = "github.com/opencontainers/image-spec@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/opencontainers/image-spec/com_github_opencontainers_image_spec-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/opencontainers/image-spec/com_github_opencontainers_image_spec-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/opencontainers/image-spec/com_github_opencontainers_image_spec-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/opencontainers/image-spec/com_github_opencontainers_image_spec-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_openpeedeep_depguard_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/OpenPeeDeeP/depguard/v2",
        sha256 = "6a67e3856dcf09d9304fd7fc23d9f98b360aa2d85ffa4ccae14bf9a9d7e3fc28",
        strip_prefix = "github.com/OpenPeeDeeP/depguard/v2@v2.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/OpenPeeDeeP/depguard/v2/com_github_openpeedeep_depguard_v2-v2.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/OpenPeeDeeP/depguard/v2/com_github_openpeedeep_depguard_v2-v2.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/OpenPeeDeeP/depguard/v2/com_github_openpeedeep_depguard_v2-v2.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/OpenPeeDeeP/depguard/v2/com_github_openpeedeep_depguard_v2-v2.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_opentracing_basictracer_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing/basictracer-go",
        sha256 = "a908957c8e55b7b036b4761fb64c643806fcb9b59d4e7c6fcd03fca1105a9156",
        strip_prefix = "github.com/opentracing/basictracer-go@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/opentracing/basictracer-go/com_github_opentracing_basictracer_go-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/opentracing/basictracer-go/com_github_opentracing_basictracer_go-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/opentracing/basictracer-go/com_github_opentracing_basictracer_go-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/opentracing/basictracer-go/com_github_opentracing_basictracer_go-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_opentracing_opentracing_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing/opentracing-go",
        sha256 = "9b1a75e9a454a0cf01a26c18e48cd321e3b300943ac5adb9098ba033dbd40db5",
        strip_prefix = "github.com/opentracing/opentracing-go@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/opentracing/opentracing-go/com_github_opentracing_opentracing_go-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/opentracing/opentracing-go/com_github_opentracing_opentracing_go-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/opentracing/opentracing-go/com_github_opentracing_opentracing_go-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/opentracing/opentracing-go/com_github_opentracing_opentracing_go-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_otiai10_copy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/otiai10/copy",
        sha256 = "479272f4510470d86cd2eeba8509dfb2265852b0387bb184650646badcef48f7",
        strip_prefix = "github.com/otiai10/copy@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/otiai10/copy/com_github_otiai10_copy-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/otiai10/copy/com_github_otiai10_copy-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/otiai10/copy/com_github_otiai10_copy-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/otiai10/copy/com_github_otiai10_copy-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_otiai10_curr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/otiai10/curr",
        sha256 = "92b3cefe0f58f1b702f3ac92f352585b8ff25a6b35df0d0b6f3e299864de309f",
        strip_prefix = "github.com/otiai10/curr@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/otiai10/curr/com_github_otiai10_curr-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/otiai10/curr/com_github_otiai10_curr-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/otiai10/curr/com_github_otiai10_curr-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/otiai10/curr/com_github_otiai10_curr-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_otiai10_mint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/otiai10/mint",
        sha256 = "564d4d726a29a48adeb9c03e3755fc85a8329b7ec82202a24e3320f10358ae47",
        strip_prefix = "github.com/otiai10/mint@v1.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/otiai10/mint/com_github_otiai10_mint-v1.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/otiai10/mint/com_github_otiai10_mint-v1.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/otiai10/mint/com_github_otiai10_mint-v1.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/otiai10/mint/com_github_otiai10_mint-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_ovh_go_ovh",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ovh/go-ovh",
        sha256 = "011dc40423f453de4570f9ad737ff4185e0205aa11d294e1bd606fb70f07177b",
        strip_prefix = "github.com/ovh/go-ovh@v1.4.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ovh/go-ovh/com_github_ovh_go_ovh-v1.4.3.zip",
            "http://ats.apps.svc/gomod/github.com/ovh/go-ovh/com_github_ovh_go_ovh-v1.4.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ovh/go-ovh/com_github_ovh_go_ovh-v1.4.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ovh/go-ovh/com_github_ovh_go_ovh-v1.4.3.zip",
        ],
    )
    go_repository(
        name = "com_github_pborman_getopt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pborman/getopt",
        sha256 = "2c7e5c93709a3b3302d63f8239679d5b0c33f1dc0e1a18ce8167fb97df09f90a",
        strip_prefix = "github.com/pborman/getopt@v0.0.0-20180729010549-6fdd0a2c7117",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pborman/getopt/com_github_pborman_getopt-v0.0.0-20180729010549-6fdd0a2c7117.zip",
            "http://ats.apps.svc/gomod/github.com/pborman/getopt/com_github_pborman_getopt-v0.0.0-20180729010549-6fdd0a2c7117.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pborman/getopt/com_github_pborman_getopt-v0.0.0-20180729010549-6fdd0a2c7117.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pborman/getopt/com_github_pborman_getopt-v0.0.0-20180729010549-6fdd0a2c7117.zip",
        ],
    )
    go_repository(
        name = "com_github_pelletier_go_toml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pelletier/go-toml",
        sha256 = "de3dcda660cc800cd86d03273a25956d67f416e8fcbe4d2001a2cb4a01e6ac60",
        strip_prefix = "github.com/pelletier/go-toml@v1.9.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pelletier/go-toml/com_github_pelletier_go_toml-v1.9.5.zip",
            "http://ats.apps.svc/gomod/github.com/pelletier/go-toml/com_github_pelletier_go_toml-v1.9.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pelletier/go-toml/com_github_pelletier_go_toml-v1.9.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pelletier/go-toml/com_github_pelletier_go_toml-v1.9.5.zip",
        ],
    )
    go_repository(
        name = "com_github_pelletier_go_toml_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pelletier/go-toml/v2",
        sha256 = "f7550c7c319b1e80f47d23f191a8d1024063ad3c3879d77e5f225aa7b2140bfd",
        strip_prefix = "github.com/pelletier/go-toml/v2@v2.0.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pelletier/go-toml/v2/com_github_pelletier_go_toml_v2-v2.0.5.zip",
            "http://ats.apps.svc/gomod/github.com/pelletier/go-toml/v2/com_github_pelletier_go_toml_v2-v2.0.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pelletier/go-toml/v2/com_github_pelletier_go_toml_v2-v2.0.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pelletier/go-toml/v2/com_github_pelletier_go_toml_v2-v2.0.5.zip",
        ],
    )
    go_repository(
        name = "com_github_petermattis_goid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/petermattis/goid",
        sha256 = "c85422e507367742d767fb4102d312f959feec26c11122f91a0e5e73948740f7",
        strip_prefix = "github.com/petermattis/goid@v0.0.0-20231207134359-e60b3f734c67",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/petermattis/goid/com_github_petermattis_goid-v0.0.0-20231207134359-e60b3f734c67.zip",
            "http://ats.apps.svc/gomod/github.com/petermattis/goid/com_github_petermattis_goid-v0.0.0-20231207134359-e60b3f734c67.zip",
            "https://cache.hawkingrei.com/gomod/github.com/petermattis/goid/com_github_petermattis_goid-v0.0.0-20231207134359-e60b3f734c67.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/petermattis/goid/com_github_petermattis_goid-v0.0.0-20231207134359-e60b3f734c67.zip",
        ],
    )
    go_repository(
        name = "com_github_phayes_freeport",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/phayes/freeport",
        sha256 = "4ac97358de55a9b1ac60f13fdb223c5309a129fb3fb7bf731062f9c095a0796c",
        strip_prefix = "github.com/phayes/freeport@v0.0.0-20180830031419-95f893ade6f2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/phayes/freeport/com_github_phayes_freeport-v0.0.0-20180830031419-95f893ade6f2.zip",
            "http://ats.apps.svc/gomod/github.com/phayes/freeport/com_github_phayes_freeport-v0.0.0-20180830031419-95f893ade6f2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/phayes/freeport/com_github_phayes_freeport-v0.0.0-20180830031419-95f893ade6f2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/phayes/freeport/com_github_phayes_freeport-v0.0.0-20180830031419-95f893ade6f2.zip",
        ],
    )
    go_repository(
        name = "com_github_pierrec_lz4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pierrec/lz4",
        sha256 = "78594a301cfecaf409c3c814e3bbb86e8375dab3661f30ee206a59f3b1270421",
        strip_prefix = "github.com/pierrec/lz4@v2.6.1+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pierrec/lz4/com_github_pierrec_lz4-v2.6.1+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/pierrec/lz4/com_github_pierrec_lz4-v2.6.1+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pierrec/lz4/com_github_pierrec_lz4-v2.6.1+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pierrec/lz4/com_github_pierrec_lz4-v2.6.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_badger",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/badger",
        sha256 = "8dd7e3c05a5b69d2c1b6bbcccc1fcaaa9e9e9dcc79df7c3656594d9b261c344c",
        strip_prefix = "github.com/pingcap/badger@v1.5.1-0.20230103063557-828f39b09b6d",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/badger/com_github_pingcap_badger-v1.5.1-0.20230103063557-828f39b09b6d.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/badger/com_github_pingcap_badger-v1.5.1-0.20230103063557-828f39b09b6d.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/badger/com_github_pingcap_badger-v1.5.1-0.20230103063557-828f39b09b6d.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/badger/com_github_pingcap_badger-v1.5.1-0.20230103063557-828f39b09b6d.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/errors",
        sha256 = "0223d4f1e587bf6c0cf65fca39ada14d89501a56be1dd0f6a067a7bc628c8938",
        strip_prefix = "github.com/pingcap/errors@v0.11.5-0.20240311081613-f97970b88865",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/errors/com_github_pingcap_errors-v0.11.5-0.20240311081613-f97970b88865.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/errors/com_github_pingcap_errors-v0.11.5-0.20240311081613-f97970b88865.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/errors/com_github_pingcap_errors-v0.11.5-0.20240311081613-f97970b88865.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/errors/com_github_pingcap_errors-v0.11.5-0.20240311081613-f97970b88865.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_failpoint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/failpoint",
        sha256 = "ea37b4dddfbccaaed9b313f9f1099dfbf00d36d768a8416d6d175cbe2c8b1254",
        strip_prefix = "github.com/pingcap/failpoint@v0.0.0-20220801062533-2eaa32854a6c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/failpoint/com_github_pingcap_failpoint-v0.0.0-20220801062533-2eaa32854a6c.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/failpoint/com_github_pingcap_failpoint-v0.0.0-20220801062533-2eaa32854a6c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/failpoint/com_github_pingcap_failpoint-v0.0.0-20220801062533-2eaa32854a6c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/failpoint/com_github_pingcap_failpoint-v0.0.0-20220801062533-2eaa32854a6c.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_fn",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/fn",
        sha256 = "475d1567fdb2c9f630089100ce709d35fbaae2b4b1cf088a0581b98699443658",
        strip_prefix = "github.com/pingcap/fn@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/fn/com_github_pingcap_fn-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/fn/com_github_pingcap_fn-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/fn/com_github_pingcap_fn-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/fn/com_github_pingcap_fn-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_goleveldb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/goleveldb",
        sha256 = "12ddc24d12eeab431e3414be06a6e33976dcd7e2eb2fff9c015e6f9a77a66d53",
        strip_prefix = "github.com/pingcap/goleveldb@v0.0.0-20191226122134-f82aafb29989",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/goleveldb/com_github_pingcap_goleveldb-v0.0.0-20191226122134-f82aafb29989.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/goleveldb/com_github_pingcap_goleveldb-v0.0.0-20191226122134-f82aafb29989.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/goleveldb/com_github_pingcap_goleveldb-v0.0.0-20191226122134-f82aafb29989.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/goleveldb/com_github_pingcap_goleveldb-v0.0.0-20191226122134-f82aafb29989.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_kvproto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/kvproto",
        sha256 = "07dff29e9848e79f36ac8dcd0d5b48bbfbb2796308702451afb862accb79fedb",
        strip_prefix = "github.com/pingcap/kvproto@v0.0.0-20240227073058-929ab83f9754",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/kvproto/com_github_pingcap_kvproto-v0.0.0-20240227073058-929ab83f9754.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/kvproto/com_github_pingcap_kvproto-v0.0.0-20240227073058-929ab83f9754.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/kvproto/com_github_pingcap_kvproto-v0.0.0-20240227073058-929ab83f9754.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/kvproto/com_github_pingcap_kvproto-v0.0.0-20240227073058-929ab83f9754.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_log",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/log",
        sha256 = "c393f943bb688e240dcf9a50ac1d915aa27371803524887ab61a235834bb7438",
        strip_prefix = "github.com/pingcap/log@v1.1.1-0.20240314023424-862ccc32f18d",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/log/com_github_pingcap_log-v1.1.1-0.20240314023424-862ccc32f18d.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/log/com_github_pingcap_log-v1.1.1-0.20240314023424-862ccc32f18d.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/log/com_github_pingcap_log-v1.1.1-0.20240314023424-862ccc32f18d.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/log/com_github_pingcap_log-v1.1.1-0.20240314023424-862ccc32f18d.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_sysutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/sysutil",
        sha256 = "c2ffa4a6d163ca73e73831de5abaa25d47abba59c41b8a549d70935442921a56",
        strip_prefix = "github.com/pingcap/sysutil@v1.0.1-0.20240311050922-ae81ee01f3a5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/sysutil/com_github_pingcap_sysutil-v1.0.1-0.20240311050922-ae81ee01f3a5.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/sysutil/com_github_pingcap_sysutil-v1.0.1-0.20240311050922-ae81ee01f3a5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/sysutil/com_github_pingcap_sysutil-v1.0.1-0.20240311050922-ae81ee01f3a5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/sysutil/com_github_pingcap_sysutil-v1.0.1-0.20240311050922-ae81ee01f3a5.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_tipb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/tipb",
        sha256 = "5bd7643610ba897d50031477e327ebb02adc1985e69181902bda1b0fe0e66595",
        strip_prefix = "github.com/pingcap/tipb@v0.0.0-20240305085524-87f5b80908ab",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/tipb/com_github_pingcap_tipb-v0.0.0-20240305085524-87f5b80908ab.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/tipb/com_github_pingcap_tipb-v0.0.0-20240305085524-87f5b80908ab.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/tipb/com_github_pingcap_tipb-v0.0.0-20240305085524-87f5b80908ab.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/tipb/com_github_pingcap_tipb-v0.0.0-20240305085524-87f5b80908ab.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_browser",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/browser",
        sha256 = "415b8b7d7e47074cf3f6c2269d8712efa8a8433cba7bfce7eed22ca7f0b447a4",
        strip_prefix = "github.com/pkg/browser@v0.0.0-20210911075715-681adbf594b8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pkg/browser/com_github_pkg_browser-v0.0.0-20210911075715-681adbf594b8.zip",
            "http://ats.apps.svc/gomod/github.com/pkg/browser/com_github_pkg_browser-v0.0.0-20210911075715-681adbf594b8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pkg/browser/com_github_pkg_browser-v0.0.0-20210911075715-681adbf594b8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pkg/browser/com_github_pkg_browser-v0.0.0-20210911075715-681adbf594b8.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/diff",
        sha256 = "f35b23fdd2b9522ddd46cc5c0161b4f0765c514475d5d4ca2a86aca31388c8bd",
        strip_prefix = "github.com/pkg/diff@v0.0.0-20210226163009-20ebb0f2a09e",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pkg/diff/com_github_pkg_diff-v0.0.0-20210226163009-20ebb0f2a09e.zip",
            "http://ats.apps.svc/gomod/github.com/pkg/diff/com_github_pkg_diff-v0.0.0-20210226163009-20ebb0f2a09e.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pkg/diff/com_github_pkg_diff-v0.0.0-20210226163009-20ebb0f2a09e.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pkg/diff/com_github_pkg_diff-v0.0.0-20210226163009-20ebb0f2a09e.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/errors",
        sha256 = "d4c36b8bcd0616290a3913215e0f53b931bd6e00670596f2960df1b44af2bd07",
        strip_prefix = "github.com/pkg/errors@v0.9.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pkg/errors/com_github_pkg_errors-v0.9.1.zip",
            "http://ats.apps.svc/gomod/github.com/pkg/errors/com_github_pkg_errors-v0.9.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pkg/errors/com_github_pkg_errors-v0.9.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pkg/errors/com_github_pkg_errors-v0.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_profile",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/profile",
        sha256 = "0584fead1e820230c0b8910c3ce43668a1875c82e398faa0450c9e72c2d29c0a",
        strip_prefix = "github.com/pkg/profile@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pkg/profile/com_github_pkg_profile-v1.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/pkg/profile/com_github_pkg_profile-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pkg/profile/com_github_pkg_profile-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pkg/profile/com_github_pkg_profile-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_xattr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/xattr",
        sha256 = "03aa1ce578e02548201e7099bd53bd18a56d8cd7ae44bb7d8ab9457a5fb34b06",
        strip_prefix = "github.com/pkg/xattr@v0.4.9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pkg/xattr/com_github_pkg_xattr-v0.4.9.zip",
            "http://ats.apps.svc/gomod/github.com/pkg/xattr/com_github_pkg_xattr-v0.4.9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pkg/xattr/com_github_pkg_xattr-v0.4.9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pkg/xattr/com_github_pkg_xattr-v0.4.9.zip",
        ],
    )
    go_repository(
        name = "com_github_pmezard_go_difflib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pmezard/go-difflib",
        sha256 = "24ff45e356f638a53bd0c89fff961fbeaecfdb0dc5e482ceed0a2230e0e5f3b7",
        strip_prefix = "github.com/pmezard/go-difflib@v1.0.1-0.20181226105442-5d4384ee4fb2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pmezard/go-difflib/com_github_pmezard_go_difflib-v1.0.1-0.20181226105442-5d4384ee4fb2.zip",
            "http://ats.apps.svc/gomod/github.com/pmezard/go-difflib/com_github_pmezard_go_difflib-v1.0.1-0.20181226105442-5d4384ee4fb2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pmezard/go-difflib/com_github_pmezard_go_difflib-v1.0.1-0.20181226105442-5d4384ee4fb2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pmezard/go-difflib/com_github_pmezard_go_difflib-v1.0.1-0.20181226105442-5d4384ee4fb2.zip",
        ],
    )
    go_repository(
        name = "com_github_polyfloyd_go_errorlint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/polyfloyd/go-errorlint",
        sha256 = "e8e25aa733cc13a4c1a4301ffc2f657043bff25bec5e907f5dbfa5c33e32733e",
        strip_prefix = "github.com/polyfloyd/go-errorlint@v1.4.8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/polyfloyd/go-errorlint/com_github_polyfloyd_go_errorlint-v1.4.8.zip",
            "http://ats.apps.svc/gomod/github.com/polyfloyd/go-errorlint/com_github_polyfloyd_go_errorlint-v1.4.8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/polyfloyd/go-errorlint/com_github_polyfloyd_go_errorlint-v1.4.8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/polyfloyd/go-errorlint/com_github_polyfloyd_go_errorlint-v1.4.8.zip",
        ],
    )
    go_repository(
        name = "com_github_power_devops_perfstat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/power-devops/perfstat",
        sha256 = "4006b3bcc7ee4fa14a2075e64ae352f825afda0b3b62f227b5a5e1c0613af0fa",
        strip_prefix = "github.com/power-devops/perfstat@v0.0.0-20221212215047-62379fc7944b",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/power-devops/perfstat/com_github_power_devops_perfstat-v0.0.0-20221212215047-62379fc7944b.zip",
            "http://ats.apps.svc/gomod/github.com/power-devops/perfstat/com_github_power_devops_perfstat-v0.0.0-20221212215047-62379fc7944b.zip",
            "https://cache.hawkingrei.com/gomod/github.com/power-devops/perfstat/com_github_power_devops_perfstat-v0.0.0-20221212215047-62379fc7944b.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/power-devops/perfstat/com_github_power_devops_perfstat-v0.0.0-20221212215047-62379fc7944b.zip",
        ],
    )
    go_repository(
        name = "com_github_prashantv_gostub",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prashantv/gostub",
        sha256 = "9a56047ad14092b80489df340d6ff1adbb7db588f1558714dd5584f4d163d41e",
        strip_prefix = "github.com/prashantv/gostub@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prashantv/gostub/com_github_prashantv_gostub-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/prashantv/gostub/com_github_prashantv_gostub-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prashantv/gostub/com_github_prashantv_gostub-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prashantv/gostub/com_github_prashantv_gostub-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_alertmanager",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/alertmanager",
        sha256 = "7666007c7ccec339fd09aaeec1d15c5b8c26cb01d387c9a9f7273f904db825b0",
        strip_prefix = "github.com/prometheus/alertmanager@v0.26.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/alertmanager/com_github_prometheus_alertmanager-v0.26.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/alertmanager/com_github_prometheus_alertmanager-v0.26.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/alertmanager/com_github_prometheus_alertmanager-v0.26.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/alertmanager/com_github_prometheus_alertmanager-v0.26.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_client_golang",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/client_golang",
        sha256 = "304b411a9071773bba5e2739506412e5bc6ecacb6571ac465175554e5d50d0b3",
        strip_prefix = "github.com/prometheus/client_golang@v1.19.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/client_golang/com_github_prometheus_client_golang-v1.19.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/client_golang/com_github_prometheus_client_golang-v1.19.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/client_golang/com_github_prometheus_client_golang-v1.19.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/client_golang/com_github_prometheus_client_golang-v1.19.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_client_model",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/client_model",
        sha256 = "d2a1fcdd8ff430f66772419f13bc59310f4e2b786393366589200b56af747a91",
        strip_prefix = "github.com/prometheus/client_model@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/client_model/com_github_prometheus_client_model-v0.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/client_model/com_github_prometheus_client_model-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/client_model/com_github_prometheus_client_model-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/client_model/com_github_prometheus_client_model-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_common",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/common",
        sha256 = "62f8ef01a9303e8767a035de11b10ef05ec100275109ab17734af21dbc22fa09",
        strip_prefix = "github.com/prometheus/common@v0.48.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/common/com_github_prometheus_common-v0.48.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/common/com_github_prometheus_common-v0.48.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/common/com_github_prometheus_common-v0.48.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/common/com_github_prometheus_common-v0.48.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_common_assets",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/common/assets",
        sha256 = "e8bcf444eb69d4dc41764f84401d57a181d282250e4c97b3c2bb31edc93e984b",
        strip_prefix = "github.com/prometheus/common/assets@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/common/assets/com_github_prometheus_common_assets-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/common/assets/com_github_prometheus_common_assets-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/common/assets/com_github_prometheus_common_assets-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/common/assets/com_github_prometheus_common_assets-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_common_sigv4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/common/sigv4",
        sha256 = "e76ec796837158dc2624343f88da4ba3c5d9d4b45e66b359358eba5db39846dd",
        strip_prefix = "github.com/prometheus/common/sigv4@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/common/sigv4/com_github_prometheus_common_sigv4-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/common/sigv4/com_github_prometheus_common_sigv4-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/common/sigv4/com_github_prometheus_common_sigv4-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/common/sigv4/com_github_prometheus_common_sigv4-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_exporter_toolkit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/exporter-toolkit",
        sha256 = "d6d1eee3a082bd82744db81a52b01e4923932b498f92411ca57390e7489cf34b",
        strip_prefix = "github.com/prometheus/exporter-toolkit@v0.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/exporter-toolkit/com_github_prometheus_exporter_toolkit-v0.10.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/exporter-toolkit/com_github_prometheus_exporter_toolkit-v0.10.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/exporter-toolkit/com_github_prometheus_exporter_toolkit-v0.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/exporter-toolkit/com_github_prometheus_exporter_toolkit-v0.10.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_procfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/procfs",
        sha256 = "6fe923fc0ca170a60524d1031cf9ee634cb2eba16798edcbe1ed02e591994cfa",
        strip_prefix = "github.com/prometheus/procfs@v0.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/procfs/com_github_prometheus_procfs-v0.13.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/procfs/com_github_prometheus_procfs-v0.13.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/procfs/com_github_prometheus_procfs-v0.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/procfs/com_github_prometheus_procfs-v0.13.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_prometheus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/prometheus",
        sha256 = "70aa4f4a5d34ccb0425e89461637c0962502ad994ddcf1a48e87a75ebc0232b1",
        strip_prefix = "github.com/prometheus/prometheus@v0.49.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/prometheus/com_github_prometheus_prometheus-v0.49.1.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/prometheus/com_github_prometheus_prometheus-v0.49.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/prometheus/com_github_prometheus_prometheus-v0.49.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/prometheus/com_github_prometheus_prometheus-v0.49.1.zip",
        ],
    )
    go_repository(
        name = "com_github_quasilyte_go_ruleguard",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/go-ruleguard",
        sha256 = "3b00887b9a2f9f9b452520b8f0d52bef3c8e2ac03988ab7d3243b6a8afb9d3cd",
        strip_prefix = "github.com/quasilyte/go-ruleguard@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/quasilyte/go-ruleguard/com_github_quasilyte_go_ruleguard-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/quasilyte/go-ruleguard/com_github_quasilyte_go_ruleguard-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/quasilyte/go-ruleguard/com_github_quasilyte_go_ruleguard-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/quasilyte/go-ruleguard/com_github_quasilyte_go_ruleguard-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_quasilyte_go_ruleguard_dsl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/go-ruleguard/dsl",
        sha256 = "8770b31a1a936d800b61064c5d7684bbd57923ad51254e2507eaa04c8b75e5c1",
        strip_prefix = "github.com/quasilyte/go-ruleguard/dsl@v0.3.22",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/quasilyte/go-ruleguard/dsl/com_github_quasilyte_go_ruleguard_dsl-v0.3.22.zip",
            "http://ats.apps.svc/gomod/github.com/quasilyte/go-ruleguard/dsl/com_github_quasilyte_go_ruleguard_dsl-v0.3.22.zip",
            "https://cache.hawkingrei.com/gomod/github.com/quasilyte/go-ruleguard/dsl/com_github_quasilyte_go_ruleguard_dsl-v0.3.22.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/quasilyte/go-ruleguard/dsl/com_github_quasilyte_go_ruleguard_dsl-v0.3.22.zip",
        ],
    )
    go_repository(
        name = "com_github_quasilyte_gogrep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/gogrep",
        sha256 = "1829fbd111ee3f64ac594e8bfb7e7fcf8d89a2fc2e6563ebec3e33d677240b4f",
        strip_prefix = "github.com/quasilyte/gogrep@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/quasilyte/gogrep/com_github_quasilyte_gogrep-v0.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/quasilyte/gogrep/com_github_quasilyte_gogrep-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/quasilyte/gogrep/com_github_quasilyte_gogrep-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/quasilyte/gogrep/com_github_quasilyte_gogrep-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_quasilyte_regex_syntax",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/regex/syntax",
        sha256 = "59e43fa28684f36048d17ac869c87b145eae14c591625a18036b51be94b11f6d",
        strip_prefix = "github.com/quasilyte/regex/syntax@v0.0.0-20210819130434-b3f0c404a727",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/quasilyte/regex/syntax/com_github_quasilyte_regex_syntax-v0.0.0-20210819130434-b3f0c404a727.zip",
            "http://ats.apps.svc/gomod/github.com/quasilyte/regex/syntax/com_github_quasilyte_regex_syntax-v0.0.0-20210819130434-b3f0c404a727.zip",
            "https://cache.hawkingrei.com/gomod/github.com/quasilyte/regex/syntax/com_github_quasilyte_regex_syntax-v0.0.0-20210819130434-b3f0c404a727.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/quasilyte/regex/syntax/com_github_quasilyte_regex_syntax-v0.0.0-20210819130434-b3f0c404a727.zip",
        ],
    )
    go_repository(
        name = "com_github_quasilyte_stdinfo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/stdinfo",
        sha256 = "d411dd9c4a34df4cf217d9f0f05f45e3b6ef7deed6bdfbdd36aa4015646d5373",
        strip_prefix = "github.com/quasilyte/stdinfo@v0.0.0-20220114132959-f7386bf02567",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/quasilyte/stdinfo/com_github_quasilyte_stdinfo-v0.0.0-20220114132959-f7386bf02567.zip",
            "http://ats.apps.svc/gomod/github.com/quasilyte/stdinfo/com_github_quasilyte_stdinfo-v0.0.0-20220114132959-f7386bf02567.zip",
            "https://cache.hawkingrei.com/gomod/github.com/quasilyte/stdinfo/com_github_quasilyte_stdinfo-v0.0.0-20220114132959-f7386bf02567.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/quasilyte/stdinfo/com_github_quasilyte_stdinfo-v0.0.0-20220114132959-f7386bf02567.zip",
        ],
    )
    go_repository(
        name = "com_github_rcrowley_go_metrics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rcrowley/go-metrics",
        sha256 = "e4dbd20c185cb05019fd7d4a361266bd5d182938f49fd9577df4d12c16dc81c3",
        strip_prefix = "github.com/rcrowley/go-metrics@v0.0.0-20201227073835-cf1acfcdf475",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/rcrowley/go-metrics/com_github_rcrowley_go_metrics-v0.0.0-20201227073835-cf1acfcdf475.zip",
            "http://ats.apps.svc/gomod/github.com/rcrowley/go-metrics/com_github_rcrowley_go_metrics-v0.0.0-20201227073835-cf1acfcdf475.zip",
            "https://cache.hawkingrei.com/gomod/github.com/rcrowley/go-metrics/com_github_rcrowley_go_metrics-v0.0.0-20201227073835-cf1acfcdf475.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/rcrowley/go-metrics/com_github_rcrowley_go_metrics-v0.0.0-20201227073835-cf1acfcdf475.zip",
        ],
    )
    go_repository(
        name = "com_github_remyoudompheng_bigfft",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/remyoudompheng/bigfft",
        sha256 = "9be16c32c384d55d0f7bd7b03f1ff1e9a4e4b91b000f0aa87a567a01b9b82398",
        strip_prefix = "github.com/remyoudompheng/bigfft@v0.0.0-20230129092748-24d4a6f8daec",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/remyoudompheng/bigfft/com_github_remyoudompheng_bigfft-v0.0.0-20230129092748-24d4a6f8daec.zip",
            "http://ats.apps.svc/gomod/github.com/remyoudompheng/bigfft/com_github_remyoudompheng_bigfft-v0.0.0-20230129092748-24d4a6f8daec.zip",
            "https://cache.hawkingrei.com/gomod/github.com/remyoudompheng/bigfft/com_github_remyoudompheng_bigfft-v0.0.0-20230129092748-24d4a6f8daec.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/remyoudompheng/bigfft/com_github_remyoudompheng_bigfft-v0.0.0-20230129092748-24d4a6f8daec.zip",
        ],
    )
    go_repository(
        name = "com_github_rivo_uniseg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rivo/uniseg",
        sha256 = "eca600065be5a1ead37478e645ad07d70dadaf4f06f681827d81158316538b23",
        strip_prefix = "github.com/rivo/uniseg@v0.4.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/rivo/uniseg/com_github_rivo_uniseg-v0.4.6.zip",
            "http://ats.apps.svc/gomod/github.com/rivo/uniseg/com_github_rivo_uniseg-v0.4.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/rivo/uniseg/com_github_rivo_uniseg-v0.4.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/rivo/uniseg/com_github_rivo_uniseg-v0.4.6.zip",
        ],
    )
    go_repository(
        name = "com_github_robfig_cron_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/robfig/cron/v3",
        sha256 = "ebe6454642220832a451b8cc50eae5f9150fd8d36b90b242a5de27676be86c70",
        strip_prefix = "github.com/robfig/cron/v3@v3.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/robfig/cron/v3/com_github_robfig_cron_v3-v3.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/robfig/cron/v3/com_github_robfig_cron_v3-v3.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/robfig/cron/v3/com_github_robfig_cron_v3-v3.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/robfig/cron/v3/com_github_robfig_cron_v3-v3.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_rogpeppe_fastuuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rogpeppe/fastuuid",
        sha256 = "f9b8293f5e20270e26fb4214ca7afec864de92c73d03ff62b5ee29d1db4e72a1",
        strip_prefix = "github.com/rogpeppe/fastuuid@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/rogpeppe/fastuuid/com_github_rogpeppe_fastuuid-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/rogpeppe/fastuuid/com_github_rogpeppe_fastuuid-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/rogpeppe/fastuuid/com_github_rogpeppe_fastuuid-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/rogpeppe/fastuuid/com_github_rogpeppe_fastuuid-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_rogpeppe_go_internal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rogpeppe/go-internal",
        sha256 = "3629d4b2e457fdba5e9d51a376e2bead9b28a20696fa905b701c79250188c4e3",
        strip_prefix = "github.com/rogpeppe/go-internal@v1.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/rogpeppe/go-internal/com_github_rogpeppe_go_internal-v1.11.0.zip",
            "http://ats.apps.svc/gomod/github.com/rogpeppe/go-internal/com_github_rogpeppe_go_internal-v1.11.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/rogpeppe/go-internal/com_github_rogpeppe_go_internal-v1.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/rogpeppe/go-internal/com_github_rogpeppe_go_internal-v1.11.0.zip",
        ],
    )
    go_repository(
        name = "com_github_russross_blackfriday",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/russross/blackfriday",
        sha256 = "ba3408459608d91f693cffe853d2169116b8327c0f3c5d42e3818f43e41d1c87",
        strip_prefix = "github.com/russross/blackfriday@v1.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/russross/blackfriday/com_github_russross_blackfriday-v1.5.2.zip",
            "http://ats.apps.svc/gomod/github.com/russross/blackfriday/com_github_russross_blackfriday-v1.5.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/russross/blackfriday/com_github_russross_blackfriday-v1.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/russross/blackfriday/com_github_russross_blackfriday-v1.5.2.zip",
        ],
    )
    go_repository(
        name = "com_github_russross_blackfriday_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/russross/blackfriday/v2",
        sha256 = "7852750d58a053ce38b01f2c203208817564f552ebf371b2b630081d7004c6ae",
        strip_prefix = "github.com/russross/blackfriday/v2@v2.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/russross/blackfriday/v2/com_github_russross_blackfriday_v2-v2.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/russross/blackfriday/v2/com_github_russross_blackfriday_v2-v2.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/russross/blackfriday/v2/com_github_russross_blackfriday_v2-v2.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/russross/blackfriday/v2/com_github_russross_blackfriday_v2-v2.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ryancurrah_gomodguard",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryancurrah/gomodguard",
        sha256 = "b793513a8352361ec7311bfe6707333ad2892bb6830af91a87f02ea83f88d721",
        strip_prefix = "github.com/ryancurrah/gomodguard@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ryancurrah/gomodguard/com_github_ryancurrah_gomodguard-v1.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/ryancurrah/gomodguard/com_github_ryancurrah_gomodguard-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ryancurrah/gomodguard/com_github_ryancurrah_gomodguard-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ryancurrah/gomodguard/com_github_ryancurrah_gomodguard-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ryanrolds_sqlclosecheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryanrolds/sqlclosecheck",
        sha256 = "fdfe57b2a9d1b22c756acefaf4b9c254e1437e8d6ec7eb3400a8cb379a06a11b",
        strip_prefix = "github.com/ryanrolds/sqlclosecheck@v0.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ryanrolds/sqlclosecheck/com_github_ryanrolds_sqlclosecheck-v0.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/ryanrolds/sqlclosecheck/com_github_ryanrolds_sqlclosecheck-v0.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ryanrolds/sqlclosecheck/com_github_ryanrolds_sqlclosecheck-v0.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ryanrolds/sqlclosecheck/com_github_ryanrolds_sqlclosecheck-v0.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_ryanuber_columnize",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryanuber/columnize",
        sha256 = "ff687e133db2e470640e511c90cf474154941537a94cd97bb0cf7a28a7d00dc7",
        strip_prefix = "github.com/ryanuber/columnize@v2.1.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ryanuber/columnize/com_github_ryanuber_columnize-v2.1.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/ryanuber/columnize/com_github_ryanuber_columnize-v2.1.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ryanuber/columnize/com_github_ryanuber_columnize-v2.1.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ryanuber/columnize/com_github_ryanuber_columnize-v2.1.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_ryszard_goskiplist",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryszard/goskiplist",
        sha256 = "12c65729fc31d5a9bf246eb387bd4c268d0d68bf33b913cccd81bebd47d6f80d",
        strip_prefix = "github.com/ryszard/goskiplist@v0.0.0-20150312221310-2dfbae5fcf46",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ryszard/goskiplist/com_github_ryszard_goskiplist-v0.0.0-20150312221310-2dfbae5fcf46.zip",
            "http://ats.apps.svc/gomod/github.com/ryszard/goskiplist/com_github_ryszard_goskiplist-v0.0.0-20150312221310-2dfbae5fcf46.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ryszard/goskiplist/com_github_ryszard_goskiplist-v0.0.0-20150312221310-2dfbae5fcf46.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ryszard/goskiplist/com_github_ryszard_goskiplist-v0.0.0-20150312221310-2dfbae5fcf46.zip",
        ],
    )
    go_repository(
        name = "com_github_sanposhiho_wastedassign_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sanposhiho/wastedassign/v2",
        sha256 = "397cbeb6b185df9643c9de8a651bcecf621347543309c33b2c2e2e2794d872e2",
        strip_prefix = "github.com/sanposhiho/wastedassign/v2@v2.0.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sanposhiho/wastedassign/v2/com_github_sanposhiho_wastedassign_v2-v2.0.7.zip",
            "http://ats.apps.svc/gomod/github.com/sanposhiho/wastedassign/v2/com_github_sanposhiho_wastedassign_v2-v2.0.7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sanposhiho/wastedassign/v2/com_github_sanposhiho_wastedassign_v2-v2.0.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sanposhiho/wastedassign/v2/com_github_sanposhiho_wastedassign_v2-v2.0.7.zip",
        ],
    )
    go_repository(
        name = "com_github_sasha_s_go_deadlock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sasha-s/go-deadlock",
        sha256 = "82eaa020f254a21d5025b6cae9a908315ffa382f941ef228431c10177b9657d4",
        strip_prefix = "github.com/sasha-s/go-deadlock@v0.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sasha-s/go-deadlock/com_github_sasha_s_go_deadlock-v0.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/sasha-s/go-deadlock/com_github_sasha_s_go_deadlock-v0.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sasha-s/go-deadlock/com_github_sasha_s_go_deadlock-v0.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sasha-s/go-deadlock/com_github_sasha_s_go_deadlock-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_sashamelentyev_interfacebloat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sashamelentyev/interfacebloat",
        sha256 = "a1bd04f014594596e85a8d6fff2eb65a64cb9f05a83ed4766b76c3db74d7123a",
        strip_prefix = "github.com/sashamelentyev/interfacebloat@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sashamelentyev/interfacebloat/com_github_sashamelentyev_interfacebloat-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/sashamelentyev/interfacebloat/com_github_sashamelentyev_interfacebloat-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sashamelentyev/interfacebloat/com_github_sashamelentyev_interfacebloat-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sashamelentyev/interfacebloat/com_github_sashamelentyev_interfacebloat-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_sashamelentyev_usestdlibvars",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sashamelentyev/usestdlibvars",
        sha256 = "483070c7a6bce16675617d59126190d4631b2b4207426467cc5c35c4bf0b21c9",
        strip_prefix = "github.com/sashamelentyev/usestdlibvars@v1.25.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sashamelentyev/usestdlibvars/com_github_sashamelentyev_usestdlibvars-v1.25.0.zip",
            "http://ats.apps.svc/gomod/github.com/sashamelentyev/usestdlibvars/com_github_sashamelentyev_usestdlibvars-v1.25.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sashamelentyev/usestdlibvars/com_github_sashamelentyev_usestdlibvars-v1.25.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sashamelentyev/usestdlibvars/com_github_sashamelentyev_usestdlibvars-v1.25.0.zip",
        ],
    )
    go_repository(
        name = "com_github_scalalang2_golang_fifo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/scalalang2/golang-fifo",
        sha256 = "48ed9feefc3680b12116a212eaac53af5d6c7183ffe80ed1427eb8504a3b05cc",
        strip_prefix = "github.com/scalalang2/golang-fifo@v0.1.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/scalalang2/golang-fifo/com_github_scalalang2_golang_fifo-v0.1.5.zip",
            "http://ats.apps.svc/gomod/github.com/scalalang2/golang-fifo/com_github_scalalang2_golang_fifo-v0.1.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/scalalang2/golang-fifo/com_github_scalalang2_golang_fifo-v0.1.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/scalalang2/golang-fifo/com_github_scalalang2_golang_fifo-v0.1.5.zip",
        ],
    )
    go_repository(
        name = "com_github_scaleway_scaleway_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/scaleway/scaleway-sdk-go",
        sha256 = "c1c638a823b55c10a89bf55a501c55dc91ee2aced5e677d66748363923d34108",
        strip_prefix = "github.com/scaleway/scaleway-sdk-go@v1.0.0-beta.21",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/scaleway/scaleway-sdk-go/com_github_scaleway_scaleway_sdk_go-v1.0.0-beta.21.zip",
            "http://ats.apps.svc/gomod/github.com/scaleway/scaleway-sdk-go/com_github_scaleway_scaleway_sdk_go-v1.0.0-beta.21.zip",
            "https://cache.hawkingrei.com/gomod/github.com/scaleway/scaleway-sdk-go/com_github_scaleway_scaleway_sdk_go-v1.0.0-beta.21.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/scaleway/scaleway-sdk-go/com_github_scaleway_scaleway_sdk_go-v1.0.0-beta.21.zip",
        ],
    )
    go_repository(
        name = "com_github_sclevine_agouti",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sclevine/agouti",
        sha256 = "b20c8a6a2c1fda0ae6a9cd6d319e78a7a5afea4bc90810cd46b99246d8219d23",
        strip_prefix = "github.com/sclevine/agouti@v3.0.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sclevine/agouti/com_github_sclevine_agouti-v3.0.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/sclevine/agouti/com_github_sclevine_agouti-v3.0.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sclevine/agouti/com_github_sclevine_agouti-v3.0.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sclevine/agouti/com_github_sclevine_agouti-v3.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_securego_gosec_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/securego/gosec/v2",
        sha256 = "29e9318db81a1a3542dc9ce10edcd3423a13f5d67482fbb2855b608ef1a2d32c",
        strip_prefix = "github.com/securego/gosec/v2@v2.19.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/securego/gosec/v2/com_github_securego_gosec_v2-v2.19.0.zip",
            "http://ats.apps.svc/gomod/github.com/securego/gosec/v2/com_github_securego_gosec_v2-v2.19.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/securego/gosec/v2/com_github_securego_gosec_v2-v2.19.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/securego/gosec/v2/com_github_securego_gosec_v2-v2.19.0.zip",
        ],
    )
    go_repository(
        name = "com_github_segmentio_asm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/segmentio/asm",
        sha256 = "8e2815672f1ab3049b10185b5494006320c32afb419ccf9f14385bc25ea44def",
        strip_prefix = "github.com/segmentio/asm@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/segmentio/asm/com_github_segmentio_asm-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/segmentio/asm/com_github_segmentio_asm-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/segmentio/asm/com_github_segmentio_asm-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/segmentio/asm/com_github_segmentio_asm-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_sergi_go_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sergi/go-diff",
        sha256 = "a9c0203d5188745f59c29e170e8b1a7e7c8bd007634bce75932ffac042e43eac",
        strip_prefix = "github.com/sergi/go-diff@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sergi/go-diff/com_github_sergi_go_diff-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/sergi/go-diff/com_github_sergi_go_diff-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sergi/go-diff/com_github_sergi_go_diff-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sergi/go-diff/com_github_sergi_go_diff-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_shabbyrobe_gocovmerge",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shabbyrobe/gocovmerge",
        sha256 = "3c4cbe51a4350af0f4f042034e5b27470e7df81c842fb22d13cb73cdcba31b66",
        strip_prefix = "github.com/shabbyrobe/gocovmerge@v0.0.0-20190829150210-3e036491d500",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shabbyrobe/gocovmerge/com_github_shabbyrobe_gocovmerge-v0.0.0-20190829150210-3e036491d500.zip",
            "http://ats.apps.svc/gomod/github.com/shabbyrobe/gocovmerge/com_github_shabbyrobe_gocovmerge-v0.0.0-20190829150210-3e036491d500.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shabbyrobe/gocovmerge/com_github_shabbyrobe_gocovmerge-v0.0.0-20190829150210-3e036491d500.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shabbyrobe/gocovmerge/com_github_shabbyrobe_gocovmerge-v0.0.0-20190829150210-3e036491d500.zip",
        ],
    )
    go_repository(
        name = "com_github_shazow_go_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shazow/go-diff",
        sha256 = "74ce56e11770c59db446af8ffe6335e7f5e513d973353558095e846eff39ca61",
        strip_prefix = "github.com/shazow/go-diff@v0.0.0-20160112020656-b6b7b6733b8c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shazow/go-diff/com_github_shazow_go_diff-v0.0.0-20160112020656-b6b7b6733b8c.zip",
            "http://ats.apps.svc/gomod/github.com/shazow/go-diff/com_github_shazow_go_diff-v0.0.0-20160112020656-b6b7b6733b8c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shazow/go-diff/com_github_shazow_go_diff-v0.0.0-20160112020656-b6b7b6733b8c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shazow/go-diff/com_github_shazow_go_diff-v0.0.0-20160112020656-b6b7b6733b8c.zip",
        ],
    )
    go_repository(
        name = "com_github_shirou_gopsutil_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shirou/gopsutil/v3",
        sha256 = "c921a54b5981cd281113b3f605d45762352514c5c0d4c9c7e4d484723029cd5a",
        strip_prefix = "github.com/shirou/gopsutil/v3@v3.24.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shirou/gopsutil/v3/com_github_shirou_gopsutil_v3-v3.24.1.zip",
            "http://ats.apps.svc/gomod/github.com/shirou/gopsutil/v3/com_github_shirou_gopsutil_v3-v3.24.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shirou/gopsutil/v3/com_github_shirou_gopsutil_v3-v3.24.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shirou/gopsutil/v3/com_github_shirou_gopsutil_v3-v3.24.1.zip",
        ],
    )
    go_repository(
        name = "com_github_shoenig_go_m1cpu",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shoenig/go-m1cpu",
        sha256 = "0ceab2ec73ef7d1291bd7663dd39203ee7037ee9dccb6fc3381ad819bd8550d1",
        strip_prefix = "github.com/shoenig/go-m1cpu@v0.1.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shoenig/go-m1cpu/com_github_shoenig_go_m1cpu-v0.1.6.zip",
            "http://ats.apps.svc/gomod/github.com/shoenig/go-m1cpu/com_github_shoenig_go_m1cpu-v0.1.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shoenig/go-m1cpu/com_github_shoenig_go_m1cpu-v0.1.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shoenig/go-m1cpu/com_github_shoenig_go_m1cpu-v0.1.6.zip",
        ],
    )
    go_repository(
        name = "com_github_shoenig_test",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shoenig/test",
        sha256 = "c2f3912a0f4bb15e24d2c61beb63bd3093aafafb033c1ab71c0918c352df0781",
        strip_prefix = "github.com/shoenig/test@v0.6.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shoenig/test/com_github_shoenig_test-v0.6.4.zip",
            "http://ats.apps.svc/gomod/github.com/shoenig/test/com_github_shoenig_test-v0.6.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shoenig/test/com_github_shoenig_test-v0.6.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shoenig/test/com_github_shoenig_test-v0.6.4.zip",
        ],
    )
    go_repository(
        name = "com_github_shopify_goreferrer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Shopify/goreferrer",
        sha256 = "e47cdf750e6aa39707b90e62f4f87e97abb8d64b2525a16c021c82efb24f9969",
        strip_prefix = "github.com/Shopify/goreferrer@v0.0.0-20181106222321-ec9c9a553398",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Shopify/goreferrer/com_github_shopify_goreferrer-v0.0.0-20181106222321-ec9c9a553398.zip",
            "http://ats.apps.svc/gomod/github.com/Shopify/goreferrer/com_github_shopify_goreferrer-v0.0.0-20181106222321-ec9c9a553398.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Shopify/goreferrer/com_github_shopify_goreferrer-v0.0.0-20181106222321-ec9c9a553398.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Shopify/goreferrer/com_github_shopify_goreferrer-v0.0.0-20181106222321-ec9c9a553398.zip",
        ],
    )
    go_repository(
        name = "com_github_shopify_sarama",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Shopify/sarama",
        sha256 = "e40e234e595aee956281eb80bcdda92342f2686180313cfcb379e7bcd1d49b58",
        strip_prefix = "github.com/Shopify/sarama@v1.29.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Shopify/sarama/com_github_shopify_sarama-v1.29.0.zip",
            "http://ats.apps.svc/gomod/github.com/Shopify/sarama/com_github_shopify_sarama-v1.29.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Shopify/sarama/com_github_shopify_sarama-v1.29.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Shopify/sarama/com_github_shopify_sarama-v1.29.0.zip",
        ],
    )
    go_repository(
        name = "com_github_shopify_toxiproxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Shopify/toxiproxy",
        sha256 = "9427e70698ee6a906904dfa0652624f640619acef40652a1e5490e13b31e7f61",
        strip_prefix = "github.com/Shopify/toxiproxy@v2.1.4+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Shopify/toxiproxy/com_github_shopify_toxiproxy-v2.1.4+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/Shopify/toxiproxy/com_github_shopify_toxiproxy-v2.1.4+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Shopify/toxiproxy/com_github_shopify_toxiproxy-v2.1.4+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Shopify/toxiproxy/com_github_shopify_toxiproxy-v2.1.4+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_shopspring_decimal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shopspring/decimal",
        sha256 = "65c34c248e7f736cadf03a7caa0c0870d15499eb593f933fe106c96c2b7699a7",
        strip_prefix = "github.com/shopspring/decimal@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shopspring/decimal/com_github_shopspring_decimal-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/shopspring/decimal/com_github_shopspring_decimal-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shopspring/decimal/com_github_shopspring_decimal-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shopspring/decimal/com_github_shopspring_decimal-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_shurcool_httpfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/httpfs",
        sha256 = "4b3bea8ded4d221b448bf34d21cfe0b84d60faa71aa21ac2664c67009365d7f6",
        strip_prefix = "github.com/shurcooL/httpfs@v0.0.0-20230704072500-f1e31cf0ba5c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shurcooL/httpfs/com_github_shurcool_httpfs-v0.0.0-20230704072500-f1e31cf0ba5c.zip",
            "http://ats.apps.svc/gomod/github.com/shurcooL/httpfs/com_github_shurcool_httpfs-v0.0.0-20230704072500-f1e31cf0ba5c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shurcooL/httpfs/com_github_shurcool_httpfs-v0.0.0-20230704072500-f1e31cf0ba5c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shurcooL/httpfs/com_github_shurcool_httpfs-v0.0.0-20230704072500-f1e31cf0ba5c.zip",
        ],
    )
    go_repository(
        name = "com_github_shurcool_httpgzip",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/httpgzip",
        sha256 = "70ef73fce2f89d622f828cb439fd6c7b48a7fe63600410a8c0a936042c0e4631",
        strip_prefix = "github.com/shurcooL/httpgzip@v0.0.0-20190720172056-320755c1c1b0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shurcooL/httpgzip/com_github_shurcool_httpgzip-v0.0.0-20190720172056-320755c1c1b0.zip",
            "http://ats.apps.svc/gomod/github.com/shurcooL/httpgzip/com_github_shurcool_httpgzip-v0.0.0-20190720172056-320755c1c1b0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shurcooL/httpgzip/com_github_shurcool_httpgzip-v0.0.0-20190720172056-320755c1c1b0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shurcooL/httpgzip/com_github_shurcool_httpgzip-v0.0.0-20190720172056-320755c1c1b0.zip",
        ],
    )
    go_repository(
        name = "com_github_shurcool_sanitized_anchor_name",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/sanitized_anchor_name",
        sha256 = "0af034323e0627a9e94367f87aa50ce29e5b165d54c8da2926cbaffd5834f757",
        strip_prefix = "github.com/shurcooL/sanitized_anchor_name@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shurcooL/sanitized_anchor_name/com_github_shurcool_sanitized_anchor_name-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/shurcooL/sanitized_anchor_name/com_github_shurcool_sanitized_anchor_name-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shurcooL/sanitized_anchor_name/com_github_shurcool_sanitized_anchor_name-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shurcooL/sanitized_anchor_name/com_github_shurcool_sanitized_anchor_name-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_shurcool_vfsgen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/vfsgen",
        sha256 = "8a093681b21159514a1742b1a49e88fa2cf562673a5a0055e9abeb7ff590ee19",
        strip_prefix = "github.com/shurcooL/vfsgen@v0.0.0-20181202132449-6a9ea43bcacd",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shurcooL/vfsgen/com_github_shurcool_vfsgen-v0.0.0-20181202132449-6a9ea43bcacd.zip",
            "http://ats.apps.svc/gomod/github.com/shurcooL/vfsgen/com_github_shurcool_vfsgen-v0.0.0-20181202132449-6a9ea43bcacd.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shurcooL/vfsgen/com_github_shurcool_vfsgen-v0.0.0-20181202132449-6a9ea43bcacd.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shurcooL/vfsgen/com_github_shurcool_vfsgen-v0.0.0-20181202132449-6a9ea43bcacd.zip",
        ],
    )
    go_repository(
        name = "com_github_sirupsen_logrus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sirupsen/logrus",
        sha256 = "4501f4e6b858bfdd997671fcdd2f647a3178b29b6b4d1344caa7c07517121dd0",
        strip_prefix = "github.com/sirupsen/logrus@v1.9.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sirupsen/logrus/com_github_sirupsen_logrus-v1.9.3.zip",
            "http://ats.apps.svc/gomod/github.com/sirupsen/logrus/com_github_sirupsen_logrus-v1.9.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sirupsen/logrus/com_github_sirupsen_logrus-v1.9.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sirupsen/logrus/com_github_sirupsen_logrus-v1.9.3.zip",
        ],
    )
    go_repository(
        name = "com_github_sivchari_containedctx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sivchari/containedctx",
        sha256 = "d765afffc476a51173fa7622e44db9054c1467b85d024b03b5148c42b9182f60",
        strip_prefix = "github.com/sivchari/containedctx@v1.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sivchari/containedctx/com_github_sivchari_containedctx-v1.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/sivchari/containedctx/com_github_sivchari_containedctx-v1.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sivchari/containedctx/com_github_sivchari_containedctx-v1.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sivchari/containedctx/com_github_sivchari_containedctx-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_sivchari_nosnakecase",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sivchari/nosnakecase",
        sha256 = "555a2935c7908dc5eab61770a8d0c799d6876b9336786109f52ecdf32f6fa7a8",
        strip_prefix = "github.com/sivchari/nosnakecase@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sivchari/nosnakecase/com_github_sivchari_nosnakecase-v1.7.0.zip",
            "http://ats.apps.svc/gomod/github.com/sivchari/nosnakecase/com_github_sivchari_nosnakecase-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sivchari/nosnakecase/com_github_sivchari_nosnakecase-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sivchari/nosnakecase/com_github_sivchari_nosnakecase-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_sivchari_tenv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sivchari/tenv",
        sha256 = "619d861bc16c8a9cb98fbe8f8f417847ffbc11c1f9c9eac359d43121537a166e",
        strip_prefix = "github.com/sivchari/tenv@v1.7.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sivchari/tenv/com_github_sivchari_tenv-v1.7.1.zip",
            "http://ats.apps.svc/gomod/github.com/sivchari/tenv/com_github_sivchari_tenv-v1.7.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sivchari/tenv/com_github_sivchari_tenv-v1.7.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sivchari/tenv/com_github_sivchari_tenv-v1.7.1.zip",
        ],
    )
    go_repository(
        name = "com_github_smartystreets_assertions",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/smartystreets/assertions",
        sha256 = "0434c12c41cb4c3e7ebf2fa44d3eeaafa75b9a61a80e30408ed8e09c3d5d3d70",
        strip_prefix = "github.com/smartystreets/assertions@v0.0.0-20180927180507-b2de0cb4f26d",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/smartystreets/assertions/com_github_smartystreets_assertions-v0.0.0-20180927180507-b2de0cb4f26d.zip",
            "http://ats.apps.svc/gomod/github.com/smartystreets/assertions/com_github_smartystreets_assertions-v0.0.0-20180927180507-b2de0cb4f26d.zip",
            "https://cache.hawkingrei.com/gomod/github.com/smartystreets/assertions/com_github_smartystreets_assertions-v0.0.0-20180927180507-b2de0cb4f26d.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/smartystreets/assertions/com_github_smartystreets_assertions-v0.0.0-20180927180507-b2de0cb4f26d.zip",
        ],
    )
    go_repository(
        name = "com_github_smartystreets_goconvey",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/smartystreets/goconvey",
        sha256 = "a931413713a303a958a9c3ac31305498905fb91465e725552472462130396dda",
        strip_prefix = "github.com/smartystreets/goconvey@v1.6.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/smartystreets/goconvey/com_github_smartystreets_goconvey-v1.6.4.zip",
            "http://ats.apps.svc/gomod/github.com/smartystreets/goconvey/com_github_smartystreets_goconvey-v1.6.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/smartystreets/goconvey/com_github_smartystreets_goconvey-v1.6.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/smartystreets/goconvey/com_github_smartystreets_goconvey-v1.6.4.zip",
        ],
    )
    go_repository(
        name = "com_github_soheilhy_cmux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/soheilhy/cmux",
        sha256 = "27ae4f072970e4d09f4ecc75951b6cbc4dcf607da9ace4df4fb5a7a5f69054c0",
        strip_prefix = "github.com/soheilhy/cmux@v0.1.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/soheilhy/cmux/com_github_soheilhy_cmux-v0.1.5.zip",
            "http://ats.apps.svc/gomod/github.com/soheilhy/cmux/com_github_soheilhy_cmux-v0.1.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/soheilhy/cmux/com_github_soheilhy_cmux-v0.1.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/soheilhy/cmux/com_github_soheilhy_cmux-v0.1.5.zip",
        ],
    )
    go_repository(
        name = "com_github_sonatard_noctx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sonatard/noctx",
        sha256 = "19ac5f6cd6f1f85beed1e5ce4cb126da2c546f0a82e3ced6d4d969f50129f7bc",
        strip_prefix = "github.com/sonatard/noctx@v0.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sonatard/noctx/com_github_sonatard_noctx-v0.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/sonatard/noctx/com_github_sonatard_noctx-v0.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sonatard/noctx/com_github_sonatard_noctx-v0.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sonatard/noctx/com_github_sonatard_noctx-v0.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_sourcegraph_go_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sourcegraph/go-diff",
        sha256 = "893c60e8d7b38e88f029d560cb2bc5ce8402631b25e4bdd8f6d371f9a397b140",
        strip_prefix = "github.com/sourcegraph/go-diff@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sourcegraph/go-diff/com_github_sourcegraph_go_diff-v0.7.0.zip",
            "http://ats.apps.svc/gomod/github.com/sourcegraph/go-diff/com_github_sourcegraph_go_diff-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sourcegraph/go-diff/com_github_sourcegraph_go_diff-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sourcegraph/go-diff/com_github_sourcegraph_go_diff-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_afero",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/afero",
        sha256 = "70ae59086db6bfb64b509f597c8eff3d7ab56f8f0052f947f67b68899da3491c",
        strip_prefix = "github.com/spf13/afero@v1.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spf13/afero/com_github_spf13_afero-v1.11.0.zip",
            "http://ats.apps.svc/gomod/github.com/spf13/afero/com_github_spf13_afero-v1.11.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spf13/afero/com_github_spf13_afero-v1.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spf13/afero/com_github_spf13_afero-v1.11.0.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_cast",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/cast",
        sha256 = "0d6f70dc849ce1e56f2b50ceeac0a7eec9dd2b8414b556ad183a35cc5c84342a",
        strip_prefix = "github.com/spf13/cast@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spf13/cast/com_github_spf13_cast-v1.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/spf13/cast/com_github_spf13_cast-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spf13/cast/com_github_spf13_cast-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spf13/cast/com_github_spf13_cast-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_cobra",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/cobra",
        sha256 = "ba12924bbf9b40c3dfaddee45fb971a43908eb73fe0ffbbf7fd9e659e285c99c",
        strip_prefix = "github.com/spf13/cobra@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spf13/cobra/com_github_spf13_cobra-v1.8.0.zip",
            "http://ats.apps.svc/gomod/github.com/spf13/cobra/com_github_spf13_cobra-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spf13/cobra/com_github_spf13_cobra-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spf13/cobra/com_github_spf13_cobra-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_jwalterweatherman",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/jwalterweatherman",
        sha256 = "43cc5f056caf66dc8225dca36637bfc18509521b103a69ca76fbc2b6519194a3",
        strip_prefix = "github.com/spf13/jwalterweatherman@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spf13/jwalterweatherman/com_github_spf13_jwalterweatherman-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/spf13/jwalterweatherman/com_github_spf13_jwalterweatherman-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spf13/jwalterweatherman/com_github_spf13_jwalterweatherman-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spf13/jwalterweatherman/com_github_spf13_jwalterweatherman-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_pflag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/pflag",
        sha256 = "fc6e704f2f6a84ddcdce6de0404e5340fa20c8676181bf5d381b17888107ba84",
        strip_prefix = "github.com/spf13/pflag@v1.0.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spf13/pflag/com_github_spf13_pflag-v1.0.5.zip",
            "http://ats.apps.svc/gomod/github.com/spf13/pflag/com_github_spf13_pflag-v1.0.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spf13/pflag/com_github_spf13_pflag-v1.0.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spf13/pflag/com_github_spf13_pflag-v1.0.5.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_viper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/viper",
        sha256 = "51dcee7805a5d57f5c12fcc7be630045823d522cfab2b8436b4e595fc784108c",
        strip_prefix = "github.com/spf13/viper@v1.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spf13/viper/com_github_spf13_viper-v1.12.0.zip",
            "http://ats.apps.svc/gomod/github.com/spf13/viper/com_github_spf13_viper-v1.12.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spf13/viper/com_github_spf13_viper-v1.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spf13/viper/com_github_spf13_viper-v1.12.0.zip",
        ],
    )
    go_repository(
        name = "com_github_spkg_bom",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spkg/bom",
        sha256 = "e920b03c06974b4676684dca3d498ed20dfd1f7b995d704dc48eecf63101fc26",
        strip_prefix = "github.com/spkg/bom@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spkg/bom/com_github_spkg_bom-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/spkg/bom/com_github_spkg_bom-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spkg/bom/com_github_spkg_bom-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spkg/bom/com_github_spkg_bom-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ssgreg_nlreturn_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ssgreg/nlreturn/v2",
        sha256 = "0a1fdd4b7568deafbd5b8a28fef8ae0145cc1bc66365b03ebba542f6d2cfdd35",
        strip_prefix = "github.com/ssgreg/nlreturn/v2@v2.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ssgreg/nlreturn/v2/com_github_ssgreg_nlreturn_v2-v2.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/ssgreg/nlreturn/v2/com_github_ssgreg_nlreturn_v2-v2.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ssgreg/nlreturn/v2/com_github_ssgreg_nlreturn_v2-v2.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ssgreg/nlreturn/v2/com_github_ssgreg_nlreturn_v2-v2.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_stathat_consistent",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stathat/consistent",
        sha256 = "4e890b0a4d0fea70e2c8107c13af64029bfea8c0bd9ba7a97a105b84b263caaa",
        strip_prefix = "github.com/stathat/consistent@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/stathat/consistent/com_github_stathat_consistent-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/stathat/consistent/com_github_stathat_consistent-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/stathat/consistent/com_github_stathat_consistent-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/stathat/consistent/com_github_stathat_consistent-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_stbenjam_no_sprintf_host_port",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stbenjam/no-sprintf-host-port",
        sha256 = "06e7dd3f5352aece172ffbdf6ca59b5f9421bfd0779ef6852df4a0b29b7093d0",
        strip_prefix = "github.com/stbenjam/no-sprintf-host-port@v0.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/stbenjam/no-sprintf-host-port/com_github_stbenjam_no_sprintf_host_port-v0.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/stbenjam/no-sprintf-host-port/com_github_stbenjam_no_sprintf_host_port-v0.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/stbenjam/no-sprintf-host-port/com_github_stbenjam_no_sprintf_host_port-v0.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/stbenjam/no-sprintf-host-port/com_github_stbenjam_no_sprintf_host_port-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_stretchr_objx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stretchr/objx",
        sha256 = "3c22c1d1c4c4024eb16a12f0187775640bf35d51b0a06649febc7797119451c0",
        strip_prefix = "github.com/stretchr/objx@v0.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/stretchr/objx/com_github_stretchr_objx-v0.5.2.zip",
            "http://ats.apps.svc/gomod/github.com/stretchr/objx/com_github_stretchr_objx-v0.5.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/stretchr/objx/com_github_stretchr_objx-v0.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/stretchr/objx/com_github_stretchr_objx-v0.5.2.zip",
        ],
    )
    go_repository(
        name = "com_github_stretchr_testify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stretchr/testify",
        sha256 = "ee5d4f73cb689b1b5432c6908a189f9fbdb172507c49c32dbdf79b239ea9b8e0",
        strip_prefix = "github.com/stretchr/testify@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/stretchr/testify/com_github_stretchr_testify-v1.9.0.zip",
            "http://ats.apps.svc/gomod/github.com/stretchr/testify/com_github_stretchr_testify-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/stretchr/testify/com_github_stretchr_testify-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/stretchr/testify/com_github_stretchr_testify-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_github_subosito_gotenv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/subosito/gotenv",
        sha256 = "8a4468ab0e49d730116acd47911ebfa217e8237707bf7662671f10864be24372",
        strip_prefix = "github.com/subosito/gotenv@v1.4.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/subosito/gotenv/com_github_subosito_gotenv-v1.4.1.zip",
            "http://ats.apps.svc/gomod/github.com/subosito/gotenv/com_github_subosito_gotenv-v1.4.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/subosito/gotenv/com_github_subosito_gotenv-v1.4.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/subosito/gotenv/com_github_subosito_gotenv-v1.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_t_yuki_gocover_cobertura",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/t-yuki/gocover-cobertura",
        sha256 = "ffaddbf6d6c7d7064b450b5e6a0baf841399baa442810bc3173c28aa5a765082",
        strip_prefix = "github.com/t-yuki/gocover-cobertura@v0.0.0-20180217150009-aaee18c8195c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/t-yuki/gocover-cobertura/com_github_t_yuki_gocover_cobertura-v0.0.0-20180217150009-aaee18c8195c.zip",
            "http://ats.apps.svc/gomod/github.com/t-yuki/gocover-cobertura/com_github_t_yuki_gocover_cobertura-v0.0.0-20180217150009-aaee18c8195c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/t-yuki/gocover-cobertura/com_github_t_yuki_gocover_cobertura-v0.0.0-20180217150009-aaee18c8195c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/t-yuki/gocover-cobertura/com_github_t_yuki_gocover_cobertura-v0.0.0-20180217150009-aaee18c8195c.zip",
        ],
    )
    go_repository(
        name = "com_github_tdakkota_asciicheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tdakkota/asciicheck",
        sha256 = "b5bcd3f627a67d1e0e1303172b5accd226e2f25207a5c96bec895b0b6b0c3bd6",
        strip_prefix = "github.com/tdakkota/asciicheck@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tdakkota/asciicheck/com_github_tdakkota_asciicheck-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/tdakkota/asciicheck/com_github_tdakkota_asciicheck-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tdakkota/asciicheck/com_github_tdakkota_asciicheck-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tdakkota/asciicheck/com_github_tdakkota_asciicheck-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_tenntenn_modver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tenntenn/modver",
        sha256 = "534486db97677626935f51594e1cb7c3913a646338f5bfc43175dcf4110b2672",
        strip_prefix = "github.com/tenntenn/modver@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tenntenn/modver/com_github_tenntenn_modver-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/tenntenn/modver/com_github_tenntenn_modver-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tenntenn/modver/com_github_tenntenn_modver-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tenntenn/modver/com_github_tenntenn_modver-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_tenntenn_text_transform",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tenntenn/text/transform",
        sha256 = "41c601d6ef3c9ffa633b56069b2efb240810e0764ebbf70da1a697dde0bf8c5e",
        strip_prefix = "github.com/tenntenn/text/transform@v0.0.0-20200319021203-7eef512accb3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tenntenn/text/transform/com_github_tenntenn_text_transform-v0.0.0-20200319021203-7eef512accb3.zip",
            "http://ats.apps.svc/gomod/github.com/tenntenn/text/transform/com_github_tenntenn_text_transform-v0.0.0-20200319021203-7eef512accb3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tenntenn/text/transform/com_github_tenntenn_text_transform-v0.0.0-20200319021203-7eef512accb3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tenntenn/text/transform/com_github_tenntenn_text_transform-v0.0.0-20200319021203-7eef512accb3.zip",
        ],
    )
    go_repository(
        name = "com_github_tetafro_godot",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tetafro/godot",
        sha256 = "a262c88d1f320abf0f9027e90298b022469ed74103d9e4647e01c7fd90adce2f",
        strip_prefix = "github.com/tetafro/godot@v1.4.16",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tetafro/godot/com_github_tetafro_godot-v1.4.16.zip",
            "http://ats.apps.svc/gomod/github.com/tetafro/godot/com_github_tetafro_godot-v1.4.16.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tetafro/godot/com_github_tetafro_godot-v1.4.16.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tetafro/godot/com_github_tetafro_godot-v1.4.16.zip",
        ],
    )
    go_repository(
        name = "com_github_tiancaiamao_appdash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tiancaiamao/appdash",
        sha256 = "a9961e6079339aec983f97fdb39d5d7258bf8d2031da68482e58e17b27a93a78",
        strip_prefix = "github.com/tiancaiamao/appdash@v0.0.0-20181126055449-889f96f722a2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tiancaiamao/appdash/com_github_tiancaiamao_appdash-v0.0.0-20181126055449-889f96f722a2.zip",
            "http://ats.apps.svc/gomod/github.com/tiancaiamao/appdash/com_github_tiancaiamao_appdash-v0.0.0-20181126055449-889f96f722a2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tiancaiamao/appdash/com_github_tiancaiamao_appdash-v0.0.0-20181126055449-889f96f722a2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tiancaiamao/appdash/com_github_tiancaiamao_appdash-v0.0.0-20181126055449-889f96f722a2.zip",
        ],
    )
    go_repository(
        name = "com_github_tiancaiamao_gp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tiancaiamao/gp",
        sha256 = "0980e2518360748b37b801c1896550b4a37a3c1fc62ebbf90631e5c67de165d3",
        strip_prefix = "github.com/tiancaiamao/gp@v0.0.0-20221230034425-4025bc8a4d4a",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tiancaiamao/gp/com_github_tiancaiamao_gp-v0.0.0-20221230034425-4025bc8a4d4a.zip",
            "http://ats.apps.svc/gomod/github.com/tiancaiamao/gp/com_github_tiancaiamao_gp-v0.0.0-20221230034425-4025bc8a4d4a.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tiancaiamao/gp/com_github_tiancaiamao_gp-v0.0.0-20221230034425-4025bc8a4d4a.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tiancaiamao/gp/com_github_tiancaiamao_gp-v0.0.0-20221230034425-4025bc8a4d4a.zip",
        ],
    )
    go_repository(
        name = "com_github_tidwall_btree",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tidwall/btree",
        sha256 = "4a6619eb936c836841702933a9d66f27abe83b7ffb541de44d12db4aa3a809d5",
        strip_prefix = "github.com/tidwall/btree@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tidwall/btree/com_github_tidwall_btree-v1.7.0.zip",
            "http://ats.apps.svc/gomod/github.com/tidwall/btree/com_github_tidwall_btree-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tidwall/btree/com_github_tidwall_btree-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tidwall/btree/com_github_tidwall_btree-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_tikv_client_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tikv/client-go/v2",
        sha256 = "a64217d02a58e7fae026ad404d15f1422960c828a1d6aec377976444ed8b3440",
        strip_prefix = "github.com/tikv/client-go/v2@v2.0.8-0.20240315074230-0606e74e8e37",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tikv/client-go/v2/com_github_tikv_client_go_v2-v2.0.8-0.20240315074230-0606e74e8e37.zip",
            "http://ats.apps.svc/gomod/github.com/tikv/client-go/v2/com_github_tikv_client_go_v2-v2.0.8-0.20240315074230-0606e74e8e37.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tikv/client-go/v2/com_github_tikv_client_go_v2-v2.0.8-0.20240315074230-0606e74e8e37.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tikv/client-go/v2/com_github_tikv_client_go_v2-v2.0.8-0.20240315074230-0606e74e8e37.zip",
        ],
    )
    go_repository(
        name = "com_github_tikv_pd_client",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tikv/pd/client",
        sha256 = "d146feec4d22cef3825cf50e6bf5e6ab10179678ccb389ca5bf6e88f3c625294",
        strip_prefix = "github.com/tikv/pd/client@v0.0.0-20240229065730-92a31c12238e",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tikv/pd/client/com_github_tikv_pd_client-v0.0.0-20240229065730-92a31c12238e.zip",
            "http://ats.apps.svc/gomod/github.com/tikv/pd/client/com_github_tikv_pd_client-v0.0.0-20240229065730-92a31c12238e.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tikv/pd/client/com_github_tikv_pd_client-v0.0.0-20240229065730-92a31c12238e.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tikv/pd/client/com_github_tikv_pd_client-v0.0.0-20240229065730-92a31c12238e.zip",
        ],
    )
    go_repository(
        name = "com_github_timakin_bodyclose",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/timakin/bodyclose",
        sha256 = "3de254c19ef794681171109fb1776bdbb927dd857f314ce49ebcc18e61ea404f",
        strip_prefix = "github.com/timakin/bodyclose@v0.0.0-20240125160201-f835fa56326a",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/timakin/bodyclose/com_github_timakin_bodyclose-v0.0.0-20240125160201-f835fa56326a.zip",
            "http://ats.apps.svc/gomod/github.com/timakin/bodyclose/com_github_timakin_bodyclose-v0.0.0-20240125160201-f835fa56326a.zip",
            "https://cache.hawkingrei.com/gomod/github.com/timakin/bodyclose/com_github_timakin_bodyclose-v0.0.0-20240125160201-f835fa56326a.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/timakin/bodyclose/com_github_timakin_bodyclose-v0.0.0-20240125160201-f835fa56326a.zip",
        ],
    )
    go_repository(
        name = "com_github_timonwong_loggercheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/timonwong/loggercheck",
        sha256 = "888ee3060fe763b312ad712dc14f8584b4ea5a2cd221a03f7b35559f918c8863",
        strip_prefix = "github.com/timonwong/loggercheck@v0.9.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/timonwong/loggercheck/com_github_timonwong_loggercheck-v0.9.4.zip",
            "http://ats.apps.svc/gomod/github.com/timonwong/loggercheck/com_github_timonwong_loggercheck-v0.9.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/timonwong/loggercheck/com_github_timonwong_loggercheck-v0.9.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/timonwong/loggercheck/com_github_timonwong_loggercheck-v0.9.4.zip",
        ],
    )
    go_repository(
        name = "com_github_tklauser_go_sysconf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tklauser/go-sysconf",
        sha256 = "95a4a24b6f5fc5af05d94bbab39ff847a220d30c5adb4fb0a09f9c7926a2ffe8",
        strip_prefix = "github.com/tklauser/go-sysconf@v0.3.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tklauser/go-sysconf/com_github_tklauser_go_sysconf-v0.3.12.zip",
            "http://ats.apps.svc/gomod/github.com/tklauser/go-sysconf/com_github_tklauser_go_sysconf-v0.3.12.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tklauser/go-sysconf/com_github_tklauser_go_sysconf-v0.3.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tklauser/go-sysconf/com_github_tklauser_go_sysconf-v0.3.12.zip",
        ],
    )
    go_repository(
        name = "com_github_tklauser_numcpus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tklauser/numcpus",
        sha256 = "267c7f91c5be3f1d091ee215825f91c315196f45e1fd6c4e8abb447f38549e03",
        strip_prefix = "github.com/tklauser/numcpus@v0.6.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tklauser/numcpus/com_github_tklauser_numcpus-v0.6.1.zip",
            "http://ats.apps.svc/gomod/github.com/tklauser/numcpus/com_github_tklauser_numcpus-v0.6.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tklauser/numcpus/com_github_tklauser_numcpus-v0.6.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tklauser/numcpus/com_github_tklauser_numcpus-v0.6.1.zip",
        ],
    )
    go_repository(
        name = "com_github_tmc_grpc_websocket_proxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tmc/grpc-websocket-proxy",
        sha256 = "7773cd68e54dc6087f6b0d1dc2e71e85f3151aa5cd9858884870954266480e24",
        strip_prefix = "github.com/tmc/grpc-websocket-proxy@v0.0.0-20220101234140-673ab2c3ae75",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tmc/grpc-websocket-proxy/com_github_tmc_grpc_websocket_proxy-v0.0.0-20220101234140-673ab2c3ae75.zip",
            "http://ats.apps.svc/gomod/github.com/tmc/grpc-websocket-proxy/com_github_tmc_grpc_websocket_proxy-v0.0.0-20220101234140-673ab2c3ae75.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tmc/grpc-websocket-proxy/com_github_tmc_grpc_websocket_proxy-v0.0.0-20220101234140-673ab2c3ae75.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tmc/grpc-websocket-proxy/com_github_tmc_grpc_websocket_proxy-v0.0.0-20220101234140-673ab2c3ae75.zip",
        ],
    )
    go_repository(
        name = "com_github_tomarrell_wrapcheck_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tomarrell/wrapcheck/v2",
        sha256 = "eaba01b3ab8e890bca0ed32dc7c57b8b898f66a65b58368cee9382e7021624f9",
        strip_prefix = "github.com/tomarrell/wrapcheck/v2@v2.8.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tomarrell/wrapcheck/v2/com_github_tomarrell_wrapcheck_v2-v2.8.1.zip",
            "http://ats.apps.svc/gomod/github.com/tomarrell/wrapcheck/v2/com_github_tomarrell_wrapcheck_v2-v2.8.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tomarrell/wrapcheck/v2/com_github_tomarrell_wrapcheck_v2-v2.8.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tomarrell/wrapcheck/v2/com_github_tomarrell_wrapcheck_v2-v2.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_tommy_muehle_go_mnd_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tommy-muehle/go-mnd/v2",
        sha256 = "c23c7d903604381a09b9e1b1a63173bcf4c0cb179b8d0ae9975d1d0ace65172d",
        strip_prefix = "github.com/tommy-muehle/go-mnd/v2@v2.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tommy-muehle/go-mnd/v2/com_github_tommy_muehle_go_mnd_v2-v2.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/tommy-muehle/go-mnd/v2/com_github_tommy_muehle_go_mnd_v2-v2.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tommy-muehle/go-mnd/v2/com_github_tommy_muehle_go_mnd_v2-v2.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tommy-muehle/go-mnd/v2/com_github_tommy_muehle_go_mnd_v2-v2.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_twmb_murmur3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/twmb/murmur3",
        sha256 = "c51ce05e38d9e399654814fd3849eb6eca78d0a134972926fd36f53a1e182f12",
        strip_prefix = "github.com/twmb/murmur3@v1.1.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/twmb/murmur3/com_github_twmb_murmur3-v1.1.6.zip",
            "http://ats.apps.svc/gomod/github.com/twmb/murmur3/com_github_twmb_murmur3-v1.1.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/twmb/murmur3/com_github_twmb_murmur3-v1.1.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/twmb/murmur3/com_github_twmb_murmur3-v1.1.6.zip",
        ],
    )
    go_repository(
        name = "com_github_uber_jaeger_client_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber/jaeger-client-go",
        sha256 = "5c837b40527bd3a61b37f5c39739568b22fb72cbbad654931a567842c266a477",
        strip_prefix = "github.com/uber/jaeger-client-go@v2.22.1+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/uber/jaeger-client-go/com_github_uber_jaeger_client_go-v2.22.1+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/uber/jaeger-client-go/com_github_uber_jaeger_client_go-v2.22.1+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/uber/jaeger-client-go/com_github_uber_jaeger_client_go-v2.22.1+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/uber/jaeger-client-go/com_github_uber_jaeger_client_go-v2.22.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_uber_jaeger_lib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber/jaeger-lib",
        sha256 = "b43fc0c89c3c54498ae6108453ca2af987e074680742dd79bdceda94685a7efb",
        strip_prefix = "github.com/uber/jaeger-lib@v2.4.1+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/uber/jaeger-lib/com_github_uber_jaeger_lib-v2.4.1+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/uber/jaeger-lib/com_github_uber_jaeger_lib-v2.4.1+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/uber/jaeger-lib/com_github_uber_jaeger_lib-v2.4.1+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/uber/jaeger-lib/com_github_uber_jaeger_lib-v2.4.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_ugorji_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ugorji/go",
        sha256 = "9db847f1d70b63a1d95f8ea44eaa1b271d5cd00498c867bbff122be5e5516c0b",
        strip_prefix = "github.com/ugorji/go@v1.1.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ugorji/go/com_github_ugorji_go-v1.1.4.zip",
            "http://ats.apps.svc/gomod/github.com/ugorji/go/com_github_ugorji_go-v1.1.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ugorji/go/com_github_ugorji_go-v1.1.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ugorji/go/com_github_ugorji_go-v1.1.4.zip",
        ],
    )
    go_repository(
        name = "com_github_ugorji_go_codec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ugorji/go/codec",
        sha256 = "5de1462961c82567bcddfaf480d19b4fbf902adb9da8670dc3be1e6f2652f0f3",
        strip_prefix = "github.com/ugorji/go/codec@v0.0.0-20181204163529-d75b2dcb6bc8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ugorji/go/codec/com_github_ugorji_go_codec-v0.0.0-20181204163529-d75b2dcb6bc8.zip",
            "http://ats.apps.svc/gomod/github.com/ugorji/go/codec/com_github_ugorji_go_codec-v0.0.0-20181204163529-d75b2dcb6bc8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ugorji/go/codec/com_github_ugorji_go_codec-v0.0.0-20181204163529-d75b2dcb6bc8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ugorji/go/codec/com_github_ugorji_go_codec-v0.0.0-20181204163529-d75b2dcb6bc8.zip",
        ],
    )
    go_repository(
        name = "com_github_ultraware_funlen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ultraware/funlen",
        sha256 = "8c7d8c936def91004546b6ae231505373ce7863540ad826a7a9cd51a5aae0c0f",
        strip_prefix = "github.com/ultraware/funlen@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ultraware/funlen/com_github_ultraware_funlen-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/ultraware/funlen/com_github_ultraware_funlen-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ultraware/funlen/com_github_ultraware_funlen-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ultraware/funlen/com_github_ultraware_funlen-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ultraware_whitespace",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ultraware/whitespace",
        sha256 = "43c2ee4bae63b133741fd18677473fd5ff652c673f4137f3bebd85f34e9903fa",
        strip_prefix = "github.com/ultraware/whitespace@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ultraware/whitespace/com_github_ultraware_whitespace-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/ultraware/whitespace/com_github_ultraware_whitespace-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ultraware/whitespace/com_github_ultraware_whitespace-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ultraware/whitespace/com_github_ultraware_whitespace-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_urfave_negroni",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/urfave/negroni",
        sha256 = "7b50615961d34d748866565b8885edd7013e33812acdbaed47502d7cc73a4bbd",
        strip_prefix = "github.com/urfave/negroni@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/urfave/negroni/com_github_urfave_negroni-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/urfave/negroni/com_github_urfave_negroni-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/urfave/negroni/com_github_urfave_negroni-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/urfave/negroni/com_github_urfave_negroni-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_uudashr_gocognit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uudashr/gocognit",
        sha256 = "fc0b179859880b38cf2c356abac5261852afff67b7f61c8517a7f9b18f4a7c4e",
        strip_prefix = "github.com/uudashr/gocognit@v1.1.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/uudashr/gocognit/com_github_uudashr_gocognit-v1.1.2.zip",
            "http://ats.apps.svc/gomod/github.com/uudashr/gocognit/com_github_uudashr_gocognit-v1.1.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/uudashr/gocognit/com_github_uudashr_gocognit-v1.1.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/uudashr/gocognit/com_github_uudashr_gocognit-v1.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_valyala_bytebufferpool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/bytebufferpool",
        sha256 = "7f59f32c568539afee9a21a665a4156962b019beaac8404e26ba37af056b4f1e",
        strip_prefix = "github.com/valyala/bytebufferpool@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/valyala/bytebufferpool/com_github_valyala_bytebufferpool-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/valyala/bytebufferpool/com_github_valyala_bytebufferpool-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/valyala/bytebufferpool/com_github_valyala_bytebufferpool-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/valyala/bytebufferpool/com_github_valyala_bytebufferpool-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_valyala_fasthttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/fasthttp",
        sha256 = "b15a953ed5395599871097c94977d21c026205e6ca7ad6e340cd595096d5840e",
        strip_prefix = "github.com/valyala/fasthttp@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/valyala/fasthttp/com_github_valyala_fasthttp-v1.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/valyala/fasthttp/com_github_valyala_fasthttp-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/valyala/fasthttp/com_github_valyala_fasthttp-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/valyala/fasthttp/com_github_valyala_fasthttp-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_valyala_fasttemplate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/fasttemplate",
        sha256 = "b4d9f77c6c15a0404952925ad59b759102c0ff48426b6fc88d6bfd347fe243b8",
        strip_prefix = "github.com/valyala/fasttemplate@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/valyala/fasttemplate/com_github_valyala_fasttemplate-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/valyala/fasttemplate/com_github_valyala_fasttemplate-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/valyala/fasttemplate/com_github_valyala_fasttemplate-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/valyala/fasttemplate/com_github_valyala_fasttemplate-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_valyala_quicktemplate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/quicktemplate",
        sha256 = "047e3ef69c9088bc3c91ca3824c00a946d26f25d3825069c4046c927767d0052",
        strip_prefix = "github.com/valyala/quicktemplate@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/valyala/quicktemplate/com_github_valyala_quicktemplate-v1.7.0.zip",
            "http://ats.apps.svc/gomod/github.com/valyala/quicktemplate/com_github_valyala_quicktemplate-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/valyala/quicktemplate/com_github_valyala_quicktemplate-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/valyala/quicktemplate/com_github_valyala_quicktemplate-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_valyala_tcplisten",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/tcplisten",
        sha256 = "07066d5b879a94d6bc1feed20ad4003c62865975dd1f4c062673178be406206a",
        strip_prefix = "github.com/valyala/tcplisten@v0.0.0-20161114210144-ceec8f93295a",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/valyala/tcplisten/com_github_valyala_tcplisten-v0.0.0-20161114210144-ceec8f93295a.zip",
            "http://ats.apps.svc/gomod/github.com/valyala/tcplisten/com_github_valyala_tcplisten-v0.0.0-20161114210144-ceec8f93295a.zip",
            "https://cache.hawkingrei.com/gomod/github.com/valyala/tcplisten/com_github_valyala_tcplisten-v0.0.0-20161114210144-ceec8f93295a.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/valyala/tcplisten/com_github_valyala_tcplisten-v0.0.0-20161114210144-ceec8f93295a.zip",
        ],
    )
    go_repository(
        name = "com_github_vbauerster_mpb_v7",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vbauerster/mpb/v7",
        sha256 = "1b2efa91de6840a3d628fa2c3d48d3762d411e1a47a71b2e388b49ed2264cd38",
        strip_prefix = "github.com/vbauerster/mpb/v7@v7.5.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/vbauerster/mpb/v7/com_github_vbauerster_mpb_v7-v7.5.3.zip",
            "http://ats.apps.svc/gomod/github.com/vbauerster/mpb/v7/com_github_vbauerster_mpb_v7-v7.5.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/vbauerster/mpb/v7/com_github_vbauerster_mpb_v7-v7.5.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/vbauerster/mpb/v7/com_github_vbauerster_mpb_v7-v7.5.3.zip",
        ],
    )
    go_repository(
        name = "com_github_vividcortex_ewma",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/VividCortex/ewma",
        sha256 = "facfeeec2dac447211e733ed6f190e9068a8a89d770ea40b1d6955fa6cff36cf",
        strip_prefix = "github.com/VividCortex/ewma@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/VividCortex/ewma/com_github_vividcortex_ewma-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/VividCortex/ewma/com_github_vividcortex_ewma-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/VividCortex/ewma/com_github_vividcortex_ewma-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/VividCortex/ewma/com_github_vividcortex_ewma-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_vultr_govultr_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vultr/govultr/v2",
        sha256 = "3c8f94575d509164614b364a75529b1dab895c228a5b5516b7b6334c96e5094a",
        strip_prefix = "github.com/vultr/govultr/v2@v2.17.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/vultr/govultr/v2/com_github_vultr_govultr_v2-v2.17.2.zip",
            "http://ats.apps.svc/gomod/github.com/vultr/govultr/v2/com_github_vultr_govultr_v2-v2.17.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/vultr/govultr/v2/com_github_vultr_govultr_v2-v2.17.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/vultr/govultr/v2/com_github_vultr_govultr_v2-v2.17.2.zip",
        ],
    )
    go_repository(
        name = "com_github_wangjohn_quickselect",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/wangjohn/quickselect",
        sha256 = "90a1aa0080655f76952ef9362e3661c0d56899061a540a6504aedd50306e79f0",
        strip_prefix = "github.com/wangjohn/quickselect@v0.0.0-20161129230411-ed8402a42d5f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/wangjohn/quickselect/com_github_wangjohn_quickselect-v0.0.0-20161129230411-ed8402a42d5f.zip",
            "http://ats.apps.svc/gomod/github.com/wangjohn/quickselect/com_github_wangjohn_quickselect-v0.0.0-20161129230411-ed8402a42d5f.zip",
            "https://cache.hawkingrei.com/gomod/github.com/wangjohn/quickselect/com_github_wangjohn_quickselect-v0.0.0-20161129230411-ed8402a42d5f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/wangjohn/quickselect/com_github_wangjohn_quickselect-v0.0.0-20161129230411-ed8402a42d5f.zip",
        ],
    )
    go_repository(
        name = "com_github_xdg_scram",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xdg/scram",
        sha256 = "56875f465f0ed3170846db4d300328b9c769c35a3c59a479c8b9ac659765e48c",
        strip_prefix = "github.com/xdg/scram@v1.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xdg/scram/com_github_xdg_scram-v1.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/xdg/scram/com_github_xdg_scram-v1.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xdg/scram/com_github_xdg_scram-v1.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xdg/scram/com_github_xdg_scram-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_xdg_stringprep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xdg/stringprep",
        sha256 = "7cb9711fd7b3c1518e1fbd4e39be11737d7006a5e4a59f1ceb4ba9c205eb90fa",
        strip_prefix = "github.com/xdg/stringprep@v1.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xdg/stringprep/com_github_xdg_stringprep-v1.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/xdg/stringprep/com_github_xdg_stringprep-v1.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xdg/stringprep/com_github_xdg_stringprep-v1.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xdg/stringprep/com_github_xdg_stringprep-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonpointer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xeipuuv/gojsonpointer",
        sha256 = "5b1a4bcc8e003f214c92b3fa52959d9eb0e3af1c0c529efa55815db951146e48",
        strip_prefix = "github.com/xeipuuv/gojsonpointer@v0.0.0-20180127040702-4e3ac2762d5f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xeipuuv/gojsonpointer/com_github_xeipuuv_gojsonpointer-v0.0.0-20180127040702-4e3ac2762d5f.zip",
            "http://ats.apps.svc/gomod/github.com/xeipuuv/gojsonpointer/com_github_xeipuuv_gojsonpointer-v0.0.0-20180127040702-4e3ac2762d5f.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xeipuuv/gojsonpointer/com_github_xeipuuv_gojsonpointer-v0.0.0-20180127040702-4e3ac2762d5f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xeipuuv/gojsonpointer/com_github_xeipuuv_gojsonpointer-v0.0.0-20180127040702-4e3ac2762d5f.zip",
        ],
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonreference",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xeipuuv/gojsonreference",
        sha256 = "7ec98f4df894413f4dc58c8df330ca8b24ff425b05a8e1074c3028c99f7e45e7",
        strip_prefix = "github.com/xeipuuv/gojsonreference@v0.0.0-20180127040603-bd5ef7bd5415",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xeipuuv/gojsonreference/com_github_xeipuuv_gojsonreference-v0.0.0-20180127040603-bd5ef7bd5415.zip",
            "http://ats.apps.svc/gomod/github.com/xeipuuv/gojsonreference/com_github_xeipuuv_gojsonreference-v0.0.0-20180127040603-bd5ef7bd5415.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xeipuuv/gojsonreference/com_github_xeipuuv_gojsonreference-v0.0.0-20180127040603-bd5ef7bd5415.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xeipuuv/gojsonreference/com_github_xeipuuv_gojsonreference-v0.0.0-20180127040603-bd5ef7bd5415.zip",
        ],
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonschema",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xeipuuv/gojsonschema",
        sha256 = "55c8ce068257aa0d263aad7470113dafcd50f955ee754fc853c2fdcd31ad096f",
        strip_prefix = "github.com/xeipuuv/gojsonschema@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xeipuuv/gojsonschema/com_github_xeipuuv_gojsonschema-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/xeipuuv/gojsonschema/com_github_xeipuuv_gojsonschema-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xeipuuv/gojsonschema/com_github_xeipuuv_gojsonschema-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xeipuuv/gojsonschema/com_github_xeipuuv_gojsonschema-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_xen0n_gosmopolitan",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xen0n/gosmopolitan",
        sha256 = "bd5a8adb28bbaffd2b8cb7f7000cad640cd0dd3f1ce3e396958555665c45277b",
        strip_prefix = "github.com/xen0n/gosmopolitan@v1.2.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xen0n/gosmopolitan/com_github_xen0n_gosmopolitan-v1.2.2.zip",
            "http://ats.apps.svc/gomod/github.com/xen0n/gosmopolitan/com_github_xen0n_gosmopolitan-v1.2.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xen0n/gosmopolitan/com_github_xen0n_gosmopolitan-v1.2.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xen0n/gosmopolitan/com_github_xen0n_gosmopolitan-v1.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_xhit_go_str2duration_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xhit/go-str2duration/v2",
        sha256 = "907db1bdf362568191e659f82339c21a4031d433bc5ac52f36de23eeceb8cb26",
        strip_prefix = "github.com/xhit/go-str2duration/v2@v2.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xhit/go-str2duration/v2/com_github_xhit_go_str2duration_v2-v2.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/xhit/go-str2duration/v2/com_github_xhit_go_str2duration_v2-v2.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xhit/go-str2duration/v2/com_github_xhit_go_str2duration_v2-v2.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xhit/go-str2duration/v2/com_github_xhit_go_str2duration_v2-v2.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_xiang90_probing",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xiang90/probing",
        sha256 = "ee5b87f49c72ea40bddc94ed228874ba9fcd3a3745ad613011131147c773b3ff",
        strip_prefix = "github.com/xiang90/probing@v0.0.0-20221125231312-a49e3df8f510",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xiang90/probing/com_github_xiang90_probing-v0.0.0-20221125231312-a49e3df8f510.zip",
            "http://ats.apps.svc/gomod/github.com/xiang90/probing/com_github_xiang90_probing-v0.0.0-20221125231312-a49e3df8f510.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xiang90/probing/com_github_xiang90_probing-v0.0.0-20221125231312-a49e3df8f510.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xiang90/probing/com_github_xiang90_probing-v0.0.0-20221125231312-a49e3df8f510.zip",
        ],
    )
    go_repository(
        name = "com_github_xitongsys_parquet_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xitongsys/parquet-go",
        sha256 = "12f897439857389593402023bd18c9c4d21c19011d6a014910528a3ff3fad28e",
        strip_prefix = "github.com/xitongsys/parquet-go@v1.5.5-0.20201110004701-b09c49d6d457",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xitongsys/parquet-go/com_github_xitongsys_parquet_go-v1.5.5-0.20201110004701-b09c49d6d457.zip",
            "http://ats.apps.svc/gomod/github.com/xitongsys/parquet-go/com_github_xitongsys_parquet_go-v1.5.5-0.20201110004701-b09c49d6d457.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xitongsys/parquet-go/com_github_xitongsys_parquet_go-v1.5.5-0.20201110004701-b09c49d6d457.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xitongsys/parquet-go/com_github_xitongsys_parquet_go-v1.5.5-0.20201110004701-b09c49d6d457.zip",
        ],
    )
    go_repository(
        name = "com_github_xitongsys_parquet_go_source",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xitongsys/parquet-go-source",
        sha256 = "9fa786105465c7da0b4d0a3f334b5d284cce486229a0631e5bd962e4dc67cd50",
        strip_prefix = "github.com/xitongsys/parquet-go-source@v0.0.0-20200817004010-026bad9b25d0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xitongsys/parquet-go-source/com_github_xitongsys_parquet_go_source-v0.0.0-20200817004010-026bad9b25d0.zip",
            "http://ats.apps.svc/gomod/github.com/xitongsys/parquet-go-source/com_github_xitongsys_parquet_go_source-v0.0.0-20200817004010-026bad9b25d0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xitongsys/parquet-go-source/com_github_xitongsys_parquet_go_source-v0.0.0-20200817004010-026bad9b25d0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xitongsys/parquet-go-source/com_github_xitongsys_parquet_go_source-v0.0.0-20200817004010-026bad9b25d0.zip",
        ],
    )
    go_repository(
        name = "com_github_xordataexchange_crypt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xordataexchange/crypt",
        sha256 = "46dc29ef77d77a2bc3e7bd70c94dbaeec0062dd3bd6fcacbaab785c15dcd625b",
        strip_prefix = "github.com/xordataexchange/crypt@v0.0.3-0.20170626215501-b2862e3d0a77",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xordataexchange/crypt/com_github_xordataexchange_crypt-v0.0.3-0.20170626215501-b2862e3d0a77.zip",
            "http://ats.apps.svc/gomod/github.com/xordataexchange/crypt/com_github_xordataexchange_crypt-v0.0.3-0.20170626215501-b2862e3d0a77.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xordataexchange/crypt/com_github_xordataexchange_crypt-v0.0.3-0.20170626215501-b2862e3d0a77.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xordataexchange/crypt/com_github_xordataexchange_crypt-v0.0.3-0.20170626215501-b2862e3d0a77.zip",
        ],
    )
    go_repository(
        name = "com_github_yagipy_maintidx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yagipy/maintidx",
        sha256 = "a2b1f6b7c18ec97172872f416c18d20ad5e843c3b91c802290a27354113a653a",
        strip_prefix = "github.com/yagipy/maintidx@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/yagipy/maintidx/com_github_yagipy_maintidx-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/yagipy/maintidx/com_github_yagipy_maintidx-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/yagipy/maintidx/com_github_yagipy_maintidx-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/yagipy/maintidx/com_github_yagipy_maintidx-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_yalp_jsonpath",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yalp/jsonpath",
        sha256 = "2cb9c5b63fa0616fbcf73bc1c652f930212d243fdf5f73d1379921deff6dc051",
        strip_prefix = "github.com/yalp/jsonpath@v0.0.0-20180802001716-5cc68e5049a0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/yalp/jsonpath/com_github_yalp_jsonpath-v0.0.0-20180802001716-5cc68e5049a0.zip",
            "http://ats.apps.svc/gomod/github.com/yalp/jsonpath/com_github_yalp_jsonpath-v0.0.0-20180802001716-5cc68e5049a0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/yalp/jsonpath/com_github_yalp_jsonpath-v0.0.0-20180802001716-5cc68e5049a0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/yalp/jsonpath/com_github_yalp_jsonpath-v0.0.0-20180802001716-5cc68e5049a0.zip",
        ],
    )
    go_repository(
        name = "com_github_yeya24_promlinter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yeya24/promlinter",
        sha256 = "4cb02c5b5875f37d89ca8a908911f6943784e5348eedd2d7096d6d2e8e263f8c",
        strip_prefix = "github.com/yeya24/promlinter@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/yeya24/promlinter/com_github_yeya24_promlinter-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/yeya24/promlinter/com_github_yeya24_promlinter-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/yeya24/promlinter/com_github_yeya24_promlinter-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/yeya24/promlinter/com_github_yeya24_promlinter-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ykadowak_zerologlint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ykadowak/zerologlint",
        sha256 = "33e308a504d15d2e5b8bb8345ed0d4783c3a921e918691bda5c308fd3a744bcf",
        strip_prefix = "github.com/ykadowak/zerologlint@v0.1.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ykadowak/zerologlint/com_github_ykadowak_zerologlint-v0.1.5.zip",
            "http://ats.apps.svc/gomod/github.com/ykadowak/zerologlint/com_github_ykadowak_zerologlint-v0.1.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ykadowak/zerologlint/com_github_ykadowak_zerologlint-v0.1.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ykadowak/zerologlint/com_github_ykadowak_zerologlint-v0.1.5.zip",
        ],
    )
    go_repository(
        name = "com_github_yudai_gojsondiff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yudai/gojsondiff",
        sha256 = "90c457b595a661a25760d9f10cfda3fec27f7213c0e7026a5b97b30168e8f2d1",
        strip_prefix = "github.com/yudai/gojsondiff@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/yudai/gojsondiff/com_github_yudai_gojsondiff-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/yudai/gojsondiff/com_github_yudai_gojsondiff-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/yudai/gojsondiff/com_github_yudai_gojsondiff-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/yudai/gojsondiff/com_github_yudai_gojsondiff-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_yudai_golcs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yudai/golcs",
        sha256 = "ab50327aa849e409b14f5373543635fb53476792b65a1914f6f90c46fc64ee44",
        strip_prefix = "github.com/yudai/golcs@v0.0.0-20170316035057-ecda9a501e82",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/yudai/golcs/com_github_yudai_golcs-v0.0.0-20170316035057-ecda9a501e82.zip",
            "http://ats.apps.svc/gomod/github.com/yudai/golcs/com_github_yudai_golcs-v0.0.0-20170316035057-ecda9a501e82.zip",
            "https://cache.hawkingrei.com/gomod/github.com/yudai/golcs/com_github_yudai_golcs-v0.0.0-20170316035057-ecda9a501e82.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/yudai/golcs/com_github_yudai_golcs-v0.0.0-20170316035057-ecda9a501e82.zip",
        ],
    )
    go_repository(
        name = "com_github_yudai_pp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yudai/pp",
        sha256 = "ecfda4152182e295f2b21a7b2726e2865a9415fc135a955ce42e039db29e7a20",
        strip_prefix = "github.com/yudai/pp@v2.0.1+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/yudai/pp/com_github_yudai_pp-v2.0.1+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/yudai/pp/com_github_yudai_pp-v2.0.1+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/yudai/pp/com_github_yudai_pp-v2.0.1+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/yudai/pp/com_github_yudai_pp-v2.0.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_yuin_goldmark",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yuin/goldmark",
        sha256 = "bb41a602b174345fda392c8ad83fcc93217c285c763699677630be90feb7a5e3",
        strip_prefix = "github.com/yuin/goldmark@v1.4.13",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/yuin/goldmark/com_github_yuin_goldmark-v1.4.13.zip",
            "http://ats.apps.svc/gomod/github.com/yuin/goldmark/com_github_yuin_goldmark-v1.4.13.zip",
            "https://cache.hawkingrei.com/gomod/github.com/yuin/goldmark/com_github_yuin_goldmark-v1.4.13.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/yuin/goldmark/com_github_yuin_goldmark-v1.4.13.zip",
        ],
    )
    go_repository(
        name = "com_github_yusufpapurcu_wmi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yusufpapurcu/wmi",
        sha256 = "59e85d4487bc90217d5914ba20c34926b6ed6c780f1b2e84658cc9a069931cca",
        strip_prefix = "github.com/yusufpapurcu/wmi@v1.2.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/yusufpapurcu/wmi/com_github_yusufpapurcu_wmi-v1.2.3.zip",
            "http://ats.apps.svc/gomod/github.com/yusufpapurcu/wmi/com_github_yusufpapurcu_wmi-v1.2.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/yusufpapurcu/wmi/com_github_yusufpapurcu_wmi-v1.2.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/yusufpapurcu/wmi/com_github_yusufpapurcu_wmi-v1.2.3.zip",
        ],
    )
    go_repository(
        name = "com_gitlab_bosi_decorder",
        build_file_proto_mode = "disable_global",
        importpath = "gitlab.com/bosi/decorder",
        sha256 = "ada0aaccd3bee67d4eabb98c83c320f00c65dca36441ed92c5638cf269c60ba6",
        strip_prefix = "gitlab.com/bosi/decorder@v0.4.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gitlab.com/bosi/decorder/com_gitlab_bosi_decorder-v0.4.1.zip",
            "http://ats.apps.svc/gomod/gitlab.com/bosi/decorder/com_gitlab_bosi_decorder-v0.4.1.zip",
            "https://cache.hawkingrei.com/gomod/gitlab.com/bosi/decorder/com_gitlab_bosi_decorder-v0.4.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gitlab.com/bosi/decorder/com_gitlab_bosi_decorder-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go",
        sha256 = "b64ae080db87dc2827d84b409daaed7f4e42b56391db239e1f41e1bf076c1dd3",
        strip_prefix = "cloud.google.com/go@v0.112.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/com_google_cloud_go-v0.112.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/com_google_cloud_go-v0.112.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/com_google_cloud_go-v0.112.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/com_google_cloud_go-v0.112.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_accessapproval",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/accessapproval",
        sha256 = "48066ab6a359de0c060f5f427ae5c7ee0d10080b197d18dc1f2bd7108d16f9f3",
        strip_prefix = "cloud.google.com/go/accessapproval@v1.7.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/accessapproval/com_google_cloud_go_accessapproval-v1.7.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/accessapproval/com_google_cloud_go_accessapproval-v1.7.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/accessapproval/com_google_cloud_go_accessapproval-v1.7.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/accessapproval/com_google_cloud_go_accessapproval-v1.7.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_accesscontextmanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/accesscontextmanager",
        sha256 = "cc7ff5deab5067c41d6f3f68043868f088be52d1ce8582da7601f543ba393be5",
        strip_prefix = "cloud.google.com/go/accesscontextmanager@v1.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/accesscontextmanager/com_google_cloud_go_accesscontextmanager-v1.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/accesscontextmanager/com_google_cloud_go_accesscontextmanager-v1.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/accesscontextmanager/com_google_cloud_go_accesscontextmanager-v1.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/accesscontextmanager/com_google_cloud_go_accesscontextmanager-v1.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_aiplatform",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/aiplatform",
        sha256 = "ad7c373d618de9c619486880fc1803cac8ab0f90238fc6a6aee5c3a870efaff5",
        strip_prefix = "cloud.google.com/go/aiplatform@v1.60.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/aiplatform/com_google_cloud_go_aiplatform-v1.60.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/aiplatform/com_google_cloud_go_aiplatform-v1.60.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/aiplatform/com_google_cloud_go_aiplatform-v1.60.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/aiplatform/com_google_cloud_go_aiplatform-v1.60.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_analytics",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/analytics",
        sha256 = "9af1681ba6c9090c51b227f5f26137f6a139258587cc569b367e424f4974e556",
        strip_prefix = "cloud.google.com/go/analytics@v0.23.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/analytics/com_google_cloud_go_analytics-v0.23.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/analytics/com_google_cloud_go_analytics-v0.23.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/analytics/com_google_cloud_go_analytics-v0.23.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/analytics/com_google_cloud_go_analytics-v0.23.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_apigateway",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigateway",
        sha256 = "66cb6ae25ac2d5e983c2281f9b68ae72baef1697e55eace91360606c7cebd22f",
        strip_prefix = "cloud.google.com/go/apigateway@v1.6.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/apigateway/com_google_cloud_go_apigateway-v1.6.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/apigateway/com_google_cloud_go_apigateway-v1.6.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/apigateway/com_google_cloud_go_apigateway-v1.6.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/apigateway/com_google_cloud_go_apigateway-v1.6.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_apigeeconnect",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigeeconnect",
        sha256 = "2047e90bdc5a103ceab7747f6afc8ba7cd3e62333a408d8d97dd44ca30f7b125",
        strip_prefix = "cloud.google.com/go/apigeeconnect@v1.6.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/apigeeconnect/com_google_cloud_go_apigeeconnect-v1.6.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/apigeeconnect/com_google_cloud_go_apigeeconnect-v1.6.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/apigeeconnect/com_google_cloud_go_apigeeconnect-v1.6.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/apigeeconnect/com_google_cloud_go_apigeeconnect-v1.6.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_apigeeregistry",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigeeregistry",
        sha256 = "b30180fda8417c97a5ecd039552c5a45222be85936227267831bbac135870505",
        strip_prefix = "cloud.google.com/go/apigeeregistry@v0.8.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/apigeeregistry/com_google_cloud_go_apigeeregistry-v0.8.3.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/apigeeregistry/com_google_cloud_go_apigeeregistry-v0.8.3.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/apigeeregistry/com_google_cloud_go_apigeeregistry-v0.8.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/apigeeregistry/com_google_cloud_go_apigeeregistry-v0.8.3.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_appengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/appengine",
        sha256 = "55f6ffdadd031dd49c8e07bbc2df97f17025c5b273bc03160c75fff7542c8cec",
        strip_prefix = "cloud.google.com/go/appengine@v1.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/appengine/com_google_cloud_go_appengine-v1.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/appengine/com_google_cloud_go_appengine-v1.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/appengine/com_google_cloud_go_appengine-v1.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/appengine/com_google_cloud_go_appengine-v1.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_area120",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/area120",
        sha256 = "215a423244d6e4079ceb47935ef4435e710e15c1d354aef2b7adc91dd2379091",
        strip_prefix = "cloud.google.com/go/area120@v0.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/area120/com_google_cloud_go_area120-v0.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/area120/com_google_cloud_go_area120-v0.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/area120/com_google_cloud_go_area120-v0.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/area120/com_google_cloud_go_area120-v0.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_artifactregistry",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/artifactregistry",
        sha256 = "811813420ecafb28fd83630ec085c5c8c18048978d357dabfdf56699c34c1b69",
        strip_prefix = "cloud.google.com/go/artifactregistry@v1.14.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/artifactregistry/com_google_cloud_go_artifactregistry-v1.14.7.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/artifactregistry/com_google_cloud_go_artifactregistry-v1.14.7.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/artifactregistry/com_google_cloud_go_artifactregistry-v1.14.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/artifactregistry/com_google_cloud_go_artifactregistry-v1.14.7.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_asset",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/asset",
        sha256 = "16a77c7774c87fe0a0f87b772411a1980c077db3f71692de6faa208d9ce45d52",
        strip_prefix = "cloud.google.com/go/asset@v1.17.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/asset/com_google_cloud_go_asset-v1.17.2.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/asset/com_google_cloud_go_asset-v1.17.2.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/asset/com_google_cloud_go_asset-v1.17.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/asset/com_google_cloud_go_asset-v1.17.2.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_assuredworkloads",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/assuredworkloads",
        sha256 = "232f945a5f780c968089e5c9a03c6081e8c0256aa8d93d4cf1ea1b5e22a0f178",
        strip_prefix = "cloud.google.com/go/assuredworkloads@v1.11.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/assuredworkloads/com_google_cloud_go_assuredworkloads-v1.11.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/assuredworkloads/com_google_cloud_go_assuredworkloads-v1.11.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/assuredworkloads/com_google_cloud_go_assuredworkloads-v1.11.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/assuredworkloads/com_google_cloud_go_assuredworkloads-v1.11.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_automl",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/automl",
        sha256 = "fd3fd5c3c639bb85331411260f3aca150bac0daea62c37ca1a8f85933a1984d1",
        strip_prefix = "cloud.google.com/go/automl@v1.13.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/automl/com_google_cloud_go_automl-v1.13.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/automl/com_google_cloud_go_automl-v1.13.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/automl/com_google_cloud_go_automl-v1.13.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/automl/com_google_cloud_go_automl-v1.13.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_baremetalsolution",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/baremetalsolution",
        sha256 = "cb51f2f4a79130b7ee2144526da55951318df4ed271f1559956e910488c49fbe",
        strip_prefix = "cloud.google.com/go/baremetalsolution@v1.2.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/baremetalsolution/com_google_cloud_go_baremetalsolution-v1.2.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/baremetalsolution/com_google_cloud_go_baremetalsolution-v1.2.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/baremetalsolution/com_google_cloud_go_baremetalsolution-v1.2.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/baremetalsolution/com_google_cloud_go_baremetalsolution-v1.2.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_batch",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/batch",
        sha256 = "009c51e5067877c2cb63c16ae70bdced460b73d723f6e318e629632771ab6917",
        strip_prefix = "cloud.google.com/go/batch@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/batch/com_google_cloud_go_batch-v1.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/batch/com_google_cloud_go_batch-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/batch/com_google_cloud_go_batch-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/batch/com_google_cloud_go_batch-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_beyondcorp",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/beyondcorp",
        sha256 = "c65fae2e6401e2d847a9590df932e86e6226f504357203357e33bb31634f9a16",
        strip_prefix = "cloud.google.com/go/beyondcorp@v1.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/beyondcorp/com_google_cloud_go_beyondcorp-v1.0.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/beyondcorp/com_google_cloud_go_beyondcorp-v1.0.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/beyondcorp/com_google_cloud_go_beyondcorp-v1.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/beyondcorp/com_google_cloud_go_beyondcorp-v1.0.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/bigquery",
        sha256 = "50bb376bc1ced07fc35ed7a3e6ebe043b0fec289e17dfabbfe32ef0b5113ca54",
        strip_prefix = "cloud.google.com/go/bigquery@v1.59.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/bigquery/com_google_cloud_go_bigquery-v1.59.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/bigquery/com_google_cloud_go_bigquery-v1.59.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/bigquery/com_google_cloud_go_bigquery-v1.59.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/bigquery/com_google_cloud_go_bigquery-v1.59.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_billing",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/billing",
        sha256 = "ff169192f71f00fd632a525600b11550badf5965badc9ea3e537facec86cdbf1",
        strip_prefix = "cloud.google.com/go/billing@v1.18.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/billing/com_google_cloud_go_billing-v1.18.2.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/billing/com_google_cloud_go_billing-v1.18.2.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/billing/com_google_cloud_go_billing-v1.18.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/billing/com_google_cloud_go_billing-v1.18.2.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_binaryauthorization",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/binaryauthorization",
        sha256 = "dee98d01d410ad8b4923c657955f96921aeea6166172b7893eb3d1f09c6aaa0a",
        strip_prefix = "cloud.google.com/go/binaryauthorization@v1.8.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/binaryauthorization/com_google_cloud_go_binaryauthorization-v1.8.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/binaryauthorization/com_google_cloud_go_binaryauthorization-v1.8.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/binaryauthorization/com_google_cloud_go_binaryauthorization-v1.8.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/binaryauthorization/com_google_cloud_go_binaryauthorization-v1.8.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_certificatemanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/certificatemanager",
        sha256 = "780e8e315a9f7225546b6673356c2229f72219410346b95207ac049511b98841",
        strip_prefix = "cloud.google.com/go/certificatemanager@v1.7.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/certificatemanager/com_google_cloud_go_certificatemanager-v1.7.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/certificatemanager/com_google_cloud_go_certificatemanager-v1.7.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/certificatemanager/com_google_cloud_go_certificatemanager-v1.7.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/certificatemanager/com_google_cloud_go_certificatemanager-v1.7.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_channel",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/channel",
        sha256 = "73db84def08affd03be8b491c903a020eac8b37cb12fb8dcad4eeeaa4993e25d",
        strip_prefix = "cloud.google.com/go/channel@v1.17.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/channel/com_google_cloud_go_channel-v1.17.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/channel/com_google_cloud_go_channel-v1.17.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/channel/com_google_cloud_go_channel-v1.17.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/channel/com_google_cloud_go_channel-v1.17.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_cloudbuild",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/cloudbuild",
        sha256 = "f6ee875558b6af58a958f7e186258352268943552f6aa14b550375ec91a151bd",
        strip_prefix = "cloud.google.com/go/cloudbuild@v1.15.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/cloudbuild/com_google_cloud_go_cloudbuild-v1.15.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/cloudbuild/com_google_cloud_go_cloudbuild-v1.15.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/cloudbuild/com_google_cloud_go_cloudbuild-v1.15.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/cloudbuild/com_google_cloud_go_cloudbuild-v1.15.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_clouddms",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/clouddms",
        sha256 = "75062644daa91a5d0a8988a779a7b78af897d50d1136d158744a6218ae4be50d",
        strip_prefix = "cloud.google.com/go/clouddms@v1.7.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/clouddms/com_google_cloud_go_clouddms-v1.7.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/clouddms/com_google_cloud_go_clouddms-v1.7.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/clouddms/com_google_cloud_go_clouddms-v1.7.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/clouddms/com_google_cloud_go_clouddms-v1.7.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_cloudtasks",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/cloudtasks",
        sha256 = "2bc9e56b75f82d47c912fdab8a4bdf498e90af3e798c1e36d41c129edfec19c2",
        strip_prefix = "cloud.google.com/go/cloudtasks@v1.12.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/cloudtasks/com_google_cloud_go_cloudtasks-v1.12.6.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/cloudtasks/com_google_cloud_go_cloudtasks-v1.12.6.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/cloudtasks/com_google_cloud_go_cloudtasks-v1.12.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/cloudtasks/com_google_cloud_go_cloudtasks-v1.12.6.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/compute",
        sha256 = "0cf3d4325e378c92ff90cef3d1b7752682a77f0eaa0b11c092cc3ea32e5ed638",
        strip_prefix = "cloud.google.com/go/compute@v1.24.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/compute/com_google_cloud_go_compute-v1.24.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/compute/com_google_cloud_go_compute-v1.24.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/compute/com_google_cloud_go_compute-v1.24.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/compute/com_google_cloud_go_compute-v1.24.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_compute_metadata",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/compute/metadata",
        sha256 = "292864dbd0b1de37a968e285e949885e573384837d81cd3695be5ce2e2391887",
        strip_prefix = "cloud.google.com/go/compute/metadata@v0.2.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/compute/metadata/com_google_cloud_go_compute_metadata-v0.2.3.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/compute/metadata/com_google_cloud_go_compute_metadata-v0.2.3.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/compute/metadata/com_google_cloud_go_compute_metadata-v0.2.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/compute/metadata/com_google_cloud_go_compute_metadata-v0.2.3.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_contactcenterinsights",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/contactcenterinsights",
        sha256 = "69a6ecff3f5d040e62b61555d71406bb8c87dbe07addc05d30a0e8bb935d55b0",
        strip_prefix = "cloud.google.com/go/contactcenterinsights@v1.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/contactcenterinsights/com_google_cloud_go_contactcenterinsights-v1.13.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/contactcenterinsights/com_google_cloud_go_contactcenterinsights-v1.13.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/contactcenterinsights/com_google_cloud_go_contactcenterinsights-v1.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/contactcenterinsights/com_google_cloud_go_contactcenterinsights-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_container",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/container",
        sha256 = "840b125be4780c31ba03ea6abcfc55729eb4927f592d21ae0d314d981bc67057",
        strip_prefix = "cloud.google.com/go/container@v1.31.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/container/com_google_cloud_go_container-v1.31.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/container/com_google_cloud_go_container-v1.31.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/container/com_google_cloud_go_container-v1.31.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/container/com_google_cloud_go_container-v1.31.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_containeranalysis",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/containeranalysis",
        sha256 = "9aa4f7e5cbfa7317beed95fc032d5f9039c4c2881e49a942a4abcfc847150c7a",
        strip_prefix = "cloud.google.com/go/containeranalysis@v0.11.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/containeranalysis/com_google_cloud_go_containeranalysis-v0.11.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/containeranalysis/com_google_cloud_go_containeranalysis-v0.11.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/containeranalysis/com_google_cloud_go_containeranalysis-v0.11.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/containeranalysis/com_google_cloud_go_containeranalysis-v0.11.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datacatalog",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datacatalog",
        sha256 = "158ea05506494f5a3b82d23c235c15542bfe64837310ce2b8cb4b7fe42536b40",
        strip_prefix = "cloud.google.com/go/datacatalog@v1.19.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datacatalog/com_google_cloud_go_datacatalog-v1.19.3.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datacatalog/com_google_cloud_go_datacatalog-v1.19.3.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datacatalog/com_google_cloud_go_datacatalog-v1.19.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datacatalog/com_google_cloud_go_datacatalog-v1.19.3.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataflow",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataflow",
        sha256 = "53a924bc78f46210856c26bd93e9170312391107c511669377104340d0636c3b",
        strip_prefix = "cloud.google.com/go/dataflow@v0.9.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataflow/com_google_cloud_go_dataflow-v0.9.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataflow/com_google_cloud_go_dataflow-v0.9.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataflow/com_google_cloud_go_dataflow-v0.9.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataflow/com_google_cloud_go_dataflow-v0.9.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataform",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataform",
        sha256 = "d70f87bac2c275cb315b4ce7e4cc202cb9ab0e66ac1055ea4ab16bb829b6e528",
        strip_prefix = "cloud.google.com/go/dataform@v0.9.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataform/com_google_cloud_go_dataform-v0.9.2.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataform/com_google_cloud_go_dataform-v0.9.2.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataform/com_google_cloud_go_dataform-v0.9.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataform/com_google_cloud_go_dataform-v0.9.2.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datafusion",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datafusion",
        sha256 = "e8a2869286204a3592a5ca17b9e08f0bd0c8cddc89d9a2145424492cf6117cd1",
        strip_prefix = "cloud.google.com/go/datafusion@v1.7.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datafusion/com_google_cloud_go_datafusion-v1.7.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datafusion/com_google_cloud_go_datafusion-v1.7.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datafusion/com_google_cloud_go_datafusion-v1.7.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datafusion/com_google_cloud_go_datafusion-v1.7.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datalabeling",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datalabeling",
        sha256 = "9d622cbd38c7c7fda283655efeb49e94afe893d26719c9083081f4541dc8fc07",
        strip_prefix = "cloud.google.com/go/datalabeling@v0.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datalabeling/com_google_cloud_go_datalabeling-v0.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datalabeling/com_google_cloud_go_datalabeling-v0.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datalabeling/com_google_cloud_go_datalabeling-v0.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datalabeling/com_google_cloud_go_datalabeling-v0.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataplex",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataplex",
        sha256 = "a1d23438c094389cc0b18c5a342459b699ff74b2c0b8ec81c83d0d3b019283d4",
        strip_prefix = "cloud.google.com/go/dataplex@v1.14.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataplex/com_google_cloud_go_dataplex-v1.14.2.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataplex/com_google_cloud_go_dataplex-v1.14.2.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataplex/com_google_cloud_go_dataplex-v1.14.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataplex/com_google_cloud_go_dataplex-v1.14.2.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataproc_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataproc/v2",
        sha256 = "a06ef35391acd2074b1454c6c90b1db967872679426d353add376a23650abff4",
        strip_prefix = "cloud.google.com/go/dataproc/v2@v2.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataproc/v2/com_google_cloud_go_dataproc_v2-v2.4.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataproc/v2/com_google_cloud_go_dataproc_v2-v2.4.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataproc/v2/com_google_cloud_go_dataproc_v2-v2.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataproc/v2/com_google_cloud_go_dataproc_v2-v2.4.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataqna",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataqna",
        sha256 = "8dd6dfc408512a77bcd0e2a421128a0ff60563479dd149c273e129bf4a659513",
        strip_prefix = "cloud.google.com/go/dataqna@v0.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataqna/com_google_cloud_go_dataqna-v0.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataqna/com_google_cloud_go_dataqna-v0.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataqna/com_google_cloud_go_dataqna-v0.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataqna/com_google_cloud_go_dataqna-v0.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datastore",
        sha256 = "8b89b61b9655adcfb197079184d0438dc15fc12aa7c3ef72f61fa8ddbad22880",
        strip_prefix = "cloud.google.com/go/datastore@v1.15.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datastore/com_google_cloud_go_datastore-v1.15.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datastore/com_google_cloud_go_datastore-v1.15.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datastore/com_google_cloud_go_datastore-v1.15.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datastore/com_google_cloud_go_datastore-v1.15.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datastream",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datastream",
        sha256 = "d4e33da4b94b839b561119fab0927ed96848a0f2ab007d28d05b492fbf5ee89b",
        strip_prefix = "cloud.google.com/go/datastream@v1.10.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datastream/com_google_cloud_go_datastream-v1.10.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datastream/com_google_cloud_go_datastream-v1.10.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datastream/com_google_cloud_go_datastream-v1.10.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datastream/com_google_cloud_go_datastream-v1.10.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_deploy",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/deploy",
        sha256 = "255d773b063c5a25553fbf5b15dd80b82629a720c939e98253d3dcb47ba39bba",
        strip_prefix = "cloud.google.com/go/deploy@v1.17.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/deploy/com_google_cloud_go_deploy-v1.17.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/deploy/com_google_cloud_go_deploy-v1.17.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/deploy/com_google_cloud_go_deploy-v1.17.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/deploy/com_google_cloud_go_deploy-v1.17.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dialogflow",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dialogflow",
        sha256 = "0f5a512760a40552a701d6da6d4b5adf2ecdbc9e520c2943478290819ab377bd",
        strip_prefix = "cloud.google.com/go/dialogflow@v1.49.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dialogflow/com_google_cloud_go_dialogflow-v1.49.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dialogflow/com_google_cloud_go_dialogflow-v1.49.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dialogflow/com_google_cloud_go_dialogflow-v1.49.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dialogflow/com_google_cloud_go_dialogflow-v1.49.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dlp",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dlp",
        sha256 = "ce1b28549395dae09e1b35dc6111e05bff9d377914d1898a2f2da29fc819f2be",
        strip_prefix = "cloud.google.com/go/dlp@v1.11.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dlp/com_google_cloud_go_dlp-v1.11.2.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dlp/com_google_cloud_go_dlp-v1.11.2.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dlp/com_google_cloud_go_dlp-v1.11.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dlp/com_google_cloud_go_dlp-v1.11.2.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_documentai",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/documentai",
        sha256 = "a71869a7be5bed35de419b4648d78679c80d7b460907e87b3858490984e84f5e",
        strip_prefix = "cloud.google.com/go/documentai@v1.25.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/documentai/com_google_cloud_go_documentai-v1.25.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/documentai/com_google_cloud_go_documentai-v1.25.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/documentai/com_google_cloud_go_documentai-v1.25.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/documentai/com_google_cloud_go_documentai-v1.25.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_domains",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/domains",
        sha256 = "52be9698870dabb6b10fd8fad795b46476de29feb174d0bfc6f10c9fc6707f13",
        strip_prefix = "cloud.google.com/go/domains@v0.9.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/domains/com_google_cloud_go_domains-v0.9.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/domains/com_google_cloud_go_domains-v0.9.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/domains/com_google_cloud_go_domains-v0.9.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/domains/com_google_cloud_go_domains-v0.9.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_edgecontainer",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/edgecontainer",
        sha256 = "c5065dee8ac4386ae642e4f7ae7b182b4a06f99092d4d89c60487e1f947fdd03",
        strip_prefix = "cloud.google.com/go/edgecontainer@v1.1.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/edgecontainer/com_google_cloud_go_edgecontainer-v1.1.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/edgecontainer/com_google_cloud_go_edgecontainer-v1.1.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/edgecontainer/com_google_cloud_go_edgecontainer-v1.1.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/edgecontainer/com_google_cloud_go_edgecontainer-v1.1.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_errorreporting",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/errorreporting",
        sha256 = "7b6ee6ab85d13d042543e1f2eff7e4c73104ba76981a85a6aed7dc302cf20585",
        strip_prefix = "cloud.google.com/go/errorreporting@v0.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/errorreporting/com_google_cloud_go_errorreporting-v0.3.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/errorreporting/com_google_cloud_go_errorreporting-v0.3.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/errorreporting/com_google_cloud_go_errorreporting-v0.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/errorreporting/com_google_cloud_go_errorreporting-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_essentialcontacts",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/essentialcontacts",
        sha256 = "bc60afb97314e44c3f7e65cb2e0342e5c11ef3748839105ca8a551fafb9afcfd",
        strip_prefix = "cloud.google.com/go/essentialcontacts@v1.6.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/essentialcontacts/com_google_cloud_go_essentialcontacts-v1.6.6.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/essentialcontacts/com_google_cloud_go_essentialcontacts-v1.6.6.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/essentialcontacts/com_google_cloud_go_essentialcontacts-v1.6.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/essentialcontacts/com_google_cloud_go_essentialcontacts-v1.6.6.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_eventarc",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/eventarc",
        sha256 = "58cfd142c358fcef531f6290749d49bfeb90df2e0153109cc83cf95f75042272",
        strip_prefix = "cloud.google.com/go/eventarc@v1.13.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/eventarc/com_google_cloud_go_eventarc-v1.13.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/eventarc/com_google_cloud_go_eventarc-v1.13.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/eventarc/com_google_cloud_go_eventarc-v1.13.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/eventarc/com_google_cloud_go_eventarc-v1.13.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_filestore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/filestore",
        sha256 = "b1a9002fa292bc485ab496718e29bcf5ecccf60ace73138b104649a13ebf1e5a",
        strip_prefix = "cloud.google.com/go/filestore@v1.8.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/filestore/com_google_cloud_go_filestore-v1.8.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/filestore/com_google_cloud_go_filestore-v1.8.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/filestore/com_google_cloud_go_filestore-v1.8.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/filestore/com_google_cloud_go_filestore-v1.8.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/firestore",
        sha256 = "426e3589567d5b7bea9f7936863b4fe9fc7172029afc2b03cded5f69bcf3baf2",
        strip_prefix = "cloud.google.com/go/firestore@v1.14.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/firestore/com_google_cloud_go_firestore-v1.14.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/firestore/com_google_cloud_go_firestore-v1.14.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/firestore/com_google_cloud_go_firestore-v1.14.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/firestore/com_google_cloud_go_firestore-v1.14.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_functions",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/functions",
        sha256 = "6c5dd0e47056107770ea8c0a278803161fac4ac4bb4357aef5c40c6c8b5f5e44",
        strip_prefix = "cloud.google.com/go/functions@v1.16.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/functions/com_google_cloud_go_functions-v1.16.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/functions/com_google_cloud_go_functions-v1.16.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/functions/com_google_cloud_go_functions-v1.16.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/functions/com_google_cloud_go_functions-v1.16.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gkebackup",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkebackup",
        sha256 = "7617734c17dd1ef31b84691b910001187d48fa88858a1d6147a7f3f192c5283c",
        strip_prefix = "cloud.google.com/go/gkebackup@v1.3.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gkebackup/com_google_cloud_go_gkebackup-v1.3.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gkebackup/com_google_cloud_go_gkebackup-v1.3.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gkebackup/com_google_cloud_go_gkebackup-v1.3.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gkebackup/com_google_cloud_go_gkebackup-v1.3.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gkeconnect",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkeconnect",
        sha256 = "e2826d1bfb49f0958d9d39117e32e18f910fe85adad4e40a35956da8a84d9e53",
        strip_prefix = "cloud.google.com/go/gkeconnect@v0.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gkeconnect/com_google_cloud_go_gkeconnect-v0.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gkeconnect/com_google_cloud_go_gkeconnect-v0.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gkeconnect/com_google_cloud_go_gkeconnect-v0.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gkeconnect/com_google_cloud_go_gkeconnect-v0.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gkehub",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkehub",
        sha256 = "753d6f2b9a22a87bff6fabc8ce751b2c149368bffb430cd258d7630d67a5fc1b",
        strip_prefix = "cloud.google.com/go/gkehub@v0.14.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gkehub/com_google_cloud_go_gkehub-v0.14.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gkehub/com_google_cloud_go_gkehub-v0.14.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gkehub/com_google_cloud_go_gkehub-v0.14.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gkehub/com_google_cloud_go_gkehub-v0.14.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gkemulticloud",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkemulticloud",
        sha256 = "a33995596063889a8b166cad7bc6a327a12ec6cde1ba5c1b75cf4598469d7592",
        strip_prefix = "cloud.google.com/go/gkemulticloud@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gkemulticloud/com_google_cloud_go_gkemulticloud-v1.1.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gkemulticloud/com_google_cloud_go_gkemulticloud-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gkemulticloud/com_google_cloud_go_gkemulticloud-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gkemulticloud/com_google_cloud_go_gkemulticloud-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gsuiteaddons",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gsuiteaddons",
        sha256 = "b43bd8eb7d8781aea96e06527905845fe04c1715da6b8b41342232725ef3d871",
        strip_prefix = "cloud.google.com/go/gsuiteaddons@v1.6.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gsuiteaddons/com_google_cloud_go_gsuiteaddons-v1.6.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gsuiteaddons/com_google_cloud_go_gsuiteaddons-v1.6.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gsuiteaddons/com_google_cloud_go_gsuiteaddons-v1.6.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gsuiteaddons/com_google_cloud_go_gsuiteaddons-v1.6.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iam",
        sha256 = "2340ade8662748d6581ef29e470410e3bd8a48621805f135167bf55bc9b052f2",
        strip_prefix = "cloud.google.com/go/iam@v1.1.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/iam/com_google_cloud_go_iam-v1.1.6.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/iam/com_google_cloud_go_iam-v1.1.6.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/iam/com_google_cloud_go_iam-v1.1.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/iam/com_google_cloud_go_iam-v1.1.6.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_iap",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iap",
        sha256 = "923456340072c0cb9deffeb221c8bf2e67f3404cb652159238dca9b962cc7a82",
        strip_prefix = "cloud.google.com/go/iap@v1.9.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/iap/com_google_cloud_go_iap-v1.9.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/iap/com_google_cloud_go_iap-v1.9.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/iap/com_google_cloud_go_iap-v1.9.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/iap/com_google_cloud_go_iap-v1.9.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_ids",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/ids",
        sha256 = "2ee442696e20e1fe380b48f45d458fcd38ae0a187bb66264a1b104d104024cce",
        strip_prefix = "cloud.google.com/go/ids@v1.4.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/ids/com_google_cloud_go_ids-v1.4.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/ids/com_google_cloud_go_ids-v1.4.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/ids/com_google_cloud_go_ids-v1.4.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/ids/com_google_cloud_go_ids-v1.4.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_iot",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iot",
        sha256 = "7727fc21d7400157c0753d1fb90d85cbd101a3db6ae665d540b52d74bc2b3a15",
        strip_prefix = "cloud.google.com/go/iot@v1.7.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/iot/com_google_cloud_go_iot-v1.7.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/iot/com_google_cloud_go_iot-v1.7.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/iot/com_google_cloud_go_iot-v1.7.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/iot/com_google_cloud_go_iot-v1.7.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/kms",
        sha256 = "efe728dbf66dddb1f6684fe3063c97c160bc5c3b84a686a5fd5614d74d23733f",
        strip_prefix = "cloud.google.com/go/kms@v1.15.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/kms/com_google_cloud_go_kms-v1.15.7.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/kms/com_google_cloud_go_kms-v1.15.7.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/kms/com_google_cloud_go_kms-v1.15.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/kms/com_google_cloud_go_kms-v1.15.7.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_language",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/language",
        sha256 = "d7def4827c112b93ae2da079244155dc631871b0c460e3c309d8e2c23cea6fd5",
        strip_prefix = "cloud.google.com/go/language@v1.12.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/language/com_google_cloud_go_language-v1.12.3.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/language/com_google_cloud_go_language-v1.12.3.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/language/com_google_cloud_go_language-v1.12.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/language/com_google_cloud_go_language-v1.12.3.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_lifesciences",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/lifesciences",
        sha256 = "f0a13c8842d12f7766eb5ae855051db836b46fbcb7ff799b7ab4e29e1880e484",
        strip_prefix = "cloud.google.com/go/lifesciences@v0.9.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/lifesciences/com_google_cloud_go_lifesciences-v0.9.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/lifesciences/com_google_cloud_go_lifesciences-v0.9.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/lifesciences/com_google_cloud_go_lifesciences-v0.9.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/lifesciences/com_google_cloud_go_lifesciences-v0.9.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/logging",
        sha256 = "abc0c1703a42cbbd58108e003596bb3c803847c28a9df43354dd9f8a1a55b4b8",
        strip_prefix = "cloud.google.com/go/logging@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/logging/com_google_cloud_go_logging-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/logging/com_google_cloud_go_logging-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/logging/com_google_cloud_go_logging-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/logging/com_google_cloud_go_logging-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_longrunning",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/longrunning",
        sha256 = "d7c32818f6ca09c7d5c8dfc423b2e37d8b45a0d257e5483b12eceef40f2ad29e",
        strip_prefix = "cloud.google.com/go/longrunning@v0.5.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/longrunning/com_google_cloud_go_longrunning-v0.5.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/longrunning/com_google_cloud_go_longrunning-v0.5.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/longrunning/com_google_cloud_go_longrunning-v0.5.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/longrunning/com_google_cloud_go_longrunning-v0.5.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_managedidentities",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/managedidentities",
        sha256 = "f7c3629ff5dd4f8303e2a4e4460323025435bc1fb9cbfce5795380fcbcc71863",
        strip_prefix = "cloud.google.com/go/managedidentities@v1.6.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/managedidentities/com_google_cloud_go_managedidentities-v1.6.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/managedidentities/com_google_cloud_go_managedidentities-v1.6.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/managedidentities/com_google_cloud_go_managedidentities-v1.6.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/managedidentities/com_google_cloud_go_managedidentities-v1.6.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_maps",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/maps",
        sha256 = "f8f673a9a144e985a661a16ab9d1000b2cac9e3f5f75b2678e012c0d599389f6",
        strip_prefix = "cloud.google.com/go/maps@v1.6.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/maps/com_google_cloud_go_maps-v1.6.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/maps/com_google_cloud_go_maps-v1.6.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/maps/com_google_cloud_go_maps-v1.6.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/maps/com_google_cloud_go_maps-v1.6.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_mediatranslation",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/mediatranslation",
        sha256 = "ad4d59c5d1fd43153f62c7955a8b079fc50395c34026df1215c04722234b2d4c",
        strip_prefix = "cloud.google.com/go/mediatranslation@v0.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/mediatranslation/com_google_cloud_go_mediatranslation-v0.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/mediatranslation/com_google_cloud_go_mediatranslation-v0.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/mediatranslation/com_google_cloud_go_mediatranslation-v0.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/mediatranslation/com_google_cloud_go_mediatranslation-v0.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_memcache",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/memcache",
        sha256 = "3d21ca1f735630b714ede58fa46833157d5c96d0a9ab1b47572a13d1fcc62c65",
        strip_prefix = "cloud.google.com/go/memcache@v1.10.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/memcache/com_google_cloud_go_memcache-v1.10.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/memcache/com_google_cloud_go_memcache-v1.10.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/memcache/com_google_cloud_go_memcache-v1.10.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/memcache/com_google_cloud_go_memcache-v1.10.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_metastore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/metastore",
        sha256 = "3be4c42d5698194020364a0d7e2c9ee4b84140d9206ffdd3c46923f1e6e8405a",
        strip_prefix = "cloud.google.com/go/metastore@v1.13.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/metastore/com_google_cloud_go_metastore-v1.13.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/metastore/com_google_cloud_go_metastore-v1.13.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/metastore/com_google_cloud_go_metastore-v1.13.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/metastore/com_google_cloud_go_metastore-v1.13.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_monitoring",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/monitoring",
        sha256 = "c16947177048b8b5a0eb0736979cf067deec7aeea95405ac698ebf49da5204d6",
        strip_prefix = "cloud.google.com/go/monitoring@v1.18.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/monitoring/com_google_cloud_go_monitoring-v1.18.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/monitoring/com_google_cloud_go_monitoring-v1.18.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/monitoring/com_google_cloud_go_monitoring-v1.18.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/monitoring/com_google_cloud_go_monitoring-v1.18.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_networkconnectivity",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networkconnectivity",
        sha256 = "16094a054c49752b68585d5500370fd9d7f742470c0c26aefb8040b3d20023a1",
        strip_prefix = "cloud.google.com/go/networkconnectivity@v1.14.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/networkconnectivity/com_google_cloud_go_networkconnectivity-v1.14.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/networkconnectivity/com_google_cloud_go_networkconnectivity-v1.14.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/networkconnectivity/com_google_cloud_go_networkconnectivity-v1.14.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/networkconnectivity/com_google_cloud_go_networkconnectivity-v1.14.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_networkmanagement",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networkmanagement",
        sha256 = "08e6997d0b3ef0f6ae7f9fedf3dbf0dc4df0ca37ac48c0620def842a7b9b0ac4",
        strip_prefix = "cloud.google.com/go/networkmanagement@v1.9.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/networkmanagement/com_google_cloud_go_networkmanagement-v1.9.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/networkmanagement/com_google_cloud_go_networkmanagement-v1.9.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/networkmanagement/com_google_cloud_go_networkmanagement-v1.9.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/networkmanagement/com_google_cloud_go_networkmanagement-v1.9.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_networksecurity",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networksecurity",
        sha256 = "9fe395a99c14c2900363e97abd35140513d0501477dc8ff925d083093ee61c3c",
        strip_prefix = "cloud.google.com/go/networksecurity@v0.9.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/networksecurity/com_google_cloud_go_networksecurity-v0.9.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/networksecurity/com_google_cloud_go_networksecurity-v0.9.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/networksecurity/com_google_cloud_go_networksecurity-v0.9.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/networksecurity/com_google_cloud_go_networksecurity-v0.9.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_notebooks",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/notebooks",
        sha256 = "eb348f5082ae07532f6340963fd526920323909948e3d2a478a1c0ed60532a05",
        strip_prefix = "cloud.google.com/go/notebooks@v1.11.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/notebooks/com_google_cloud_go_notebooks-v1.11.3.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/notebooks/com_google_cloud_go_notebooks-v1.11.3.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/notebooks/com_google_cloud_go_notebooks-v1.11.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/notebooks/com_google_cloud_go_notebooks-v1.11.3.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_optimization",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/optimization",
        sha256 = "23cb4effc3aa771483f2e99eee5eed014461a4f7931be408c87b1f1cfad1304c",
        strip_prefix = "cloud.google.com/go/optimization@v1.6.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/optimization/com_google_cloud_go_optimization-v1.6.3.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/optimization/com_google_cloud_go_optimization-v1.6.3.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/optimization/com_google_cloud_go_optimization-v1.6.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/optimization/com_google_cloud_go_optimization-v1.6.3.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_orchestration",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/orchestration",
        sha256 = "3581411e89ce4af44eeb09c6c7a2fcbbeb37e8b00c3d63ecdbafcc6a1ba48557",
        strip_prefix = "cloud.google.com/go/orchestration@v1.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/orchestration/com_google_cloud_go_orchestration-v1.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/orchestration/com_google_cloud_go_orchestration-v1.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/orchestration/com_google_cloud_go_orchestration-v1.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/orchestration/com_google_cloud_go_orchestration-v1.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_orgpolicy",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/orgpolicy",
        sha256 = "a0ea6ba027808aa1c7d90b066f47c1df38e22ac8a953ad85b8efeaa8c79a22e6",
        strip_prefix = "cloud.google.com/go/orgpolicy@v1.12.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/orgpolicy/com_google_cloud_go_orgpolicy-v1.12.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/orgpolicy/com_google_cloud_go_orgpolicy-v1.12.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/orgpolicy/com_google_cloud_go_orgpolicy-v1.12.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/orgpolicy/com_google_cloud_go_orgpolicy-v1.12.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_osconfig",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/osconfig",
        sha256 = "02bf95f2522727ab882a9028c734ea6fd9cfe962846923b17b1579e9da7404a3",
        strip_prefix = "cloud.google.com/go/osconfig@v1.12.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/osconfig/com_google_cloud_go_osconfig-v1.12.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/osconfig/com_google_cloud_go_osconfig-v1.12.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/osconfig/com_google_cloud_go_osconfig-v1.12.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/osconfig/com_google_cloud_go_osconfig-v1.12.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_oslogin",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/oslogin",
        sha256 = "ca28cd9210922f2e9abd9aa283eea775060ef02f4167e3cc56bb8d92aa453c57",
        strip_prefix = "cloud.google.com/go/oslogin@v1.13.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/oslogin/com_google_cloud_go_oslogin-v1.13.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/oslogin/com_google_cloud_go_oslogin-v1.13.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/oslogin/com_google_cloud_go_oslogin-v1.13.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/oslogin/com_google_cloud_go_oslogin-v1.13.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_phishingprotection",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/phishingprotection",
        sha256 = "98951639118b05caf30d9320c39c285f0cbe224c6bde63fb39acf42c4f9bbf86",
        strip_prefix = "cloud.google.com/go/phishingprotection@v0.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/phishingprotection/com_google_cloud_go_phishingprotection-v0.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/phishingprotection/com_google_cloud_go_phishingprotection-v0.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/phishingprotection/com_google_cloud_go_phishingprotection-v0.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/phishingprotection/com_google_cloud_go_phishingprotection-v0.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_policytroubleshooter",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/policytroubleshooter",
        sha256 = "96585f3dd465551c1ba5800b2c6a1f78dbb12e47219a96ece01e0113b3e56718",
        strip_prefix = "cloud.google.com/go/policytroubleshooter@v1.10.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/policytroubleshooter/com_google_cloud_go_policytroubleshooter-v1.10.3.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/policytroubleshooter/com_google_cloud_go_policytroubleshooter-v1.10.3.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/policytroubleshooter/com_google_cloud_go_policytroubleshooter-v1.10.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/policytroubleshooter/com_google_cloud_go_policytroubleshooter-v1.10.3.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_privatecatalog",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/privatecatalog",
        sha256 = "25b7b30d8d7be00bad226d82dc456fe19476b7323510454de227a8665fd19041",
        strip_prefix = "cloud.google.com/go/privatecatalog@v0.9.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/privatecatalog/com_google_cloud_go_privatecatalog-v0.9.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/privatecatalog/com_google_cloud_go_privatecatalog-v0.9.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/privatecatalog/com_google_cloud_go_privatecatalog-v0.9.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/privatecatalog/com_google_cloud_go_privatecatalog-v0.9.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/pubsub",
        sha256 = "9feff102f2a26c5e9755b391927f24808b783363d10f988c8755c373b631efaf",
        strip_prefix = "cloud.google.com/go/pubsub@v1.36.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/pubsub/com_google_cloud_go_pubsub-v1.36.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/pubsub/com_google_cloud_go_pubsub-v1.36.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/pubsub/com_google_cloud_go_pubsub-v1.36.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/pubsub/com_google_cloud_go_pubsub-v1.36.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_pubsublite",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/pubsublite",
        sha256 = "41933a60c5e0995025320fe1c155b31d636178e60838b04aca9eab0c8c9f3227",
        strip_prefix = "cloud.google.com/go/pubsublite@v1.8.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/pubsublite/com_google_cloud_go_pubsublite-v1.8.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/pubsublite/com_google_cloud_go_pubsublite-v1.8.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/pubsublite/com_google_cloud_go_pubsublite-v1.8.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/pubsublite/com_google_cloud_go_pubsublite-v1.8.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_recaptchaenterprise_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recaptchaenterprise/v2",
        sha256 = "e83e1e652020604e58b36821cda9c9ab7fc1487c9376542a474ecbfd7f78d2db",
        strip_prefix = "cloud.google.com/go/recaptchaenterprise/v2@v2.9.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/recaptchaenterprise/v2/com_google_cloud_go_recaptchaenterprise_v2-v2.9.2.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/recaptchaenterprise/v2/com_google_cloud_go_recaptchaenterprise_v2-v2.9.2.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/recaptchaenterprise/v2/com_google_cloud_go_recaptchaenterprise_v2-v2.9.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/recaptchaenterprise/v2/com_google_cloud_go_recaptchaenterprise_v2-v2.9.2.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_recommendationengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recommendationengine",
        sha256 = "7b3a14bf4dda969087b94a195a3341d8a340b19b4200ae69745c1b72f25208eb",
        strip_prefix = "cloud.google.com/go/recommendationengine@v0.8.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/recommendationengine/com_google_cloud_go_recommendationengine-v0.8.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/recommendationengine/com_google_cloud_go_recommendationengine-v0.8.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/recommendationengine/com_google_cloud_go_recommendationengine-v0.8.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/recommendationengine/com_google_cloud_go_recommendationengine-v0.8.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_recommender",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recommender",
        sha256 = "b8e31a6c511bd19d5cc6d07029a1d93a76199c8536539b3850048c479a4b2d59",
        strip_prefix = "cloud.google.com/go/recommender@v1.12.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/recommender/com_google_cloud_go_recommender-v1.12.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/recommender/com_google_cloud_go_recommender-v1.12.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/recommender/com_google_cloud_go_recommender-v1.12.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/recommender/com_google_cloud_go_recommender-v1.12.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_redis",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/redis",
        sha256 = "2ad92f1fe9d4b8e3e2342e45dd868843e34c6e6020447045efa8f4cdf4b14bc9",
        strip_prefix = "cloud.google.com/go/redis@v1.14.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/redis/com_google_cloud_go_redis-v1.14.2.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/redis/com_google_cloud_go_redis-v1.14.2.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/redis/com_google_cloud_go_redis-v1.14.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/redis/com_google_cloud_go_redis-v1.14.2.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_resourcemanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/resourcemanager",
        sha256 = "8b78a11c34c7d82a72e346475e26f980f3b82419bfd74c94138b8c69ff50b325",
        strip_prefix = "cloud.google.com/go/resourcemanager@v1.9.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/resourcemanager/com_google_cloud_go_resourcemanager-v1.9.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/resourcemanager/com_google_cloud_go_resourcemanager-v1.9.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/resourcemanager/com_google_cloud_go_resourcemanager-v1.9.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/resourcemanager/com_google_cloud_go_resourcemanager-v1.9.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_resourcesettings",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/resourcesettings",
        sha256 = "73e8418040ec80303675503371c53980c5d840dd5b77feee60f128a9070bf794",
        strip_prefix = "cloud.google.com/go/resourcesettings@v1.6.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/resourcesettings/com_google_cloud_go_resourcesettings-v1.6.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/resourcesettings/com_google_cloud_go_resourcesettings-v1.6.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/resourcesettings/com_google_cloud_go_resourcesettings-v1.6.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/resourcesettings/com_google_cloud_go_resourcesettings-v1.6.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_retail",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/retail",
        sha256 = "a1cc280566f55e027eb7bc746f7c5a37e7a0ec5659adbde34959275fc9a45b56",
        strip_prefix = "cloud.google.com/go/retail@v1.16.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/retail/com_google_cloud_go_retail-v1.16.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/retail/com_google_cloud_go_retail-v1.16.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/retail/com_google_cloud_go_retail-v1.16.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/retail/com_google_cloud_go_retail-v1.16.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_run",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/run",
        sha256 = "932edcab991d8ed35085a57444cc4d27585ee98ca6c927ca93a333f7f119725d",
        strip_prefix = "cloud.google.com/go/run@v1.3.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/run/com_google_cloud_go_run-v1.3.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/run/com_google_cloud_go_run-v1.3.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/run/com_google_cloud_go_run-v1.3.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/run/com_google_cloud_go_run-v1.3.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_scheduler",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/scheduler",
        sha256 = "77ddd0298d34b30fa48df896899a1f928fe01b22220d4ca64fffd0a1d56ee50c",
        strip_prefix = "cloud.google.com/go/scheduler@v1.10.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/scheduler/com_google_cloud_go_scheduler-v1.10.6.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/scheduler/com_google_cloud_go_scheduler-v1.10.6.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/scheduler/com_google_cloud_go_scheduler-v1.10.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/scheduler/com_google_cloud_go_scheduler-v1.10.6.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/secretmanager",
        sha256 = "e3f0000863cc9944a97ebd4004b6cde6fa2484233cd12e1741506428c8265ca3",
        strip_prefix = "cloud.google.com/go/secretmanager@v1.11.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/secretmanager/com_google_cloud_go_secretmanager-v1.11.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/secretmanager/com_google_cloud_go_secretmanager-v1.11.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/secretmanager/com_google_cloud_go_secretmanager-v1.11.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/secretmanager/com_google_cloud_go_secretmanager-v1.11.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_security",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/security",
        sha256 = "f4dd23e113cad47462715d654c95de55c1c890b37cca8c79b47bb5a7c0ec9417",
        strip_prefix = "cloud.google.com/go/security@v1.15.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/security/com_google_cloud_go_security-v1.15.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/security/com_google_cloud_go_security-v1.15.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/security/com_google_cloud_go_security-v1.15.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/security/com_google_cloud_go_security-v1.15.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_securitycenter",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/securitycenter",
        sha256 = "2d465bd4173e7c5f7e2b395797d0053175d2501cd1c282d801f8f11cb29c03d4",
        strip_prefix = "cloud.google.com/go/securitycenter@v1.24.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/securitycenter/com_google_cloud_go_securitycenter-v1.24.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/securitycenter/com_google_cloud_go_securitycenter-v1.24.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/securitycenter/com_google_cloud_go_securitycenter-v1.24.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/securitycenter/com_google_cloud_go_securitycenter-v1.24.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_servicedirectory",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/servicedirectory",
        sha256 = "ab4aeaa7d371f1458dc3b295c9ecf712a35b4d2d853b4d4fb9192454e70815fb",
        strip_prefix = "cloud.google.com/go/servicedirectory@v1.11.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/servicedirectory/com_google_cloud_go_servicedirectory-v1.11.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/servicedirectory/com_google_cloud_go_servicedirectory-v1.11.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/servicedirectory/com_google_cloud_go_servicedirectory-v1.11.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/servicedirectory/com_google_cloud_go_servicedirectory-v1.11.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_shell",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/shell",
        sha256 = "28fea75e78add4a619d4ac65fdfcef1577599c20310e82ab884c686ace14021d",
        strip_prefix = "cloud.google.com/go/shell@v1.7.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/shell/com_google_cloud_go_shell-v1.7.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/shell/com_google_cloud_go_shell-v1.7.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/shell/com_google_cloud_go_shell-v1.7.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/shell/com_google_cloud_go_shell-v1.7.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_spanner",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/spanner",
        sha256 = "2f120e8a85e8a06dc6183447903ff7624b1e5d22ce4fd9026968d78b32335462",
        strip_prefix = "cloud.google.com/go/spanner@v1.56.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/spanner/com_google_cloud_go_spanner-v1.56.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/spanner/com_google_cloud_go_spanner-v1.56.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/spanner/com_google_cloud_go_spanner-v1.56.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/spanner/com_google_cloud_go_spanner-v1.56.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_speech",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/speech",
        sha256 = "2f1a1127cf13f85d2975f91f4296f43d59fc14273177aade2909bc94a4bbf358",
        strip_prefix = "cloud.google.com/go/speech@v1.21.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/speech/com_google_cloud_go_speech-v1.21.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/speech/com_google_cloud_go_speech-v1.21.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/speech/com_google_cloud_go_speech-v1.21.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/speech/com_google_cloud_go_speech-v1.21.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/storage",
        sha256 = "272530d5e205a825b33546e9ac349a66346267a0bc06f006d617d353cdec5525",
        strip_prefix = "cloud.google.com/go/storage@v1.36.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/storage/com_google_cloud_go_storage-v1.36.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/storage/com_google_cloud_go_storage-v1.36.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/storage/com_google_cloud_go_storage-v1.36.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/storage/com_google_cloud_go_storage-v1.36.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_storagetransfer",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/storagetransfer",
        sha256 = "4a9f5d532a1a8c52f16428e137a4c0fca6c23f2583a8526f83f4e033a9edf9a1",
        strip_prefix = "cloud.google.com/go/storagetransfer@v1.10.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/storagetransfer/com_google_cloud_go_storagetransfer-v1.10.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/storagetransfer/com_google_cloud_go_storagetransfer-v1.10.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/storagetransfer/com_google_cloud_go_storagetransfer-v1.10.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/storagetransfer/com_google_cloud_go_storagetransfer-v1.10.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_talent",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/talent",
        sha256 = "e07557cef01010fff6183a646bdf3fbad238efd6e111f614302edc74f60de896",
        strip_prefix = "cloud.google.com/go/talent@v1.6.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/talent/com_google_cloud_go_talent-v1.6.6.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/talent/com_google_cloud_go_talent-v1.6.6.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/talent/com_google_cloud_go_talent-v1.6.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/talent/com_google_cloud_go_talent-v1.6.6.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_texttospeech",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/texttospeech",
        sha256 = "c136104322364aedd222839505fdca0142d3cc1d14d9a50a40ee0be2d9966fc7",
        strip_prefix = "cloud.google.com/go/texttospeech@v1.7.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/texttospeech/com_google_cloud_go_texttospeech-v1.7.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/texttospeech/com_google_cloud_go_texttospeech-v1.7.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/texttospeech/com_google_cloud_go_texttospeech-v1.7.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/texttospeech/com_google_cloud_go_texttospeech-v1.7.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_tpu",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/tpu",
        sha256 = "a5e0671eec0aca712a9dcc697e6b6c5bc89d4897aca092f2d5a2531152bcdf06",
        strip_prefix = "cloud.google.com/go/tpu@v1.6.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/tpu/com_google_cloud_go_tpu-v1.6.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/tpu/com_google_cloud_go_tpu-v1.6.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/tpu/com_google_cloud_go_tpu-v1.6.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/tpu/com_google_cloud_go_tpu-v1.6.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_trace",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/trace",
        sha256 = "74c62f0ced3cae41b2b0a33036d0f0dfc005e4a3c598b9f977f832095a477499",
        strip_prefix = "cloud.google.com/go/trace@v1.10.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/trace/com_google_cloud_go_trace-v1.10.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/trace/com_google_cloud_go_trace-v1.10.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/trace/com_google_cloud_go_trace-v1.10.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/trace/com_google_cloud_go_trace-v1.10.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_translate",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/translate",
        sha256 = "400320ff3f535f32ab8a4b7f71283c3f7819eb9ac2c7917453e62554eee65a3f",
        strip_prefix = "cloud.google.com/go/translate@v1.10.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/translate/com_google_cloud_go_translate-v1.10.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/translate/com_google_cloud_go_translate-v1.10.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/translate/com_google_cloud_go_translate-v1.10.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/translate/com_google_cloud_go_translate-v1.10.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_video",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/video",
        sha256 = "8ad94a57f03f2063d8d13fdbecb7dcd5e0f477539955de36906ea0bd14f4a76f",
        strip_prefix = "cloud.google.com/go/video@v1.20.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/video/com_google_cloud_go_video-v1.20.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/video/com_google_cloud_go_video-v1.20.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/video/com_google_cloud_go_video-v1.20.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/video/com_google_cloud_go_video-v1.20.4.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_videointelligence",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/videointelligence",
        sha256 = "f8b6aa7f16bf09f1b581e9689b83ab3b3310397c38f48eed42c212106df5c0fd",
        strip_prefix = "cloud.google.com/go/videointelligence@v1.11.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/videointelligence/com_google_cloud_go_videointelligence-v1.11.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/videointelligence/com_google_cloud_go_videointelligence-v1.11.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/videointelligence/com_google_cloud_go_videointelligence-v1.11.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/videointelligence/com_google_cloud_go_videointelligence-v1.11.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_vision_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vision/v2",
        sha256 = "c76bd66ad2b51b7e0893605e58439003d29390398596114df6a2dab34b39ebda",
        strip_prefix = "cloud.google.com/go/vision/v2@v2.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/vision/v2/com_google_cloud_go_vision_v2-v2.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/vision/v2/com_google_cloud_go_vision_v2-v2.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/vision/v2/com_google_cloud_go_vision_v2-v2.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/vision/v2/com_google_cloud_go_vision_v2-v2.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_vmmigration",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vmmigration",
        sha256 = "4488c36b2324ef7a3c6aee1075bb13767a43ea4de12509d593a5ae168fa71513",
        strip_prefix = "cloud.google.com/go/vmmigration@v1.7.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/vmmigration/com_google_cloud_go_vmmigration-v1.7.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/vmmigration/com_google_cloud_go_vmmigration-v1.7.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/vmmigration/com_google_cloud_go_vmmigration-v1.7.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/vmmigration/com_google_cloud_go_vmmigration-v1.7.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_vmwareengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vmwareengine",
        sha256 = "6766d871cf5cca252b3d98e138e8527374cefbca747a1530062cfebe31f3ae8e",
        strip_prefix = "cloud.google.com/go/vmwareengine@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/vmwareengine/com_google_cloud_go_vmwareengine-v1.1.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/vmwareengine/com_google_cloud_go_vmwareengine-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/vmwareengine/com_google_cloud_go_vmwareengine-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/vmwareengine/com_google_cloud_go_vmwareengine-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_vpcaccess",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vpcaccess",
        sha256 = "d1aae1f25f3efe5e4f08e4f0c485d2fa839cf9f221ce87ddc815910c8e68c7db",
        strip_prefix = "cloud.google.com/go/vpcaccess@v1.7.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/vpcaccess/com_google_cloud_go_vpcaccess-v1.7.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/vpcaccess/com_google_cloud_go_vpcaccess-v1.7.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/vpcaccess/com_google_cloud_go_vpcaccess-v1.7.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/vpcaccess/com_google_cloud_go_vpcaccess-v1.7.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_webrisk",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/webrisk",
        sha256 = "1fc8a54fc71a78c9b34bca71c8e464831c63f8b745ee729305b94a69a9c94579",
        strip_prefix = "cloud.google.com/go/webrisk@v1.9.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/webrisk/com_google_cloud_go_webrisk-v1.9.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/webrisk/com_google_cloud_go_webrisk-v1.9.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/webrisk/com_google_cloud_go_webrisk-v1.9.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/webrisk/com_google_cloud_go_webrisk-v1.9.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_websecurityscanner",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/websecurityscanner",
        sha256 = "40e8fabb14645bf3c5dd8e31791ae4afe55b5c7245d460ff7cd8d6f1d169ea2f",
        strip_prefix = "cloud.google.com/go/websecurityscanner@v1.6.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/websecurityscanner/com_google_cloud_go_websecurityscanner-v1.6.5.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/websecurityscanner/com_google_cloud_go_websecurityscanner-v1.6.5.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/websecurityscanner/com_google_cloud_go_websecurityscanner-v1.6.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/websecurityscanner/com_google_cloud_go_websecurityscanner-v1.6.5.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_workflows",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/workflows",
        sha256 = "624d1d4936eebf8b2ab6e4435002b488a96b6c1b920bfd3466b2b052ff3e4d12",
        strip_prefix = "cloud.google.com/go/workflows@v1.12.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/workflows/com_google_cloud_go_workflows-v1.12.4.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/workflows/com_google_cloud_go_workflows-v1.12.4.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/workflows/com_google_cloud_go_workflows-v1.12.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/workflows/com_google_cloud_go_workflows-v1.12.4.zip",
        ],
    )
    go_repository(
        name = "com_shuralyov_dmitri_gpu_mtl",
        build_file_proto_mode = "disable_global",
        importpath = "dmitri.shuralyov.com/gpu/mtl",
        sha256 = "ca5330901fcda83d09553ac362576d196c531157bc9c502e76b237cca262b400",
        strip_prefix = "dmitri.shuralyov.com/gpu/mtl@v0.0.0-20190408044501-666a987793e9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/dmitri.shuralyov.com/gpu/mtl/com_shuralyov_dmitri_gpu_mtl-v0.0.0-20190408044501-666a987793e9.zip",
            "http://ats.apps.svc/gomod/dmitri.shuralyov.com/gpu/mtl/com_shuralyov_dmitri_gpu_mtl-v0.0.0-20190408044501-666a987793e9.zip",
            "https://cache.hawkingrei.com/gomod/dmitri.shuralyov.com/gpu/mtl/com_shuralyov_dmitri_gpu_mtl-v0.0.0-20190408044501-666a987793e9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/dmitri.shuralyov.com/gpu/mtl/com_shuralyov_dmitri_gpu_mtl-v0.0.0-20190408044501-666a987793e9.zip",
        ],
    )
    go_repository(
        name = "com_sourcegraph_sourcegraph_appdash",
        build_file_proto_mode = "disable_global",
        importpath = "sourcegraph.com/sourcegraph/appdash",
        sha256 = "c46b442fa40d2af48e08064f4c16ae3712953a9988cd0f7588fcf5e4fc7a2fed",
        strip_prefix = "github.com/sourcegraph/appdash@v0.0.0-20190731080439-ebfcffb1b5c0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sourcegraph/appdash/com_github_sourcegraph_appdash-v0.0.0-20190731080439-ebfcffb1b5c0.zip",
            "http://ats.apps.svc/gomod/github.com/sourcegraph/appdash/com_github_sourcegraph_appdash-v0.0.0-20190731080439-ebfcffb1b5c0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sourcegraph/appdash/com_github_sourcegraph_appdash-v0.0.0-20190731080439-ebfcffb1b5c0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sourcegraph/appdash/com_github_sourcegraph_appdash-v0.0.0-20190731080439-ebfcffb1b5c0.zip",
        ],
    )
    go_repository(
        name = "com_sourcegraph_sourcegraph_appdash_data",
        build_file_proto_mode = "disable_global",
        importpath = "sourcegraph.com/sourcegraph/appdash-data",
        sha256 = "59b71fa8cdb0fe2b1c02739ccf2daeaf28f2e22c4b178cdc8e1b902ad1022bc0",
        strip_prefix = "github.com/sourcegraph/appdash-data@v0.0.0-20151005221446-73f23eafcf67",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sourcegraph/appdash-data/com_github_sourcegraph_appdash_data-v0.0.0-20151005221446-73f23eafcf67.zip",
            "http://ats.apps.svc/gomod/github.com/sourcegraph/appdash-data/com_github_sourcegraph_appdash_data-v0.0.0-20151005221446-73f23eafcf67.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sourcegraph/appdash-data/com_github_sourcegraph_appdash_data-v0.0.0-20151005221446-73f23eafcf67.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sourcegraph/appdash-data/com_github_sourcegraph_appdash_data-v0.0.0-20151005221446-73f23eafcf67.zip",
        ],
    )
    go_repository(
        name = "com_stathat_c_consistent",
        build_file_proto_mode = "disable_global",
        importpath = "stathat.com/c/consistent",
        sha256 = "ceb0b3e648f223a897ac3bdd74bd1a0b98c770e9230c3c6ee30838c1d06f6b51",
        strip_prefix = "stathat.com/c/consistent@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/stathat.com/c/consistent/com_stathat_c_consistent-v1.0.0.zip",
            "http://ats.apps.svc/gomod/stathat.com/c/consistent/com_stathat_c_consistent-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/stathat.com/c/consistent/com_stathat_c_consistent-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/stathat.com/c/consistent/com_stathat_c_consistent-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_check_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/check.v1",
        sha256 = "f555684e5c5dacc2850dddb345fef1b8f93f546b72685589789da6d2b062710e",
        strip_prefix = "gopkg.in/check.v1@v1.0.0-20201130134442-10cb98267c6c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/check.v1/in_gopkg_check_v1-v1.0.0-20201130134442-10cb98267c6c.zip",
            "http://ats.apps.svc/gomod/gopkg.in/check.v1/in_gopkg_check_v1-v1.0.0-20201130134442-10cb98267c6c.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/check.v1/in_gopkg_check_v1-v1.0.0-20201130134442-10cb98267c6c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/check.v1/in_gopkg_check_v1-v1.0.0-20201130134442-10cb98267c6c.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_errgo_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/errgo.v2",
        sha256 = "6b8954819a20ec52982a206fd3eb94629ff53c5790aa77534e6d8daf7de01bee",
        strip_prefix = "gopkg.in/errgo.v2@v2.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/errgo.v2/in_gopkg_errgo_v2-v2.1.0.zip",
            "http://ats.apps.svc/gomod/gopkg.in/errgo.v2/in_gopkg_errgo_v2-v2.1.0.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/errgo.v2/in_gopkg_errgo_v2-v2.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/errgo.v2/in_gopkg_errgo_v2-v2.1.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_fsnotify_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/fsnotify.v1",
        sha256 = "ce003d540f42b3c0a3dec385deb387b255b536b25ea4438baa65b89458b28f75",
        strip_prefix = "gopkg.in/fsnotify.v1@v1.4.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/fsnotify.v1/in_gopkg_fsnotify_v1-v1.4.7.zip",
            "http://ats.apps.svc/gomod/gopkg.in/fsnotify.v1/in_gopkg_fsnotify_v1-v1.4.7.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/fsnotify.v1/in_gopkg_fsnotify_v1-v1.4.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/fsnotify.v1/in_gopkg_fsnotify_v1-v1.4.7.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_go_playground_assert_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/go-playground/assert.v1",
        sha256 = "11da2f608d82304df2384a2301e0155fe72e8414e1a17776f1966c3a4c403bc4",
        strip_prefix = "gopkg.in/go-playground/assert.v1@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/go-playground/assert.v1/in_gopkg_go_playground_assert_v1-v1.2.1.zip",
            "http://ats.apps.svc/gomod/gopkg.in/go-playground/assert.v1/in_gopkg_go_playground_assert_v1-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/go-playground/assert.v1/in_gopkg_go_playground_assert_v1-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/go-playground/assert.v1/in_gopkg_go_playground_assert_v1-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_go_playground_validator_v8",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/go-playground/validator.v8",
        sha256 = "fea7482c7122c2573d964b7d294a78f2162fa206ccd4b808d0c82f3d87b4d159",
        strip_prefix = "gopkg.in/go-playground/validator.v8@v8.18.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/go-playground/validator.v8/in_gopkg_go_playground_validator_v8-v8.18.2.zip",
            "http://ats.apps.svc/gomod/gopkg.in/go-playground/validator.v8/in_gopkg_go_playground_validator_v8-v8.18.2.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/go-playground/validator.v8/in_gopkg_go_playground_validator_v8-v8.18.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/go-playground/validator.v8/in_gopkg_go_playground_validator_v8-v8.18.2.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_inf_v0",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/inf.v0",
        sha256 = "08abac18c95cc43b725d4925f63309398d618beab68b4669659b61255e5374a0",
        strip_prefix = "gopkg.in/inf.v0@v0.9.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/inf.v0/in_gopkg_inf_v0-v0.9.1.zip",
            "http://ats.apps.svc/gomod/gopkg.in/inf.v0/in_gopkg_inf_v0-v0.9.1.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/inf.v0/in_gopkg_inf_v0-v0.9.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/inf.v0/in_gopkg_inf_v0-v0.9.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_ini_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/ini.v1",
        sha256 = "bd845dfc762a87a56e5a32a07770dc83e86976db7705d7f89c5dbafdc60b06c6",
        strip_prefix = "gopkg.in/ini.v1@v1.67.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/ini.v1/in_gopkg_ini_v1-v1.67.0.zip",
            "http://ats.apps.svc/gomod/gopkg.in/ini.v1/in_gopkg_ini_v1-v1.67.0.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/ini.v1/in_gopkg_ini_v1-v1.67.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/ini.v1/in_gopkg_ini_v1-v1.67.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_aescts_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/aescts.v1",
        sha256 = "8bfd83c7204032fb16946202d5d643bd9a7e618005bd39578f29030a7d51dcf9",
        strip_prefix = "gopkg.in/jcmturner/aescts.v1@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/jcmturner/aescts.v1/in_gopkg_jcmturner_aescts_v1-v1.0.1.zip",
            "http://ats.apps.svc/gomod/gopkg.in/jcmturner/aescts.v1/in_gopkg_jcmturner_aescts_v1-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/jcmturner/aescts.v1/in_gopkg_jcmturner_aescts_v1-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/jcmturner/aescts.v1/in_gopkg_jcmturner_aescts_v1-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_dnsutils_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/dnsutils.v1",
        sha256 = "4fb8b6a5471cb6dda1d0aabd1e01e4d54cb5ee83c395849916392b19153f5203",
        strip_prefix = "gopkg.in/jcmturner/dnsutils.v1@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/jcmturner/dnsutils.v1/in_gopkg_jcmturner_dnsutils_v1-v1.0.1.zip",
            "http://ats.apps.svc/gomod/gopkg.in/jcmturner/dnsutils.v1/in_gopkg_jcmturner_dnsutils_v1-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/jcmturner/dnsutils.v1/in_gopkg_jcmturner_dnsutils_v1-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/jcmturner/dnsutils.v1/in_gopkg_jcmturner_dnsutils_v1-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_goidentity_v3",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/goidentity.v3",
        sha256 = "1be44bee93d9080ce89f40827c57e8a396b7c801e2d19a1f5446a4325afa755e",
        strip_prefix = "gopkg.in/jcmturner/goidentity.v3@v3.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/jcmturner/goidentity.v3/in_gopkg_jcmturner_goidentity_v3-v3.0.0.zip",
            "http://ats.apps.svc/gomod/gopkg.in/jcmturner/goidentity.v3/in_gopkg_jcmturner_goidentity_v3-v3.0.0.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/jcmturner/goidentity.v3/in_gopkg_jcmturner_goidentity_v3-v3.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/jcmturner/goidentity.v3/in_gopkg_jcmturner_goidentity_v3-v3.0.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_gokrb5_v7",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/gokrb5.v7",
        sha256 = "f7e772eaadb923044924cb86b7a6ed34a3386df831705bb62b6a47dc0819a94b",
        strip_prefix = "gopkg.in/jcmturner/gokrb5.v7@v7.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/jcmturner/gokrb5.v7/in_gopkg_jcmturner_gokrb5_v7-v7.3.0.zip",
            "http://ats.apps.svc/gomod/gopkg.in/jcmturner/gokrb5.v7/in_gopkg_jcmturner_gokrb5_v7-v7.3.0.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/jcmturner/gokrb5.v7/in_gopkg_jcmturner_gokrb5_v7-v7.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/jcmturner/gokrb5.v7/in_gopkg_jcmturner_gokrb5_v7-v7.3.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_rpc_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/rpc.v1",
        sha256 = "83d897b60ecb5a66d25232b775ed04c182ca8e02431f351b3768d4d2876d07ae",
        strip_prefix = "gopkg.in/jcmturner/rpc.v1@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/jcmturner/rpc.v1/in_gopkg_jcmturner_rpc_v1-v1.1.0.zip",
            "http://ats.apps.svc/gomod/gopkg.in/jcmturner/rpc.v1/in_gopkg_jcmturner_rpc_v1-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/jcmturner/rpc.v1/in_gopkg_jcmturner_rpc_v1-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/jcmturner/rpc.v1/in_gopkg_jcmturner_rpc_v1-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_mgo_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/mgo.v2",
        sha256 = "86c056ac7d51d59bb158bb740e774c0f80b28c8ce8db56d619a569aa96b2cd03",
        strip_prefix = "gopkg.in/mgo.v2@v2.0.0-20180705113604-9856a29383ce",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/mgo.v2/in_gopkg_mgo_v2-v2.0.0-20180705113604-9856a29383ce.zip",
            "http://ats.apps.svc/gomod/gopkg.in/mgo.v2/in_gopkg_mgo_v2-v2.0.0-20180705113604-9856a29383ce.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/mgo.v2/in_gopkg_mgo_v2-v2.0.0-20180705113604-9856a29383ce.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/mgo.v2/in_gopkg_mgo_v2-v2.0.0-20180705113604-9856a29383ce.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_natefinch_lumberjack_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/natefinch/lumberjack.v2",
        sha256 = "e28804b050e7debf4f5b2dd8d241d804f5d592d0519b6e7a3dc9d4cce6f075b3",
        strip_prefix = "gopkg.in/natefinch/lumberjack.v2@v2.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/natefinch/lumberjack.v2/in_gopkg_natefinch_lumberjack_v2-v2.2.1.zip",
            "http://ats.apps.svc/gomod/gopkg.in/natefinch/lumberjack.v2/in_gopkg_natefinch_lumberjack_v2-v2.2.1.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/natefinch/lumberjack.v2/in_gopkg_natefinch_lumberjack_v2-v2.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/natefinch/lumberjack.v2/in_gopkg_natefinch_lumberjack_v2-v2.2.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_tomb_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/tomb.v1",
        sha256 = "34898dc0e38ba7a792ab74a3e0fa113116313fd9142ffb444b011fd392762186",
        strip_prefix = "gopkg.in/tomb.v1@v1.0.0-20141024135613-dd632973f1e7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/tomb.v1/in_gopkg_tomb_v1-v1.0.0-20141024135613-dd632973f1e7.zip",
            "http://ats.apps.svc/gomod/gopkg.in/tomb.v1/in_gopkg_tomb_v1-v1.0.0-20141024135613-dd632973f1e7.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/tomb.v1/in_gopkg_tomb_v1-v1.0.0-20141024135613-dd632973f1e7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/tomb.v1/in_gopkg_tomb_v1-v1.0.0-20141024135613-dd632973f1e7.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_yaml_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/yaml.v2",
        sha256 = "ede49e27c4cca6cdd2ec719aed8ea4d363710cceb3d411e7a786fbdec0d391fd",
        strip_prefix = "gopkg.in/yaml.v2@v2.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/yaml.v2/in_gopkg_yaml_v2-v2.4.0.zip",
            "http://ats.apps.svc/gomod/gopkg.in/yaml.v2/in_gopkg_yaml_v2-v2.4.0.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/yaml.v2/in_gopkg_yaml_v2-v2.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/yaml.v2/in_gopkg_yaml_v2-v2.4.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_yaml_v3",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/yaml.v3",
        sha256 = "aab8fbc4e6300ea08e6afe1caea18a21c90c79f489f52c53e2f20431f1a9a015",
        strip_prefix = "gopkg.in/yaml.v3@v3.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/yaml.v3/in_gopkg_yaml_v3-v3.0.1.zip",
            "http://ats.apps.svc/gomod/gopkg.in/yaml.v3/in_gopkg_yaml_v3-v3.0.1.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/yaml.v3/in_gopkg_yaml_v3-v3.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/yaml.v3/in_gopkg_yaml_v3-v3.0.1.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_bbolt",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/bbolt",
        sha256 = "18babae67eccdd2982ad0bd44bb77a238e8b6c8da192b5ae6bd3c0dd48d5ba31",
        strip_prefix = "go.etcd.io/bbolt@v1.3.8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/bbolt/io_etcd_go_bbolt-v1.3.8.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/bbolt/io_etcd_go_bbolt-v1.3.8.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/bbolt/io_etcd_go_bbolt-v1.3.8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/bbolt/io_etcd_go_bbolt-v1.3.8.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_api_v3",
        build_file_proto_mode = "disable",
        importpath = "go.etcd.io/etcd/api/v3",
        patch_args = ["-p2"],
        patches = [
            "//build/patches:io_etcd_go_etcd_api_v3.patch",
        ],
        sha256 = "d935e64c70766be57ab28611ef071285f1aed5f62172dd5a2acf5b1aa536684c",
        strip_prefix = "go.etcd.io/etcd/api/v3@v3.5.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/api/v3/io_etcd_go_etcd_api_v3-v3.5.12.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/api/v3/io_etcd_go_etcd_api_v3-v3.5.12.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/api/v3/io_etcd_go_etcd_api_v3-v3.5.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/api/v3/io_etcd_go_etcd_api_v3-v3.5.12.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_client_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/pkg/v3",
        sha256 = "b50862d9f544d7603255ed46f39692fdb32becc84a11e0d21187faf89c6a4773",
        strip_prefix = "go.etcd.io/etcd/client/pkg/v3@v3.5.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/client/pkg/v3/io_etcd_go_etcd_client_pkg_v3-v3.5.12.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/client/pkg/v3/io_etcd_go_etcd_client_pkg_v3-v3.5.12.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/client/pkg/v3/io_etcd_go_etcd_client_pkg_v3-v3.5.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/client/pkg/v3/io_etcd_go_etcd_client_pkg_v3-v3.5.12.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v2",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/v2",
        sha256 = "9e7b6dee43696af28c46ac389c60a7e5763edf32078354440b8544bef862f630",
        strip_prefix = "go.etcd.io/etcd/client/v2@v2.305.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/client/v2/io_etcd_go_etcd_client_v2-v2.305.12.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/client/v2/io_etcd_go_etcd_client_v2-v2.305.12.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/client/v2/io_etcd_go_etcd_client_v2-v2.305.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/client/v2/io_etcd_go_etcd_client_v2-v2.305.12.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/v3",
        sha256 = "bd561ebc5528876cd5cf9309b37e03e16a6882ac64e48442d4fad17c3b102654",
        strip_prefix = "go.etcd.io/etcd/client/v3@v3.5.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/client/v3/io_etcd_go_etcd_client_v3-v3.5.12.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/client/v3/io_etcd_go_etcd_client_v3-v3.5.12.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/client/v3/io_etcd_go_etcd_client_v3-v3.5.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/client/v3/io_etcd_go_etcd_client_v3-v3.5.12.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_etcdutl_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/etcdutl/v3",
        sha256 = "99bd26e0e5c358037f265f39c39642eaf21f4805d99dce3f88ba0b8e4c870d66",
        strip_prefix = "go.etcd.io/etcd/etcdutl/v3@v3.5.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/etcdutl/v3/io_etcd_go_etcd_etcdutl_v3-v3.5.12.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/etcdutl/v3/io_etcd_go_etcd_etcdutl_v3-v3.5.12.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/etcdutl/v3/io_etcd_go_etcd_etcdutl_v3-v3.5.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/etcdutl/v3/io_etcd_go_etcd_etcdutl_v3-v3.5.12.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/pkg/v3",
        sha256 = "73626bb3d26427d12da20ef9f0f985150098da0c9d16fdeb501824d566b2f91f",
        strip_prefix = "go.etcd.io/etcd/pkg/v3@v3.5.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/pkg/v3/io_etcd_go_etcd_pkg_v3-v3.5.12.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/pkg/v3/io_etcd_go_etcd_pkg_v3-v3.5.12.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/pkg/v3/io_etcd_go_etcd_pkg_v3-v3.5.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/pkg/v3/io_etcd_go_etcd_pkg_v3-v3.5.12.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_raft_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/raft/v3",
        sha256 = "466f1111e22c3fd72f5f54c62dafee249895e8ed9b2e96154f84ad87ba504adc",
        strip_prefix = "go.etcd.io/etcd/raft/v3@v3.5.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/raft/v3/io_etcd_go_etcd_raft_v3-v3.5.12.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/raft/v3/io_etcd_go_etcd_raft_v3-v3.5.12.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/raft/v3/io_etcd_go_etcd_raft_v3-v3.5.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/raft/v3/io_etcd_go_etcd_raft_v3-v3.5.12.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_server_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/server/v3",
        sha256 = "2971de4109014606f79b60b376e85d4da45620203cd1649f2d8414b132105c11",
        strip_prefix = "go.etcd.io/etcd/server/v3@v3.5.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/server/v3/io_etcd_go_etcd_server_v3-v3.5.12.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/server/v3/io_etcd_go_etcd_server_v3-v3.5.12.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/server/v3/io_etcd_go_etcd_server_v3-v3.5.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/server/v3/io_etcd_go_etcd_server_v3-v3.5.12.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_tests_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/tests/v3",
        sha256 = "c9bb64b9ac82d2e9f7630632259065e16c40ea6bffe2c4be0e695154def88a04",
        strip_prefix = "go.etcd.io/etcd/tests/v3@v3.5.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/tests/v3/io_etcd_go_etcd_tests_v3-v3.5.12.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/tests/v3/io_etcd_go_etcd_tests_v3-v3.5.12.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/tests/v3/io_etcd_go_etcd_tests_v3-v3.5.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/tests/v3/io_etcd_go_etcd_tests_v3-v3.5.12.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_gofail",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/gofail",
        sha256 = "4fd6977dd736aba56be58c0b16e96d73433688976a5b352578d3c54d0db9e803",
        strip_prefix = "go.etcd.io/gofail@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/gofail/io_etcd_go_gofail-v0.1.0.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/gofail/io_etcd_go_gofail-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/gofail/io_etcd_go_gofail-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/gofail/io_etcd_go_gofail-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_api",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/api",
        sha256 = "eff8202063496b701784d305dc90c235c1b6df8b394a04161f1890f3ee8164e3",
        strip_prefix = "k8s.io/api@v0.28.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/api/io_k8s_api-v0.28.4.zip",
            "http://ats.apps.svc/gomod/k8s.io/api/io_k8s_api-v0.28.4.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/api/io_k8s_api-v0.28.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/api/io_k8s_api-v0.28.4.zip",
        ],
    )
    go_repository(
        name = "io_k8s_apimachinery",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/apimachinery",
        sha256 = "731a1f026cbb3cefcca91fc0a758d6249207f3b36cc004ac2e498b3828745b48",
        strip_prefix = "k8s.io/apimachinery@v0.28.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/apimachinery/io_k8s_apimachinery-v0.28.4.zip",
            "http://ats.apps.svc/gomod/k8s.io/apimachinery/io_k8s_apimachinery-v0.28.4.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/apimachinery/io_k8s_apimachinery-v0.28.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/apimachinery/io_k8s_apimachinery-v0.28.4.zip",
        ],
    )
    go_repository(
        name = "io_k8s_client_go",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/client-go",
        sha256 = "901547d5808428fee28f3541113980ccfed7ee11dd0141976d8bfeac9fba4736",
        strip_prefix = "k8s.io/client-go@v0.28.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/client-go/io_k8s_client_go-v0.28.4.zip",
            "http://ats.apps.svc/gomod/k8s.io/client-go/io_k8s_client_go-v0.28.4.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/client-go/io_k8s_client_go-v0.28.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/client-go/io_k8s_client_go-v0.28.4.zip",
        ],
    )
    go_repository(
        name = "io_k8s_klog",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog",
        sha256 = "a564b06078ddf014c5b793a7d36643d6fda31fc131e36b95cdea94ff838b99be",
        strip_prefix = "k8s.io/klog@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/klog/io_k8s_klog-v1.0.0.zip",
            "http://ats.apps.svc/gomod/k8s.io/klog/io_k8s_klog-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/klog/io_k8s_klog-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/klog/io_k8s_klog-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_klog_v2",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog/v2",
        sha256 = "b52957447658f4e86f649fa802adcacac8c550f8e997bd972b25e24a9c6c9d1a",
        strip_prefix = "k8s.io/klog/v2@v2.110.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/klog/v2/io_k8s_klog_v2-v2.110.1.zip",
            "http://ats.apps.svc/gomod/k8s.io/klog/v2/io_k8s_klog_v2-v2.110.1.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/klog/v2/io_k8s_klog_v2-v2.110.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/klog/v2/io_k8s_klog_v2-v2.110.1.zip",
        ],
    )
    go_repository(
        name = "io_k8s_kube_openapi",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/kube-openapi",
        sha256 = "1439fcbc0a04bbf7edf72712288e1cc4d2497fd28279c76a01a366910b65d6c7",
        strip_prefix = "k8s.io/kube-openapi@v0.0.0-20230717233707-2695361300d9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/kube-openapi/io_k8s_kube_openapi-v0.0.0-20230717233707-2695361300d9.zip",
            "http://ats.apps.svc/gomod/k8s.io/kube-openapi/io_k8s_kube_openapi-v0.0.0-20230717233707-2695361300d9.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/kube-openapi/io_k8s_kube_openapi-v0.0.0-20230717233707-2695361300d9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/kube-openapi/io_k8s_kube_openapi-v0.0.0-20230717233707-2695361300d9.zip",
        ],
    )
    go_repository(
        name = "io_k8s_sigs_json",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/json",
        sha256 = "ddcc6a7c6c22b9b370c270bfefb4b68f424bf740aa18cb766f104531de6e0e6e",
        strip_prefix = "sigs.k8s.io/json@v0.0.0-20221116044647-bc3834ca7abd",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/sigs.k8s.io/json/io_k8s_sigs_json-v0.0.0-20221116044647-bc3834ca7abd.zip",
            "http://ats.apps.svc/gomod/sigs.k8s.io/json/io_k8s_sigs_json-v0.0.0-20221116044647-bc3834ca7abd.zip",
            "https://cache.hawkingrei.com/gomod/sigs.k8s.io/json/io_k8s_sigs_json-v0.0.0-20221116044647-bc3834ca7abd.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/sigs.k8s.io/json/io_k8s_sigs_json-v0.0.0-20221116044647-bc3834ca7abd.zip",
        ],
    )
    go_repository(
        name = "io_k8s_sigs_structured_merge_diff_v4",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/structured-merge-diff/v4",
        sha256 = "0a5107d9269d3fc45d3abb9a1fc0c3c4788b82d848679416cb4c2bc49cad89de",
        strip_prefix = "sigs.k8s.io/structured-merge-diff/v4@v4.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/sigs.k8s.io/structured-merge-diff/v4/io_k8s_sigs_structured_merge_diff_v4-v4.3.0.zip",
            "http://ats.apps.svc/gomod/sigs.k8s.io/structured-merge-diff/v4/io_k8s_sigs_structured_merge_diff_v4-v4.3.0.zip",
            "https://cache.hawkingrei.com/gomod/sigs.k8s.io/structured-merge-diff/v4/io_k8s_sigs_structured_merge_diff_v4-v4.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/sigs.k8s.io/structured-merge-diff/v4/io_k8s_sigs_structured_merge_diff_v4-v4.3.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_sigs_yaml",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/yaml",
        sha256 = "ef031ff78ff9b7036e174eef49dfbd77468dc4f0afb73a639b61f8ab3a1cc425",
        strip_prefix = "sigs.k8s.io/yaml@v1.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/sigs.k8s.io/yaml/io_k8s_sigs_yaml-v1.4.0.zip",
            "http://ats.apps.svc/gomod/sigs.k8s.io/yaml/io_k8s_sigs_yaml-v1.4.0.zip",
            "https://cache.hawkingrei.com/gomod/sigs.k8s.io/yaml/io_k8s_sigs_yaml-v1.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/sigs.k8s.io/yaml/io_k8s_sigs_yaml-v1.4.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_utils",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/utils",
        sha256 = "755df44d714f0c28df51b94f096c1ff5af7625a00c92ca03b5914217a759b391",
        strip_prefix = "k8s.io/utils@v0.0.0-20230711102312-30195339c3c7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/utils/io_k8s_utils-v0.0.0-20230711102312-30195339c3c7.zip",
            "http://ats.apps.svc/gomod/k8s.io/utils/io_k8s_utils-v0.0.0-20230711102312-30195339c3c7.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/utils/io_k8s_utils-v0.0.0-20230711102312-30195339c3c7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/utils/io_k8s_utils-v0.0.0-20230711102312-30195339c3c7.zip",
        ],
    )
    go_repository(
        name = "io_opencensus_go",
        build_file_proto_mode = "disable_global",
        importpath = "go.opencensus.io",
        sha256 = "203a767d7f8e7c1ebe5588220ad168d1e15b14ae70a636de7ca9a4a88a7e0d0c",
        strip_prefix = "go.opencensus.io@v0.24.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opencensus.io/io_opencensus_go-v0.24.0.zip",
            "http://ats.apps.svc/gomod/go.opencensus.io/io_opencensus_go-v0.24.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opencensus.io/io_opencensus_go-v0.24.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opencensus.io/io_opencensus_go-v0.24.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_collector_featuregate",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/collector/featuregate",
        sha256 = "edd801853ad91428eed8d717c00b6b042c1f6aa4c0c90428a79148156c33886b",
        strip_prefix = "go.opentelemetry.io/collector/featuregate@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/collector/featuregate/io_opentelemetry_go_collector_featuregate-v1.0.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/collector/featuregate/io_opentelemetry_go_collector_featuregate-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/collector/featuregate/io_opentelemetry_go_collector_featuregate-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/collector/featuregate/io_opentelemetry_go_collector_featuregate-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_collector_pdata",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/collector/pdata",
        sha256 = "7705242dc35df16befd2a69a79f9f9797b2412623a7a81e47622d69a98219f0c",
        strip_prefix = "go.opentelemetry.io/collector/pdata@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/collector/pdata/io_opentelemetry_go_collector_pdata-v1.0.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/collector/pdata/io_opentelemetry_go_collector_pdata-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/collector/pdata/io_opentelemetry_go_collector_pdata-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/collector/pdata/io_opentelemetry_go_collector_pdata-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_collector_semconv",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/collector/semconv",
        sha256 = "75958ce453b97b61b25de8c214f0006cedf200df94ece196b9f52a600a749cfd",
        strip_prefix = "go.opentelemetry.io/collector/semconv@v0.90.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/collector/semconv/io_opentelemetry_go_collector_semconv-v0.90.1.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/collector/semconv/io_opentelemetry_go_collector_semconv-v0.90.1.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/collector/semconv/io_opentelemetry_go_collector_semconv-v0.90.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/collector/semconv/io_opentelemetry_go_collector_semconv-v0.90.1.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
        sha256 = "efb13c3eb89199b0f677057a238017b06ffceb868ac55f4f649a31309ec0321d",
        strip_prefix = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc@v0.47.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc-v0.47.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc-v0.47.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc-v0.47.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc-v0.47.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
        sha256 = "476b9113a426e31a3802d8371c348ae3334c56acba9fc7228b886096c64647a1",
        strip_prefix = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp@v0.47.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp-v0.47.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp-v0.47.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp-v0.47.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp-v0.47.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel",
        sha256 = "6c02668caef05c8221cf2fb43f4f2943de701f3c4fb69096f2f7da523f1c80d2",
        strip_prefix = "go.opentelemetry.io/otel@v1.22.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/io_opentelemetry_go_otel-v1.22.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/io_opentelemetry_go_otel-v1.22.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/io_opentelemetry_go_otel-v1.22.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/io_opentelemetry_go_otel-v1.22.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace",
        sha256 = "540b6c364dff64897449227b430ce47e2c91dd1d42a09a73c2b8af51598870c8",
        strip_prefix = "go.opentelemetry.io/otel/exporters/otlp/otlptrace@v1.22.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/io_opentelemetry_go_otel_exporters_otlp_otlptrace-v1.22.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/io_opentelemetry_go_otel_exporters_otlp_otlptrace-v1.22.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/io_opentelemetry_go_otel_exporters_otlp_otlptrace-v1.22.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/io_opentelemetry_go_otel_exporters_otlp_otlptrace-v1.22.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc",
        sha256 = "bdd672c9e7fd5fd7075dab3671242301495eac85352dc0f725b325d988e4084f",
        strip_prefix = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@v1.22.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc-v1.22.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc-v1.22.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc-v1.22.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc-v1.22.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp",
        sha256 = "0cf89afaad577e3d43b7071994a0bf57e18a1c27a0d6e98a60b9e995fa9e97fb",
        strip_prefix = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp@v1.21.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp-v1.21.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp-v1.21.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp-v1.21.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp-v1.21.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_metric",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/metric",
        sha256 = "deb750b41631365dbda60fb872dc6901f16817e0b2646307fbfeb4eeb391e02f",
        strip_prefix = "go.opentelemetry.io/otel/metric@v1.22.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/metric/io_opentelemetry_go_otel_metric-v1.22.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/metric/io_opentelemetry_go_otel_metric-v1.22.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/metric/io_opentelemetry_go_otel_metric-v1.22.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/metric/io_opentelemetry_go_otel_metric-v1.22.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/sdk",
        sha256 = "d17065c74f1cbae1c5fff90289919807a859733673ab8491c5f5883475ae4657",
        strip_prefix = "go.opentelemetry.io/otel/sdk@v1.22.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/sdk/io_opentelemetry_go_otel_sdk-v1.22.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/sdk/io_opentelemetry_go_otel_sdk-v1.22.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/sdk/io_opentelemetry_go_otel_sdk-v1.22.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/sdk/io_opentelemetry_go_otel_sdk-v1.22.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/trace",
        sha256 = "d63e40f32d614b00bedfa945eae95e2bc5fe867e167cd7dbfe7f90d96fa599d7",
        strip_prefix = "go.opentelemetry.io/otel/trace@v1.22.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/trace/io_opentelemetry_go_otel_trace-v1.22.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/trace/io_opentelemetry_go_otel_trace-v1.22.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/trace/io_opentelemetry_go_otel_trace-v1.22.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/trace/io_opentelemetry_go_otel_trace-v1.22.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_proto_otlp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/proto/otlp",
        sha256 = "46e71b2f65b00ee99ac72144caac6eb83a0316a03b736a4e3a76235874783649",
        strip_prefix = "go.opentelemetry.io/proto/otlp@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/proto/otlp/io_opentelemetry_go_proto_otlp-v1.1.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/proto/otlp/io_opentelemetry_go_proto_otlp-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/proto/otlp/io_opentelemetry_go_proto_otlp-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/proto/otlp/io_opentelemetry_go_proto_otlp-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "io_rsc_binaryregexp",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/binaryregexp",
        sha256 = "b3e706aa278fa7f880d32fa1cc40ef8282d1fc7d6e00356579ed0db88f3b0047",
        strip_prefix = "rsc.io/binaryregexp@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/rsc.io/binaryregexp/io_rsc_binaryregexp-v0.2.0.zip",
            "http://ats.apps.svc/gomod/rsc.io/binaryregexp/io_rsc_binaryregexp-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/rsc.io/binaryregexp/io_rsc_binaryregexp-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/rsc.io/binaryregexp/io_rsc_binaryregexp-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "io_rsc_pdf",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/pdf",
        sha256 = "79bf310e399cf0e2d8aa61536750d2a6999c5ca884e7a27faf88d3701cd5ba8f",
        strip_prefix = "rsc.io/pdf@v0.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/rsc.io/pdf/io_rsc_pdf-v0.1.1.zip",
            "http://ats.apps.svc/gomod/rsc.io/pdf/io_rsc_pdf-v0.1.1.zip",
            "https://cache.hawkingrei.com/gomod/rsc.io/pdf/io_rsc_pdf-v0.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/rsc.io/pdf/io_rsc_pdf-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "io_rsc_quote_v3",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/quote/v3",
        sha256 = "b434cbbfc32c17b5228d0b0eddeaea89bef4ec9bd90b5c8fc55b64f8ce13eeb9",
        strip_prefix = "rsc.io/quote/v3@v3.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/rsc.io/quote/v3/io_rsc_quote_v3-v3.1.0.zip",
            "http://ats.apps.svc/gomod/rsc.io/quote/v3/io_rsc_quote_v3-v3.1.0.zip",
            "https://cache.hawkingrei.com/gomod/rsc.io/quote/v3/io_rsc_quote_v3-v3.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/rsc.io/quote/v3/io_rsc_quote_v3-v3.1.0.zip",
        ],
    )
    go_repository(
        name = "io_rsc_sampler",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/sampler",
        sha256 = "da202b0da803ab2661ab98a680bba4f64123a326e540c25582b6cdbb9dc114aa",
        strip_prefix = "rsc.io/sampler@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/rsc.io/sampler/io_rsc_sampler-v1.3.0.zip",
            "http://ats.apps.svc/gomod/rsc.io/sampler/io_rsc_sampler-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/rsc.io/sampler/io_rsc_sampler-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/rsc.io/sampler/io_rsc_sampler-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "net_starlark_go",
        build_file_proto_mode = "disable_global",
        importpath = "go.starlark.net",
        sha256 = "6f936b11557fe2855ec58245bebfd34260db79d2e4dc63ab58659f3de1dde51c",
        strip_prefix = "go.starlark.net@v0.0.0-20210223155950-e043a3d3c984",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.starlark.net/net_starlark_go-v0.0.0-20210223155950-e043a3d3c984.zip",
            "http://ats.apps.svc/gomod/go.starlark.net/net_starlark_go-v0.0.0-20210223155950-e043a3d3c984.zip",
            "https://cache.hawkingrei.com/gomod/go.starlark.net/net_starlark_go-v0.0.0-20210223155950-e043a3d3c984.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.starlark.net/net_starlark_go-v0.0.0-20210223155950-e043a3d3c984.zip",
        ],
    )
    go_repository(
        name = "org_go_simpler_musttag",
        build_file_proto_mode = "disable_global",
        importpath = "go-simpler.org/musttag",
        sha256 = "d25d425564e4d784d728492abee3a8133cceaf265cef688c7e76ac7bbe1b4fcf",
        strip_prefix = "go-simpler.org/musttag@v0.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go-simpler.org/musttag/org_go_simpler_musttag-v0.8.0.zip",
            "http://ats.apps.svc/gomod/go-simpler.org/musttag/org_go_simpler_musttag-v0.8.0.zip",
            "https://cache.hawkingrei.com/gomod/go-simpler.org/musttag/org_go_simpler_musttag-v0.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go-simpler.org/musttag/org_go_simpler_musttag-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "org_go_simpler_sloglint",
        build_file_proto_mode = "disable_global",
        importpath = "go-simpler.org/sloglint",
        sha256 = "6de44edbc79a68c39592de40cff36dde989fd85764f490d9e465dc0815d9954e",
        strip_prefix = "go-simpler.org/sloglint@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go-simpler.org/sloglint/org_go_simpler_sloglint-v0.4.0.zip",
            "http://ats.apps.svc/gomod/go-simpler.org/sloglint/org_go_simpler_sloglint-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/go-simpler.org/sloglint/org_go_simpler_sloglint-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go-simpler.org/sloglint/org_go_simpler_sloglint-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_api",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/api",
        sha256 = "5eb3019627247e9dfccf458bdad1c4227c7919079bb8f300c7f46612d118e9c7",
        strip_prefix = "google.golang.org/api@v0.162.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/api/org_golang_google_api-v0.162.0.zip",
            "http://ats.apps.svc/gomod/google.golang.org/api/org_golang_google_api-v0.162.0.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/api/org_golang_google_api-v0.162.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/api/org_golang_google_api-v0.162.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_appengine",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/appengine",
        sha256 = "23e40ee378db26bd45b7de851a85ba6c6d340c9dd353f8ba961ebe9e01bf02c6",
        strip_prefix = "google.golang.org/appengine@v1.6.8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/appengine/org_golang_google_appengine-v1.6.8.zip",
            "http://ats.apps.svc/gomod/google.golang.org/appengine/org_golang_google_appengine-v1.6.8.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/appengine/org_golang_google_appengine-v1.6.8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/appengine/org_golang_google_appengine-v1.6.8.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_genproto",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto",
        sha256 = "9b1952b84fe434fd61f9aeb6ca3766f9bd69537a5e32868a25049d135a908043",
        strip_prefix = "google.golang.org/genproto@v0.0.0-20240213162025-012b6fc9bca9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/genproto/org_golang_google_genproto-v0.0.0-20240213162025-012b6fc9bca9.zip",
            "http://ats.apps.svc/gomod/google.golang.org/genproto/org_golang_google_genproto-v0.0.0-20240213162025-012b6fc9bca9.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/genproto/org_golang_google_genproto-v0.0.0-20240213162025-012b6fc9bca9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/genproto/org_golang_google_genproto-v0.0.0-20240213162025-012b6fc9bca9.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_api",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto/googleapis/api",
        sha256 = "1ebe3c1107c126819cd1b27f2eb3966df4fcd434bbe45d5ef2cba514091a8c80",
        strip_prefix = "google.golang.org/genproto/googleapis/api@v0.0.0-20240304212257-790db918fca8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/genproto/googleapis/api/org_golang_google_genproto_googleapis_api-v0.0.0-20240304212257-790db918fca8.zip",
            "http://ats.apps.svc/gomod/google.golang.org/genproto/googleapis/api/org_golang_google_genproto_googleapis_api-v0.0.0-20240304212257-790db918fca8.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/genproto/googleapis/api/org_golang_google_genproto_googleapis_api-v0.0.0-20240304212257-790db918fca8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/genproto/googleapis/api/org_golang_google_genproto_googleapis_api-v0.0.0-20240304212257-790db918fca8.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_bytestream",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto/googleapis/bytestream",
        sha256 = "407e8ddfcc6c5c96a84bc80425139c36f0e12a5134cb5efe52ecc6233644d381",
        strip_prefix = "google.golang.org/genproto/googleapis/bytestream@v0.0.0-20240125205218-1f4bbc51befe",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/genproto/googleapis/bytestream/org_golang_google_genproto_googleapis_bytestream-v0.0.0-20240125205218-1f4bbc51befe.zip",
            "http://ats.apps.svc/gomod/google.golang.org/genproto/googleapis/bytestream/org_golang_google_genproto_googleapis_bytestream-v0.0.0-20240125205218-1f4bbc51befe.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/genproto/googleapis/bytestream/org_golang_google_genproto_googleapis_bytestream-v0.0.0-20240125205218-1f4bbc51befe.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/genproto/googleapis/bytestream/org_golang_google_genproto_googleapis_bytestream-v0.0.0-20240125205218-1f4bbc51befe.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_rpc",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto/googleapis/rpc",
        sha256 = "755a36227e2551491d44533ef50df7f47964db44911e6d4d84e4d4842a5339d6",
        strip_prefix = "google.golang.org/genproto/googleapis/rpc@v0.0.0-20240308144416-29370a3891b7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/genproto/googleapis/rpc/org_golang_google_genproto_googleapis_rpc-v0.0.0-20240308144416-29370a3891b7.zip",
            "http://ats.apps.svc/gomod/google.golang.org/genproto/googleapis/rpc/org_golang_google_genproto_googleapis_rpc-v0.0.0-20240308144416-29370a3891b7.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/genproto/googleapis/rpc/org_golang_google_genproto_googleapis_rpc-v0.0.0-20240308144416-29370a3891b7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/genproto/googleapis/rpc/org_golang_google_genproto_googleapis_rpc-v0.0.0-20240308144416-29370a3891b7.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/grpc",
        sha256 = "95226c98d052d7e4cd308c4df972464ba90e302cf12b5f180e245c4c3d2cc02f",
        strip_prefix = "google.golang.org/grpc@v1.62.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/grpc/org_golang_google_grpc-v1.62.1.zip",
            "http://ats.apps.svc/gomod/google.golang.org/grpc/org_golang_google_grpc-v1.62.1.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/grpc/org_golang_google_grpc-v1.62.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/grpc/org_golang_google_grpc-v1.62.1.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_grpc_examples",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/grpc/examples",
        sha256 = "1d6cbdae96a305d977ffa3b101fd89fa9bceb80cead93254d3f85b43faf40e07",
        strip_prefix = "google.golang.org/grpc/examples@v0.0.0-20231221225426-4f03f3ff32c9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/grpc/examples/org_golang_google_grpc_examples-v0.0.0-20231221225426-4f03f3ff32c9.zip",
            "http://ats.apps.svc/gomod/google.golang.org/grpc/examples/org_golang_google_grpc_examples-v0.0.0-20231221225426-4f03f3ff32c9.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/grpc/examples/org_golang_google_grpc_examples-v0.0.0-20231221225426-4f03f3ff32c9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/grpc/examples/org_golang_google_grpc_examples-v0.0.0-20231221225426-4f03f3ff32c9.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/protobuf",
        sha256 = "2cc1c98e12903009bd4bf0d5e938a421ca2f88ae87b0fc50004b2c7598b1fd24",
        strip_prefix = "google.golang.org/protobuf@v1.33.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/protobuf/org_golang_google_protobuf-v1.33.0.zip",
            "http://ats.apps.svc/gomod/google.golang.org/protobuf/org_golang_google_protobuf-v1.33.0.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/protobuf/org_golang_google_protobuf-v1.33.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/protobuf/org_golang_google_protobuf-v1.33.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_crypto",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/crypto",
        sha256 = "689d6b9313d406e061863b9b84eb43b02b7fbe081a49bb25097bfb192f1b90e0",
        strip_prefix = "golang.org/x/crypto@v0.21.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/crypto/org_golang_x_crypto-v0.21.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/crypto/org_golang_x_crypto-v0.21.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/crypto/org_golang_x_crypto-v0.21.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/crypto/org_golang_x_crypto-v0.21.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_exp",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/exp",
        sha256 = "b5b6cede556511e8318cda042863a37fcd68e8241bd82f751c596dbdb78d449a",
        strip_prefix = "golang.org/x/exp@v0.0.0-20240205201215-2c58cdc269a3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/exp/org_golang_x_exp-v0.0.0-20240205201215-2c58cdc269a3.zip",
            "http://ats.apps.svc/gomod/golang.org/x/exp/org_golang_x_exp-v0.0.0-20240205201215-2c58cdc269a3.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/exp/org_golang_x_exp-v0.0.0-20240205201215-2c58cdc269a3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/exp/org_golang_x_exp-v0.0.0-20240205201215-2c58cdc269a3.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_exp_typeparams",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/exp/typeparams",
        sha256 = "9c733e1b02340f677a4b2a5b1487a9ae417fbd8d03b9fcbbfd7d9cf4fc8b273c",
        strip_prefix = "golang.org/x/exp/typeparams@v0.0.0-20231219180239-dc181d75b848",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/exp/typeparams/org_golang_x_exp_typeparams-v0.0.0-20231219180239-dc181d75b848.zip",
            "http://ats.apps.svc/gomod/golang.org/x/exp/typeparams/org_golang_x_exp_typeparams-v0.0.0-20231219180239-dc181d75b848.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/exp/typeparams/org_golang_x_exp_typeparams-v0.0.0-20231219180239-dc181d75b848.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/exp/typeparams/org_golang_x_exp_typeparams-v0.0.0-20231219180239-dc181d75b848.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_image",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/image",
        sha256 = "4a44b498934a95e8f84e8374530de0cab38d81fcd558898d4880c3c5ce1efe47",
        strip_prefix = "golang.org/x/image@v0.0.0-20190802002840-cff245a6509b",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/image/org_golang_x_image-v0.0.0-20190802002840-cff245a6509b.zip",
            "http://ats.apps.svc/gomod/golang.org/x/image/org_golang_x_image-v0.0.0-20190802002840-cff245a6509b.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/image/org_golang_x_image-v0.0.0-20190802002840-cff245a6509b.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/image/org_golang_x_image-v0.0.0-20190802002840-cff245a6509b.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_lint",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/lint",
        sha256 = "4620205ccd1fd5c5ced7ccbc264217f407c53924e847f4219e48c04c7480b294",
        strip_prefix = "golang.org/x/lint@v0.0.0-20201208152925-83fdc39ff7b5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/lint/org_golang_x_lint-v0.0.0-20201208152925-83fdc39ff7b5.zip",
            "http://ats.apps.svc/gomod/golang.org/x/lint/org_golang_x_lint-v0.0.0-20201208152925-83fdc39ff7b5.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/lint/org_golang_x_lint-v0.0.0-20201208152925-83fdc39ff7b5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/lint/org_golang_x_lint-v0.0.0-20201208152925-83fdc39ff7b5.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_mobile",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/mobile",
        sha256 = "6b946c7da47acf3b6195336fd071bfc73d543cefab73f2d27528c5dc1dc829ec",
        strip_prefix = "golang.org/x/mobile@v0.0.0-20190719004257-d2bd2a29d028",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/mobile/org_golang_x_mobile-v0.0.0-20190719004257-d2bd2a29d028.zip",
            "http://ats.apps.svc/gomod/golang.org/x/mobile/org_golang_x_mobile-v0.0.0-20190719004257-d2bd2a29d028.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/mobile/org_golang_x_mobile-v0.0.0-20190719004257-d2bd2a29d028.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/mobile/org_golang_x_mobile-v0.0.0-20190719004257-d2bd2a29d028.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_mod",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/mod",
        sha256 = "44bb0b60a305036c6402f9b2d58f7d3cfca310cff57241f37c6e0a6bdafacb15",
        strip_prefix = "golang.org/x/mod@v0.16.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/mod/org_golang_x_mod-v0.16.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/mod/org_golang_x_mod-v0.16.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/mod/org_golang_x_mod-v0.16.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/mod/org_golang_x_mod-v0.16.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_net",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/net",
        sha256 = "2f624e504f4cd569e907a9449d349f1c4e3652623fb9e352e81d2155ecc2c133",
        strip_prefix = "golang.org/x/net@v0.22.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/net/org_golang_x_net-v0.22.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/net/org_golang_x_net-v0.22.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/net/org_golang_x_net-v0.22.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/net/org_golang_x_net-v0.22.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_oauth2",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/oauth2",
        sha256 = "e59eee832df8784517e9d77cf608784a659d2001c9af66591c1787e17401622e",
        strip_prefix = "golang.org/x/oauth2@v0.17.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/oauth2/org_golang_x_oauth2-v0.17.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/oauth2/org_golang_x_oauth2-v0.17.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/oauth2/org_golang_x_oauth2-v0.17.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/oauth2/org_golang_x_oauth2-v0.17.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_sync",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sync",
        sha256 = "7c75175297a3b368b806bd24c7401629df11dcc655e3c14470058282f101ca6a",
        strip_prefix = "golang.org/x/sync@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/sync/org_golang_x_sync-v0.6.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/sync/org_golang_x_sync-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/sync/org_golang_x_sync-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/sync/org_golang_x_sync-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_sys",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sys",
        sha256 = "96e3b16b15a7d193c9db2974db4cabed29b37ab4bb09f63edfa441199de6fdf8",
        strip_prefix = "golang.org/x/sys@v0.18.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/sys/org_golang_x_sys-v0.18.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/sys/org_golang_x_sys-v0.18.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/sys/org_golang_x_sys-v0.18.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/sys/org_golang_x_sys-v0.18.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_telemetry",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/telemetry",
        sha256 = "f55ae2a94f2288599047dc621479f614d88cb64bb2cddd200687b69d10768ce6",
        strip_prefix = "golang.org/x/telemetry@v0.0.0-20240208230135-b75ee8823808",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/telemetry/org_golang_x_telemetry-v0.0.0-20240208230135-b75ee8823808.zip",
            "http://ats.apps.svc/gomod/golang.org/x/telemetry/org_golang_x_telemetry-v0.0.0-20240208230135-b75ee8823808.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/telemetry/org_golang_x_telemetry-v0.0.0-20240208230135-b75ee8823808.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/telemetry/org_golang_x_telemetry-v0.0.0-20240208230135-b75ee8823808.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_term",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/term",
        sha256 = "60652f7dd2fa4185c62867bcaa3fa56e59b07f5b71083d8f72ab882d251355a6",
        strip_prefix = "golang.org/x/term@v0.18.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/term/org_golang_x_term-v0.18.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/term/org_golang_x_term-v0.18.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/term/org_golang_x_term-v0.18.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/term/org_golang_x_term-v0.18.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_text",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/text",
        sha256 = "b9814897e0e09cd576a7a013f066c7db537a3d538d2e0f60f0caee9bc1b3f4af",
        strip_prefix = "golang.org/x/text@v0.14.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/text/org_golang_x_text-v0.14.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/text/org_golang_x_text-v0.14.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/text/org_golang_x_text-v0.14.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/text/org_golang_x_text-v0.14.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_time",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/time",
        sha256 = "e0e5812d19aed367f79ac0ae0ce4770b6602c85f5cfb8d59f3f573c7487ea516",
        strip_prefix = "golang.org/x/time@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/time/org_golang_x_time-v0.5.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/time/org_golang_x_time-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/time/org_golang_x_time-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/time/org_golang_x_time-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_tools",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/tools",
        sha256 = "e78f5eb33f21ca0f604da3bac5a23f107d2504a03251cff1eeea15274fb84444",
        strip_prefix = "golang.org/x/tools@v0.18.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/tools/org_golang_x_tools-v0.18.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/tools/org_golang_x_tools-v0.18.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/tools/org_golang_x_tools-v0.18.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/tools/org_golang_x_tools-v0.18.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_xerrors",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/xerrors",
        sha256 = "df5dd109153c94d2f5c9601d28f558871094e37c42f8e3875f36db858d8be9f9",
        strip_prefix = "golang.org/x/xerrors@v0.0.0-20231012003039-104605ab7028",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/xerrors/org_golang_x_xerrors-v0.0.0-20231012003039-104605ab7028.zip",
            "http://ats.apps.svc/gomod/golang.org/x/xerrors/org_golang_x_xerrors-v0.0.0-20231012003039-104605ab7028.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/xerrors/org_golang_x_xerrors-v0.0.0-20231012003039-104605ab7028.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/xerrors/org_golang_x_xerrors-v0.0.0-20231012003039-104605ab7028.zip",
        ],
    )
    go_repository(
        name = "org_gonum_v1_gonum",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/gonum",
        sha256 = "57ecefd9c1ab5a40ed9e37e824597e523e85e78022cd8a4fc5533ff785f49863",
        strip_prefix = "gonum.org/v1/gonum@v0.8.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gonum.org/v1/gonum/org_gonum_v1_gonum-v0.8.2.zip",
            "http://ats.apps.svc/gomod/gonum.org/v1/gonum/org_gonum_v1_gonum-v0.8.2.zip",
            "https://cache.hawkingrei.com/gomod/gonum.org/v1/gonum/org_gonum_v1_gonum-v0.8.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gonum.org/v1/gonum/org_gonum_v1_gonum-v0.8.2.zip",
        ],
    )
    go_repository(
        name = "org_gonum_v1_netlib",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/netlib",
        sha256 = "eeaeb60f410b86f59d97f15c5ef89096dc72aeb42bae55141738bf9866893938",
        strip_prefix = "gonum.org/v1/netlib@v0.0.0-20190313105609-8cb42192e0e0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gonum.org/v1/netlib/org_gonum_v1_netlib-v0.0.0-20190313105609-8cb42192e0e0.zip",
            "http://ats.apps.svc/gomod/gonum.org/v1/netlib/org_gonum_v1_netlib-v0.0.0-20190313105609-8cb42192e0e0.zip",
            "https://cache.hawkingrei.com/gomod/gonum.org/v1/netlib/org_gonum_v1_netlib-v0.0.0-20190313105609-8cb42192e0e0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gonum.org/v1/netlib/org_gonum_v1_netlib-v0.0.0-20190313105609-8cb42192e0e0.zip",
        ],
    )
    go_repository(
        name = "org_gonum_v1_plot",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/plot",
        sha256 = "2d4cadb4bafb5bbfe1f614d7e402c670446fccd154bc4c6b1699e3dffde68ff4",
        strip_prefix = "gonum.org/v1/plot@v0.0.0-20190515093506-e2840ee46a6b",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gonum.org/v1/plot/org_gonum_v1_plot-v0.0.0-20190515093506-e2840ee46a6b.zip",
            "http://ats.apps.svc/gomod/gonum.org/v1/plot/org_gonum_v1_plot-v0.0.0-20190515093506-e2840ee46a6b.zip",
            "https://cache.hawkingrei.com/gomod/gonum.org/v1/plot/org_gonum_v1_plot-v0.0.0-20190515093506-e2840ee46a6b.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gonum.org/v1/plot/org_gonum_v1_plot-v0.0.0-20190515093506-e2840ee46a6b.zip",
        ],
    )
    go_repository(
        name = "org_modernc_golex",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/golex",
        sha256 = "3099b4f7e65cc38d113d6558f2a223ba4ce5288a930b182ac6ef679a96dbcfe5",
        strip_prefix = "modernc.org/golex@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/golex/org_modernc_golex-v1.1.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/golex/org_modernc_golex-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/golex/org_modernc_golex-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/golex/org_modernc_golex-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_mathutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/mathutil",
        sha256 = "3a9e2065897e172b4c092c3098e15a2d66bc2700432f88ba6812c1b6b0acf2b2",
        strip_prefix = "modernc.org/mathutil@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/mathutil/org_modernc_mathutil-v1.6.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/mathutil/org_modernc_mathutil-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/mathutil/org_modernc_mathutil-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/mathutil/org_modernc_mathutil-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_parser",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/parser",
        sha256 = "e50f98025a0dfff5ffe5fe5dba38a11f85e5402cdcb9ed7ed0a8d3db6d811b67",
        strip_prefix = "modernc.org/parser@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/parser/org_modernc_parser-v1.1.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/parser/org_modernc_parser-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/parser/org_modernc_parser-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/parser/org_modernc_parser-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_sortutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/sortutil",
        sha256 = "30f47ffd690ba68e88bcb7f2a1f3d61505580c0d62ba32c2bab5017077208f60",
        strip_prefix = "modernc.org/sortutil@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/sortutil/org_modernc_sortutil-v1.2.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/sortutil/org_modernc_sortutil-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/sortutil/org_modernc_sortutil-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/sortutil/org_modernc_sortutil-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_strutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/strutil",
        sha256 = "1ea20b81cf3fd6efad4bb1b791255cc2fd486111d5e9f2cb0e551e9d39aa3f8f",
        strip_prefix = "modernc.org/strutil@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/strutil/org_modernc_strutil-v1.2.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/strutil/org_modernc_strutil-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/strutil/org_modernc_strutil-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/strutil/org_modernc_strutil-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_y",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/y",
        sha256 = "cce55de6a0fe8fa41f1bf95184316f02f90966a12a34bc38534d610920be4720",
        strip_prefix = "modernc.org/y@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/y/org_modernc_y-v1.1.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/y/org_modernc_y-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/y/org_modernc_y-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/y/org_modernc_y-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "org_mongodb_go_mongo_driver",
        build_file_proto_mode = "disable_global",
        importpath = "go.mongodb.org/mongo-driver",
        sha256 = "72d6d482c70104374d8d5ac91653b46aec4c7c1e610e0fd4a82d5d88b4a65b3e",
        strip_prefix = "go.mongodb.org/mongo-driver@v1.13.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.mongodb.org/mongo-driver/org_mongodb_go_mongo_driver-v1.13.1.zip",
            "http://ats.apps.svc/gomod/go.mongodb.org/mongo-driver/org_mongodb_go_mongo_driver-v1.13.1.zip",
            "https://cache.hawkingrei.com/gomod/go.mongodb.org/mongo-driver/org_mongodb_go_mongo_driver-v1.13.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.mongodb.org/mongo-driver/org_mongodb_go_mongo_driver-v1.13.1.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_atomic",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/atomic",
        sha256 = "8109325abe17488245878b07f3c35b10ba7d1aa3310f792968f5b9deba432e2c",
        strip_prefix = "go.uber.org/atomic@v1.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/atomic/org_uber_go_atomic-v1.11.0.zip",
            "http://ats.apps.svc/gomod/go.uber.org/atomic/org_uber_go_atomic-v1.11.0.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/atomic/org_uber_go_atomic-v1.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/atomic/org_uber_go_atomic-v1.11.0.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_automaxprocs",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/automaxprocs",
        sha256 = "5d7328fb862935d8c2f3fb8c9987798a1b70efd824521c1d5ebd819416ab207d",
        strip_prefix = "go.uber.org/automaxprocs@v1.5.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/automaxprocs/org_uber_go_automaxprocs-v1.5.3.zip",
            "http://ats.apps.svc/gomod/go.uber.org/automaxprocs/org_uber_go_automaxprocs-v1.5.3.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/automaxprocs/org_uber_go_automaxprocs-v1.5.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/automaxprocs/org_uber_go_automaxprocs-v1.5.3.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_goleak",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/goleak",
        sha256 = "70edef0ce7d830d992f024e527fd3452069b884f94a27787a718bd68dd620702",
        strip_prefix = "go.uber.org/goleak@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/goleak/org_uber_go_goleak-v1.3.0.zip",
            "http://ats.apps.svc/gomod/go.uber.org/goleak/org_uber_go_goleak-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/goleak/org_uber_go_goleak-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/goleak/org_uber_go_goleak-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_mock",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/mock",
        sha256 = "29c088ba1621e04fba8670e388e962f92c15f47cd45a63bf0e5decd6d5d63cd1",
        strip_prefix = "go.uber.org/mock@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/mock/org_uber_go_mock-v0.4.0.zip",
            "http://ats.apps.svc/gomod/go.uber.org/mock/org_uber_go_mock-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/mock/org_uber_go_mock-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/mock/org_uber_go_mock-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_multierr",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/multierr",
        sha256 = "2249b5d2fdce61f6ee661a679d8552599af084a761cbbc871da77641bddce0c3",
        strip_prefix = "go.uber.org/multierr@v1.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/multierr/org_uber_go_multierr-v1.11.0.zip",
            "http://ats.apps.svc/gomod/go.uber.org/multierr/org_uber_go_multierr-v1.11.0.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/multierr/org_uber_go_multierr-v1.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/multierr/org_uber_go_multierr-v1.11.0.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_tools",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/tools",
        sha256 = "988dba9c5074080240d33d98e8ce511532f728698db7a9a4ac316c02c94030d6",
        strip_prefix = "go.uber.org/tools@v0.0.0-20190618225709-2cfd321de3ee",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/tools/org_uber_go_tools-v0.0.0-20190618225709-2cfd321de3ee.zip",
            "http://ats.apps.svc/gomod/go.uber.org/tools/org_uber_go_tools-v0.0.0-20190618225709-2cfd321de3ee.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/tools/org_uber_go_tools-v0.0.0-20190618225709-2cfd321de3ee.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/tools/org_uber_go_tools-v0.0.0-20190618225709-2cfd321de3ee.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_zap",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/zap",
        sha256 = "b994b96ff0bb504a3d58288ab88b9f3c6604689ea1afb69d25b509769705a6c2",
        strip_prefix = "go.uber.org/zap@v1.27.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/zap/org_uber_go_zap-v1.27.0.zip",
            "http://ats.apps.svc/gomod/go.uber.org/zap/org_uber_go_zap-v1.27.0.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/zap/org_uber_go_zap-v1.27.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/zap/org_uber_go_zap-v1.27.0.zip",
        ],
    )
    go_repository(
        name = "tech_einride_go_aip",
        build_file_proto_mode = "disable_global",
        importpath = "go.einride.tech/aip",
        sha256 = "d3e11dca3b1aba4fed53d90c0a984eacf3aa3e47ef12b2e445ff8d5d185fe9db",
        strip_prefix = "go.einride.tech/aip@v0.66.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.einride.tech/aip/tech_einride_go_aip-v0.66.0.zip",
            "http://ats.apps.svc/gomod/go.einride.tech/aip/tech_einride_go_aip-v0.66.0.zip",
            "https://cache.hawkingrei.com/gomod/go.einride.tech/aip/tech_einride_go_aip-v0.66.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.einride.tech/aip/tech_einride_go_aip-v0.66.0.zip",
        ],
    )
    go_repository(
        name = "tools_gotest_v3",
        build_file_proto_mode = "disable_global",
        importpath = "gotest.tools/v3",
        sha256 = "9c1e4b8a1477c52441aafc2025a4b4e8bc300a9817c5549c0dc7fffef34bdaef",
        strip_prefix = "gotest.tools/v3@v3.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gotest.tools/v3/tools_gotest_v3-v3.0.3.zip",
            "http://ats.apps.svc/gomod/gotest.tools/v3/tools_gotest_v3-v3.0.3.zip",
            "https://cache.hawkingrei.com/gomod/gotest.tools/v3/tools_gotest_v3-v3.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gotest.tools/v3/tools_gotest_v3-v3.0.3.zip",
        ],
    )
