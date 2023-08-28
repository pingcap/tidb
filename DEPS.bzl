load("@bazel_gazelle//:deps.bzl", "go_repository")

def go_deps():
    # NOTE: We ensure that we pin to these specific dependencies by calling
    # this function FIRST, before calls to pull in dependencies for
    # third-party libraries (e.g. rules_go, gazelle, etc.)
    go_repository(
        name = "cc_mvdan_gofumpt",
        build_file_proto_mode = "disable_global",
        importpath = "mvdan.cc/gofumpt",
        sha256 = "d8b9add6f369b110907add4a62d924586a688c32245e6035775ff648c07f4f1d",
        strip_prefix = "mvdan.cc/gofumpt@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/mvdan.cc/gofumpt/cc_mvdan_gofumpt-v0.5.0.zip",
            "http://ats.apps.svc/gomod/mvdan.cc/gofumpt/cc_mvdan_gofumpt-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/mvdan.cc/gofumpt/cc_mvdan_gofumpt-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/mvdan.cc/gofumpt/cc_mvdan_gofumpt-v0.5.0.zip",
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
        sha256 = "2a86d723e07d2e28b79f56e1d2131705b8b19a4697a2b83280fa775eea120baf",
        strip_prefix = "mvdan.cc/unparam@v0.0.0-20221223090309-7455f1af531d",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/mvdan.cc/unparam/cc_mvdan_unparam-v0.0.0-20221223090309-7455f1af531d.zip",
            "http://ats.apps.svc/gomod/mvdan.cc/unparam/cc_mvdan_unparam-v0.0.0-20221223090309-7455f1af531d.zip",
            "https://cache.hawkingrei.com/gomod/mvdan.cc/unparam/cc_mvdan_unparam-v0.0.0-20221223090309-7455f1af531d.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/mvdan.cc/unparam/cc_mvdan_unparam-v0.0.0-20221223090309-7455f1af531d.zip",
        ],
    )
    go_repository(
        name = "co_honnef_go_tools",
        build_file_proto_mode = "disable_global",
        importpath = "honnef.co/go/tools",
        sha256 = "3f7c266a830f3a0727ac0b85cd7cd74a765c05d337d73af20906219f1a4ec4c3",
        strip_prefix = "honnef.co/go/tools@v0.4.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/honnef.co/go/tools/co_honnef_go_tools-v0.4.5.zip",
            "http://ats.apps.svc/gomod/honnef.co/go/tools/co_honnef_go_tools-v0.4.5.zip",
            "https://cache.hawkingrei.com/gomod/honnef.co/go/tools/co_honnef_go_tools-v0.4.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/honnef.co/go/tools/co_honnef_go_tools-v0.4.5.zip",
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
        sha256 = "c2c12312927c75ba64f7ed6338af29b10759d09e5cf990581df4de7bda727d08",
        strip_prefix = "github.com/4meepo/tagalign@v1.2.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.2.2.zip",
            "http://ats.apps.svc/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.2.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.2.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_abirdcfly_dupword",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Abirdcfly/dupword",
        sha256 = "05d0f75b3b4ce9e1f478eefc6e6996605f056a3c1a6a2175f0140e20dd26ce5b",
        strip_prefix = "github.com/Abirdcfly/dupword@v0.0.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Abirdcfly/dupword/com_github_abirdcfly_dupword-v0.0.11.zip",
            "http://ats.apps.svc/gomod/github.com/Abirdcfly/dupword/com_github_abirdcfly_dupword-v0.0.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Abirdcfly/dupword/com_github_abirdcfly_dupword-v0.0.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Abirdcfly/dupword/com_github_abirdcfly_dupword-v0.0.11.zip",
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
        name = "com_github_alecthomas_kingpin_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/kingpin/v2",
        sha256 = "8f330f35ca429b669c397cf2e6b58b8cd6894fb6d6437f7099931d2d1400b909",
        strip_prefix = "github.com/alecthomas/kingpin/v2@v2.3.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alecthomas/kingpin/v2/com_github_alecthomas_kingpin_v2-v2.3.2.zip",
            "http://ats.apps.svc/gomod/github.com/alecthomas/kingpin/v2/com_github_alecthomas_kingpin_v2-v2.3.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alecthomas/kingpin/v2/com_github_alecthomas_kingpin_v2-v2.3.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alecthomas/kingpin/v2/com_github_alecthomas_kingpin_v2-v2.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_alecthomas_template",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/template",
        sha256 = "25e3be7192932d130d0af31ce5bcddae887647ba4afcfb32009c3b9b79dbbdb3",
        strip_prefix = "github.com/alecthomas/template@v0.0.0-20190718012654-fb15b899a751",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alecthomas/template/com_github_alecthomas_template-v0.0.0-20190718012654-fb15b899a751.zip",
            "http://ats.apps.svc/gomod/github.com/alecthomas/template/com_github_alecthomas_template-v0.0.0-20190718012654-fb15b899a751.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alecthomas/template/com_github_alecthomas_template-v0.0.0-20190718012654-fb15b899a751.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alecthomas/template/com_github_alecthomas_template-v0.0.0-20190718012654-fb15b899a751.zip",
        ],
    )
    go_repository(
        name = "com_github_alecthomas_units",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/units",
        sha256 = "b62437d74a523089af46ba0115ece1ce11bca5e321fe1e1d4c976ecca6ee78aa",
        strip_prefix = "github.com/alecthomas/units@v0.0.0-20211218093645-b94a6e3cc137",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alecthomas/units/com_github_alecthomas_units-v0.0.0-20211218093645-b94a6e3cc137.zip",
            "http://ats.apps.svc/gomod/github.com/alecthomas/units/com_github_alecthomas_units-v0.0.0-20211218093645-b94a6e3cc137.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alecthomas/units/com_github_alecthomas_units-v0.0.0-20211218093645-b94a6e3cc137.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alecthomas/units/com_github_alecthomas_units-v0.0.0-20211218093645-b94a6e3cc137.zip",
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
        sha256 = "00104bccf0cd84066ed858faf0e529110769ad08823937306576b8cd2d8a2872",
        strip_prefix = "github.com/Antonboom/errname@v0.1.10",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Antonboom/errname/com_github_antonboom_errname-v0.1.10.zip",
            "http://ats.apps.svc/gomod/github.com/Antonboom/errname/com_github_antonboom_errname-v0.1.10.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Antonboom/errname/com_github_antonboom_errname-v0.1.10.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Antonboom/errname/com_github_antonboom_errname-v0.1.10.zip",
        ],
    )
    go_repository(
        name = "com_github_antonboom_nilnil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Antonboom/nilnil",
        sha256 = "8775b711b5b8fce6fb602864d963d814015665a54b0e34497e99ca497f157536",
        strip_prefix = "github.com/Antonboom/nilnil@v0.1.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Antonboom/nilnil/com_github_antonboom_nilnil-v0.1.5.zip",
            "http://ats.apps.svc/gomod/github.com/Antonboom/nilnil/com_github_antonboom_nilnil-v0.1.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Antonboom/nilnil/com_github_antonboom_nilnil-v0.1.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Antonboom/nilnil/com_github_antonboom_nilnil-v0.1.5.zip",
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
        sha256 = "95e53f518af5dd201a7efa773db81dfcf3077767f01c1943b00cdbfae355fb34",
        strip_prefix = "github.com/apache/thrift@v0.13.1-0.20201008052519-daf620915714",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/apache/thrift/com_github_apache_thrift-v0.13.1-0.20201008052519-daf620915714.zip",
            "http://ats.apps.svc/gomod/github.com/apache/thrift/com_github_apache_thrift-v0.13.1-0.20201008052519-daf620915714.zip",
            "https://cache.hawkingrei.com/gomod/github.com/apache/thrift/com_github_apache_thrift-v0.13.1-0.20201008052519-daf620915714.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/apache/thrift/com_github_apache_thrift-v0.13.1-0.20201008052519-daf620915714.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_circbuf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/circbuf",
        sha256 = "3819cde26cd4b25c4043dc9384da7b0c1c29fd06e6e3a38604f4a6933fc017ed",
        strip_prefix = "github.com/armon/circbuf@v0.0.0-20150827004946-bbbad097214e",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/armon/circbuf/com_github_armon_circbuf-v0.0.0-20150827004946-bbbad097214e.zip",
            "http://ats.apps.svc/gomod/github.com/armon/circbuf/com_github_armon_circbuf-v0.0.0-20150827004946-bbbad097214e.zip",
            "https://cache.hawkingrei.com/gomod/github.com/armon/circbuf/com_github_armon_circbuf-v0.0.0-20150827004946-bbbad097214e.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/armon/circbuf/com_github_armon_circbuf-v0.0.0-20150827004946-bbbad097214e.zip",
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
        sha256 = "247448464a8d219611279cde2540ef86de10828a5b2679477ded835adef351b1",
        strip_prefix = "github.com/armon/go-metrics@v0.0.0-20180917152333-f0300d1749da",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/armon/go-metrics/com_github_armon_go_metrics-v0.0.0-20180917152333-f0300d1749da.zip",
            "http://ats.apps.svc/gomod/github.com/armon/go-metrics/com_github_armon_go_metrics-v0.0.0-20180917152333-f0300d1749da.zip",
            "https://cache.hawkingrei.com/gomod/github.com/armon/go-metrics/com_github_armon_go_metrics-v0.0.0-20180917152333-f0300d1749da.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/armon/go-metrics/com_github_armon_go_metrics-v0.0.0-20180917152333-f0300d1749da.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_go_radix",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/go-radix",
        sha256 = "cb090b2b3c19987353e831ca79b31eb31eaa534b1f46d11b8813b235b1058859",
        strip_prefix = "github.com/armon/go-radix@v0.0.0-20180808171621-7fddfc383310",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/armon/go-radix/com_github_armon_go_radix-v0.0.0-20180808171621-7fddfc383310.zip",
            "http://ats.apps.svc/gomod/github.com/armon/go-radix/com_github_armon_go_radix-v0.0.0-20180808171621-7fddfc383310.zip",
            "https://cache.hawkingrei.com/gomod/github.com/armon/go-radix/com_github_armon_go_radix-v0.0.0-20180808171621-7fddfc383310.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/armon/go-radix/com_github_armon_go_radix-v0.0.0-20180808171621-7fddfc383310.zip",
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
        name = "com_github_ashanbrown_forbidigo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ashanbrown/forbidigo",
        sha256 = "0742635d0a4f168b096cfb072107cde160c0cf29a65005e36ba29f78b2c697c8",
        strip_prefix = "github.com/ashanbrown/forbidigo@v1.5.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ashanbrown/forbidigo/com_github_ashanbrown_forbidigo-v1.5.3.zip",
            "http://ats.apps.svc/gomod/github.com/ashanbrown/forbidigo/com_github_ashanbrown_forbidigo-v1.5.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ashanbrown/forbidigo/com_github_ashanbrown_forbidigo-v1.5.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ashanbrown/forbidigo/com_github_ashanbrown_forbidigo-v1.5.3.zip",
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
        sha256 = "8f1bd64c68621278523ecb27db81d4a0f1c1d5539f81f767b3d987778e78abcb",
        strip_prefix = "github.com/aws/aws-sdk-go@v1.44.259",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/aws/aws-sdk-go/com_github_aws_aws_sdk_go-v1.44.259.zip",
            "http://ats.apps.svc/gomod/github.com/aws/aws-sdk-go/com_github_aws_aws_sdk_go-v1.44.259.zip",
            "https://cache.hawkingrei.com/gomod/github.com/aws/aws-sdk-go/com_github_aws_aws_sdk_go-v1.44.259.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/aws/aws-sdk-go/com_github_aws_aws_sdk_go-v1.44.259.zip",
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
        name = "com_github_azure_azure_sdk_for_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go",
        sha256 = "a607abe933a75124c63e6b03141da5fa9cfb6db23691512f343f6013944c8673",
        strip_prefix = "github.com/Azure/azure-sdk-for-go@v23.2.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/com_github_azure_azure_sdk_for_go-v23.2.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/com_github_azure_azure_sdk_for_go-v23.2.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/com_github_azure_azure_sdk_for_go-v23.2.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/com_github_azure_azure_sdk_for_go-v23.2.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azcore",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azcore",
        sha256 = "94a9a4cee0462fac76fd5f89f1381f28fac3f1a05d59cea70f09745e55638a0c",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/azcore@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/azcore/com_github_azure_azure_sdk_for_go_sdk_azcore-v1.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/azcore/com_github_azure_azure_sdk_for_go_sdk_azcore-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/azcore/com_github_azure_azure_sdk_for_go_sdk_azcore-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/azcore/com_github_azure_azure_sdk_for_go_sdk_azcore-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azidentity",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azidentity",
        sha256 = "27947f13cb64475fd59e5d9f8b9c042b3d1e8603f49c54fc42820001c33d5f78",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/azidentity@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/azidentity/com_github_azure_azure_sdk_for_go_sdk_azidentity-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/azidentity/com_github_azure_azure_sdk_for_go_sdk_azidentity-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/azidentity/com_github_azure_azure_sdk_for_go_sdk_azidentity-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/azidentity/com_github_azure_azure_sdk_for_go_sdk_azidentity-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_internal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/internal",
        sha256 = "94617f0ed66bf541976b2360c5689bfb54e03e544adb4d1fe1e03e297b66b64b",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/internal@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/internal/com_github_azure_azure_sdk_for_go_sdk_internal-v1.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/internal/com_github_azure_azure_sdk_for_go_sdk_internal-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/internal/com_github_azure_azure_sdk_for_go_sdk_internal-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/internal/com_github_azure_azure_sdk_for_go_sdk_internal-v1.3.0.zip",
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
        name = "com_github_azure_go_autorest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest",
        sha256 = "295b69ffc36f1b89b3f58025b4d7d6c06cc3e2f0131a1666d49b5456611a3501",
        strip_prefix = "github.com/Azure/go-autorest@v11.2.8+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/go-autorest/com_github_azure_go_autorest-v11.2.8+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/go-autorest/com_github_azure_go_autorest-v11.2.8+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/go-autorest/com_github_azure_go_autorest-v11.2.8+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/go-autorest/com_github_azure_go_autorest-v11.2.8+incompatible.zip",
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
        sha256 = "303670915e2c0de9e6ed4658360ce5ae07320714c9a8228f0f2d69a12b8ddf5d",
        strip_prefix = "github.com/AzureAD/microsoft-authentication-library-for-go@v0.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/AzureAD/microsoft-authentication-library-for-go/com_github_azuread_microsoft_authentication_library_for_go-v0.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/AzureAD/microsoft-authentication-library-for-go/com_github_azuread_microsoft_authentication_library_for_go-v0.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/AzureAD/microsoft-authentication-library-for-go/com_github_azuread_microsoft_authentication_library_for_go-v0.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/AzureAD/microsoft-authentication-library-for-go/com_github_azuread_microsoft_authentication_library_for_go-v0.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_bazelbuild_buildtools",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bazelbuild/buildtools",
        sha256 = "7492d11e4458e1ed0675f34d9a6d236330e6d6657c549836ac342d997ae1f62b",
        strip_prefix = "github.com/bazelbuild/buildtools@v0.0.0-20230317132445-9c3c1fc0106e",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bazelbuild/buildtools/com_github_bazelbuild_buildtools-v0.0.0-20230317132445-9c3c1fc0106e.zip",
            "http://ats.apps.svc/gomod/github.com/bazelbuild/buildtools/com_github_bazelbuild_buildtools-v0.0.0-20230317132445-9c3c1fc0106e.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bazelbuild/buildtools/com_github_bazelbuild_buildtools-v0.0.0-20230317132445-9c3c1fc0106e.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bazelbuild/buildtools/com_github_bazelbuild_buildtools-v0.0.0-20230317132445-9c3c1fc0106e.zip",
        ],
    )
    go_repository(
        name = "com_github_bazelbuild_rules_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bazelbuild/rules_go",
        sha256 = "fb2bf42f1c6c6c7b07a50fecc8c993098476baead0cc197226d13da25604b9de",
        strip_prefix = "github.com/bazelbuild/rules_go@v0.40.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bazelbuild/rules_go/com_github_bazelbuild_rules_go-v0.40.0.zip",
            "http://ats.apps.svc/gomod/github.com/bazelbuild/rules_go/com_github_bazelbuild_rules_go-v0.40.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bazelbuild/rules_go/com_github_bazelbuild_rules_go-v0.40.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bazelbuild/rules_go/com_github_bazelbuild_rules_go-v0.40.0.zip",
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
        name = "com_github_bgentry_speakeasy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bgentry/speakeasy",
        sha256 = "d4bfd48b9bf68c87f92c94478ac910bcdab272e15eb909d58f1fb939233f75f0",
        strip_prefix = "github.com/bgentry/speakeasy@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bgentry/speakeasy/com_github_bgentry_speakeasy-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/bgentry/speakeasy/com_github_bgentry_speakeasy-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bgentry/speakeasy/com_github_bgentry_speakeasy-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bgentry/speakeasy/com_github_bgentry_speakeasy-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_biogo_store",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/biogo/store",
        sha256 = "26551f8829c5ada84a68ef240732375be6747252aba423cf5c88bc03002c3600",
        strip_prefix = "github.com/biogo/store@v0.0.0-20160505134755-913427a1d5e8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/biogo/store/com_github_biogo_store-v0.0.0-20160505134755-913427a1d5e8.zip",
            "http://ats.apps.svc/gomod/github.com/biogo/store/com_github_biogo_store-v0.0.0-20160505134755-913427a1d5e8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/biogo/store/com_github_biogo_store-v0.0.0-20160505134755-913427a1d5e8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/biogo/store/com_github_biogo_store-v0.0.0-20160505134755-913427a1d5e8.zip",
        ],
    )
    go_repository(
        name = "com_github_bketelsen_crypt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bketelsen/crypt",
        sha256 = "3df95e9bd6b8861009176bc5e4f5ebc6b0ff9857df6c1b3a8ece4fb595da02e7",
        strip_prefix = "github.com/bketelsen/crypt@v0.0.3-0.20200106085610-5cbc8cc4026c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bketelsen/crypt/com_github_bketelsen_crypt-v0.0.3-0.20200106085610-5cbc8cc4026c.zip",
            "http://ats.apps.svc/gomod/github.com/bketelsen/crypt/com_github_bketelsen_crypt-v0.0.3-0.20200106085610-5cbc8cc4026c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bketelsen/crypt/com_github_bketelsen_crypt-v0.0.3-0.20200106085610-5cbc8cc4026c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bketelsen/crypt/com_github_bketelsen_crypt-v0.0.3-0.20200106085610-5cbc8cc4026c.zip",
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
        name = "com_github_bombsimon_wsl_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bombsimon/wsl/v3",
        sha256 = "1fb4838d13bae3e003b9f30bf2736a8e61dc57b6e4b0aa7354e5cd6730df399f",
        strip_prefix = "github.com/bombsimon/wsl/v3@v3.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bombsimon/wsl/v3/com_github_bombsimon_wsl_v3-v3.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/bombsimon/wsl/v3/com_github_bombsimon_wsl_v3-v3.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bombsimon/wsl/v3/com_github_bombsimon_wsl_v3-v3.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bombsimon/wsl/v3/com_github_bombsimon_wsl_v3-v3.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_breml_bidichk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/breml/bidichk",
        sha256 = "c5a3b55eb8cf5e5d4839d7df414265c2a25fcd86e1377455ff95d6c059a38158",
        strip_prefix = "github.com/breml/bidichk@v0.2.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/breml/bidichk/com_github_breml_bidichk-v0.2.4.zip",
            "http://ats.apps.svc/gomod/github.com/breml/bidichk/com_github_breml_bidichk-v0.2.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/breml/bidichk/com_github_breml_bidichk-v0.2.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/breml/bidichk/com_github_breml_bidichk-v0.2.4.zip",
        ],
    )
    go_repository(
        name = "com_github_breml_errchkjson",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/breml/errchkjson",
        sha256 = "51ffd04e0d0fb51880ea0f89b50256c166c88c98d7772990152b4247f764bcf4",
        strip_prefix = "github.com/breml/errchkjson@v0.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/breml/errchkjson/com_github_breml_errchkjson-v0.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/breml/errchkjson/com_github_breml_errchkjson-v0.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/breml/errchkjson/com_github_breml_errchkjson-v0.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/breml/errchkjson/com_github_breml_errchkjson-v0.3.1.zip",
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
        sha256 = "b7112c147f4c1094d9d44fdc695990ed4cdfba1d72b9f1427a86734a3ee544f6",
        strip_prefix = "github.com/butuzov/ireturn@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/butuzov/ireturn/com_github_butuzov_ireturn-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/butuzov/ireturn/com_github_butuzov_ireturn-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/butuzov/ireturn/com_github_butuzov_ireturn-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/butuzov/ireturn/com_github_butuzov_ireturn-v0.2.0.zip",
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
        name = "com_github_cenk_backoff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cenk/backoff",
        sha256 = "ac042b9692e3f0f8bd3b6a5d17443905e4c02ab84067650a96c0d43e5ea08138",
        strip_prefix = "github.com/cenk/backoff@v2.0.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cenk/backoff/com_github_cenk_backoff-v2.0.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/cenk/backoff/com_github_cenk_backoff-v2.0.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cenk/backoff/com_github_cenk_backoff-v2.0.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cenk/backoff/com_github_cenk_backoff-v2.0.0+incompatible.zip",
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
        name = "com_github_certifi_gocertifi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/certifi/gocertifi",
        sha256 = "11d525844c3dd711fb0ae31acc9ebd8a4d602215f14ff24ad1764ecb48464849",
        strip_prefix = "github.com/certifi/gocertifi@v0.0.0-20200922220541-2c3bb06c6054",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/certifi/gocertifi/com_github_certifi_gocertifi-v0.0.0-20200922220541-2c3bb06c6054.zip",
            "http://ats.apps.svc/gomod/github.com/certifi/gocertifi/com_github_certifi_gocertifi-v0.0.0-20200922220541-2c3bb06c6054.zip",
            "https://cache.hawkingrei.com/gomod/github.com/certifi/gocertifi/com_github_certifi_gocertifi-v0.0.0-20200922220541-2c3bb06c6054.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/certifi/gocertifi/com_github_certifi_gocertifi-v0.0.0-20200922220541-2c3bb06c6054.zip",
        ],
    )
    go_repository(
        name = "com_github_cespare_xxhash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cespare/xxhash",
        sha256 = "fe98c56670b21631f7fd3305a29a3b17e86a6cce3876a2119460717a18538e2e",
        strip_prefix = "github.com/cespare/xxhash@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cespare/xxhash/com_github_cespare_xxhash-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/cespare/xxhash/com_github_cespare_xxhash-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cespare/xxhash/com_github_cespare_xxhash-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cespare/xxhash/com_github_cespare_xxhash-v1.1.0.zip",
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
        sha256 = "3fd289252a687a412e11f3b99670cb877740ecefa6555bc56c985a907fed58d3",
        strip_prefix = "github.com/chavacava/garif@v0.0.0-20230519080132-4752330f72df",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/chavacava/garif/com_github_chavacava_garif-v0.0.0-20230519080132-4752330f72df.zip",
            "http://ats.apps.svc/gomod/github.com/chavacava/garif/com_github_chavacava_garif-v0.0.0-20230519080132-4752330f72df.zip",
            "https://cache.hawkingrei.com/gomod/github.com/chavacava/garif/com_github_chavacava_garif-v0.0.0-20230519080132-4752330f72df.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/chavacava/garif/com_github_chavacava_garif-v0.0.0-20230519080132-4752330f72df.zip",
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
        sha256 = "3dc842677887278fb33d25078d375ae6a7a94bb77a8d205ee2230b581b6947a6",
        strip_prefix = "github.com/chzyer/readline@v0.0.0-20180603132655-2972be24d48e",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/chzyer/readline/com_github_chzyer_readline-v0.0.0-20180603132655-2972be24d48e.zip",
            "http://ats.apps.svc/gomod/github.com/chzyer/readline/com_github_chzyer_readline-v0.0.0-20180603132655-2972be24d48e.zip",
            "https://cache.hawkingrei.com/gomod/github.com/chzyer/readline/com_github_chzyer_readline-v0.0.0-20180603132655-2972be24d48e.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/chzyer/readline/com_github_chzyer_readline-v0.0.0-20180603132655-2972be24d48e.zip",
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
        sha256 = "7e33dbf929da89661e8f7507706f7ea28762d7c48c899d8e8352145c11627bf4",
        strip_prefix = "github.com/cncf/xds/go@v0.0.0-20230105202645-06c439db220b",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cncf/xds/go/com_github_cncf_xds_go-v0.0.0-20230105202645-06c439db220b.zip",
            "http://ats.apps.svc/gomod/github.com/cncf/xds/go/com_github_cncf_xds_go-v0.0.0-20230105202645-06c439db220b.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cncf/xds/go/com_github_cncf_xds_go-v0.0.0-20230105202645-06c439db220b.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cncf/xds/go/com_github_cncf_xds_go-v0.0.0-20230105202645-06c439db220b.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_apd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/apd",
        sha256 = "fef7ec2fae220f84bfacb17fbfc1b04a666ab7f6fc04f3ff6d2b1e05c380777d",
        strip_prefix = "github.com/cockroachdb/apd@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/apd/com_github_cockroachdb_apd-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/apd/com_github_cockroachdb_apd-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/apd/com_github_cockroachdb_apd-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/apd/com_github_cockroachdb_apd-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_cmux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/cmux",
        sha256 = "88f6f9cf33eb535658540b46f6222f029398e590a3ff9cc873d7d561ac6debf0",
        strip_prefix = "github.com/cockroachdb/cmux@v0.0.0-20170110192607-30d10be49292",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/cmux/com_github_cockroachdb_cmux-v0.0.0-20170110192607-30d10be49292.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/cmux/com_github_cockroachdb_cmux-v0.0.0-20170110192607-30d10be49292.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/cmux/com_github_cockroachdb_cmux-v0.0.0-20170110192607-30d10be49292.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/cmux/com_github_cockroachdb_cmux-v0.0.0-20170110192607-30d10be49292.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_cockroach",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/cockroach",
        sha256 = "0c0590292ad16788da3ebe10bd407e2441a201391a24a681838c2aa26f01b991",
        strip_prefix = "github.com/cockroachdb/cockroach@v0.0.0-20170608034007-84bc9597164f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/cockroach/com_github_cockroachdb_cockroach-v0.0.0-20170608034007-84bc9597164f.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/cockroach/com_github_cockroachdb_cockroach-v0.0.0-20170608034007-84bc9597164f.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/cockroach/com_github_cockroachdb_cockroach-v0.0.0-20170608034007-84bc9597164f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/cockroach/com_github_cockroachdb_cockroach-v0.0.0-20170608034007-84bc9597164f.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_cockroach_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/cockroach-go",
        sha256 = "cbc7e32c0af7ddd88afe6ef57b45481495322cb4c973f4928bc753c8be05f1a6",
        strip_prefix = "github.com/cockroachdb/cockroach-go@v0.0.0-20181001143604-e0a95dfd547c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/cockroach-go/com_github_cockroachdb_cockroach_go-v0.0.0-20181001143604-e0a95dfd547c.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/cockroach-go/com_github_cockroachdb_cockroach_go-v0.0.0-20181001143604-e0a95dfd547c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/cockroach-go/com_github_cockroachdb_cockroach_go-v0.0.0-20181001143604-e0a95dfd547c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/cockroach-go/com_github_cockroachdb_cockroach_go-v0.0.0-20181001143604-e0a95dfd547c.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_datadriven",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/datadriven",
        sha256 = "27661be7dc3cff4288f9a150f7e82fad6bb53382bb8d87bcfe8b22a85732c414",
        strip_prefix = "github.com/cockroachdb/datadriven@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/datadriven/com_github_cockroachdb_datadriven-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/datadriven/com_github_cockroachdb_datadriven-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/datadriven/com_github_cockroachdb_datadriven-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/datadriven/com_github_cockroachdb_datadriven-v1.0.0.zip",
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
        name = "com_github_coreos_bbolt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/bbolt",
        sha256 = "097e7c6cf2dc9c50a0c8827f451bd3cba44c2cbf086d4fb684f2dfada9bfa841",
        strip_prefix = "github.com/coreos/bbolt@v1.3.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/bbolt/com_github_coreos_bbolt-v1.3.2.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/bbolt/com_github_coreos_bbolt-v1.3.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/bbolt/com_github_coreos_bbolt-v1.3.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/bbolt/com_github_coreos_bbolt-v1.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_etcd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/etcd",
        sha256 = "c32b3fc5dba0eeb8533d628489cf862c4eb360644d79c597bcc6290f3d74b046",
        strip_prefix = "github.com/coreos/etcd@v3.3.13+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/etcd/com_github_coreos_etcd-v3.3.13+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/etcd/com_github_coreos_etcd-v3.3.13+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/etcd/com_github_coreos_etcd-v3.3.13+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/etcd/com_github_coreos_etcd-v3.3.13+incompatible.zip",
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
        sha256 = "b2fc075395ffc34cff4b964681d0ae3cd22096cfcadd2970eeaa877596ceb210",
        strip_prefix = "github.com/coreos/go-semver@v0.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/go-semver/com_github_coreos_go_semver-v0.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/go-semver/com_github_coreos_go_semver-v0.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/go-semver/com_github_coreos_go_semver-v0.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/go-semver/com_github_coreos_go_semver-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_systemd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-systemd",
        sha256 = "cd349df002e0900cd0a5f9648720621840164c4b530f3e3457510e7e08589307",
        strip_prefix = "github.com/coreos/go-systemd@v0.0.0-20190321100706-95778dfbb74e",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/go-systemd/com_github_coreos_go_systemd-v0.0.0-20190321100706-95778dfbb74e.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/go-systemd/com_github_coreos_go_systemd-v0.0.0-20190321100706-95778dfbb74e.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/go-systemd/com_github_coreos_go_systemd-v0.0.0-20190321100706-95778dfbb74e.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/go-systemd/com_github_coreos_go_systemd-v0.0.0-20190321100706-95778dfbb74e.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_systemd_v22",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-systemd/v22",
        sha256 = "01134ae89bf4a91c17eeb1f8425e1064f9bde64cf3ce0c9cf546a9fa1ee25e64",
        strip_prefix = "github.com/coreos/go-systemd/v22@v22.3.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/go-systemd/v22/com_github_coreos_go_systemd_v22-v22.3.2.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/go-systemd/v22/com_github_coreos_go_systemd_v22-v22.3.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/go-systemd/v22/com_github_coreos_go_systemd_v22-v22.3.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/go-systemd/v22/com_github_coreos_go_systemd_v22-v22.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_pkg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/pkg",
        sha256 = "7fe161d49439a9b4136c932233cb4b803b9e3ac7ee46f39ce247defc4f4ea8d7",
        strip_prefix = "github.com/coreos/pkg@v0.0.0-20180928190104-399ea9e2e55f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/coreos/pkg/com_github_coreos_pkg-v0.0.0-20180928190104-399ea9e2e55f.zip",
            "http://ats.apps.svc/gomod/github.com/coreos/pkg/com_github_coreos_pkg-v0.0.0-20180928190104-399ea9e2e55f.zip",
            "https://cache.hawkingrei.com/gomod/github.com/coreos/pkg/com_github_coreos_pkg-v0.0.0-20180928190104-399ea9e2e55f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/coreos/pkg/com_github_coreos_pkg-v0.0.0-20180928190104-399ea9e2e55f.zip",
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
        sha256 = "70a7e609809cf2a92c5535104db5eb82d75c54bfcfed2d224e87dd2fd9729f62",
        strip_prefix = "github.com/cpuguy83/go-md2man/v2@v2.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cpuguy83/go-md2man/v2/com_github_cpuguy83_go_md2man_v2-v2.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/cpuguy83/go-md2man/v2/com_github_cpuguy83_go_md2man_v2-v2.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cpuguy83/go-md2man/v2/com_github_cpuguy83_go_md2man_v2-v2.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cpuguy83/go-md2man/v2/com_github_cpuguy83_go_md2man_v2-v2.0.2.zip",
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
        sha256 = "77a220852b41432c0b81aab22d95bc929c9c0092ac294b4dded0960a7705430e",
        strip_prefix = "github.com/daixiang0/gci@v0.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/daixiang0/gci/com_github_daixiang0_gci-v0.11.0.zip",
            "http://ats.apps.svc/gomod/github.com/daixiang0/gci/com_github_daixiang0_gci-v0.11.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/daixiang0/gci/com_github_daixiang0_gci-v0.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/daixiang0/gci/com_github_daixiang0_gci-v0.11.0.zip",
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
        sha256 = "6b44a843951f371b7010c754ecc3cabefe815d5ced1c5b9409fb2d697e8a890d",
        strip_prefix = "github.com/davecgh/go-spew@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/davecgh/go-spew/com_github_davecgh_go_spew-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/davecgh/go-spew/com_github_davecgh_go_spew-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/davecgh/go-spew/com_github_davecgh_go_spew-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/davecgh/go-spew/com_github_davecgh_go_spew-v1.1.1.zip",
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
        name = "com_github_dgryski_go_sip13",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgryski/go-sip13",
        sha256 = "aa6f56ab1d40952c536786beb834d9e361ef796bd7aefb7d6997e1753d4058eb",
        strip_prefix = "github.com/dgryski/go-sip13@v0.0.0-20181026042036-e10d5fee7954",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dgryski/go-sip13/com_github_dgryski_go_sip13-v0.0.0-20181026042036-e10d5fee7954.zip",
            "http://ats.apps.svc/gomod/github.com/dgryski/go-sip13/com_github_dgryski_go_sip13-v0.0.0-20181026042036-e10d5fee7954.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dgryski/go-sip13/com_github_dgryski_go_sip13-v0.0.0-20181026042036-e10d5fee7954.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dgryski/go-sip13/com_github_dgryski_go_sip13-v0.0.0-20181026042036-e10d5fee7954.zip",
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
        sha256 = "d6d94a1c8471809db30c2979add32bac647120bc577ea30f7e8fcc06436483f0",
        strip_prefix = "github.com/dnaeon/go-vcr@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dnaeon/go-vcr/com_github_dnaeon_go_vcr-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/dnaeon/go-vcr/com_github_dnaeon_go_vcr-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dnaeon/go-vcr/com_github_dnaeon_go_vcr-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dnaeon/go-vcr/com_github_dnaeon_go_vcr-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_go_units",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/go-units",
        sha256 = "0f2be7dce7b1a0ba6a4a786eb144a3398e9a61afc0eec5799a1520d9906fc58c",
        strip_prefix = "github.com/docker/go-units@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/docker/go-units/com_github_docker_go_units-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/docker/go-units/com_github_docker_go_units-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/docker/go-units/com_github_docker_go_units-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/docker/go-units/com_github_docker_go_units-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dustin_go_humanize",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dustin/go-humanize",
        sha256 = "e01916e082a6646ea12d7800d77af43045c27284ff2a0a77e3484509989cc107",
        strip_prefix = "github.com/dustin/go-humanize@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/dustin/go-humanize/com_github_dustin_go_humanize-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/dustin/go-humanize/com_github_dustin_go_humanize-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/dustin/go-humanize/com_github_dustin_go_humanize-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/dustin/go-humanize/com_github_dustin_go_humanize-v1.0.0.zip",
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
        name = "com_github_elastic_gosigar",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/elastic/gosigar",
        sha256 = "21ddafe5b9912113983e4210aa48f12a87d3a88a0a6946601e65725b736ca852",
        strip_prefix = "github.com/elastic/gosigar@v0.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/elastic/gosigar/com_github_elastic_gosigar-v0.9.0.zip",
            "http://ats.apps.svc/gomod/github.com/elastic/gosigar/com_github_elastic_gosigar-v0.9.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/elastic/gosigar/com_github_elastic_gosigar-v0.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/elastic/gosigar/com_github_elastic_gosigar-v0.9.0.zip",
        ],
    )
    go_repository(
        name = "com_github_elazarl_go_bindata_assetfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/elazarl/go-bindata-assetfs",
        sha256 = "3aa225ae5ae4a8059a671fa656d8567f09861f88b88dbef9e06a291efd90013a",
        strip_prefix = "github.com/elazarl/go-bindata-assetfs@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/elazarl/go-bindata-assetfs/com_github_elazarl_go_bindata_assetfs-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/elazarl/go-bindata-assetfs/com_github_elazarl_go_bindata_assetfs-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/elazarl/go-bindata-assetfs/com_github_elazarl_go_bindata_assetfs-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/elazarl/go-bindata-assetfs/com_github_elazarl_go_bindata_assetfs-v1.0.0.zip",
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
        sha256 = "aa0530fbbbe2d4683035547b14d58a7318f408398e10092637f20642de82c9ff",
        strip_prefix = "github.com/envoyproxy/go-control-plane@v0.10.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/envoyproxy/go-control-plane/com_github_envoyproxy_go_control_plane-v0.10.3.zip",
            "http://ats.apps.svc/gomod/github.com/envoyproxy/go-control-plane/com_github_envoyproxy_go_control_plane-v0.10.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/envoyproxy/go-control-plane/com_github_envoyproxy_go_control_plane-v0.10.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/envoyproxy/go-control-plane/com_github_envoyproxy_go_control_plane-v0.10.3.zip",
        ],
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sha256 = "7ca5aeb463c05869073076ec25ccc4144edd41d48971f1b5fd8cec1bf12a0d48",
        strip_prefix = "github.com/envoyproxy/protoc-gen-validate@v0.9.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/envoyproxy/protoc-gen-validate/com_github_envoyproxy_protoc_gen_validate-v0.9.1.zip",
            "http://ats.apps.svc/gomod/github.com/envoyproxy/protoc-gen-validate/com_github_envoyproxy_protoc_gen_validate-v0.9.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/envoyproxy/protoc-gen-validate/com_github_envoyproxy_protoc_gen_validate-v0.9.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/envoyproxy/protoc-gen-validate/com_github_envoyproxy_protoc_gen_validate-v0.9.1.zip",
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
        sha256 = "16b7637e6ddf7dcfd1de2f53447de7605007b3d9cfc0904d758eff0d12d7aff2",
        strip_prefix = "github.com/ettle/strcase@v0.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ettle/strcase/com_github_ettle_strcase-v0.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/ettle/strcase/com_github_ettle_strcase-v0.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ettle/strcase/com_github_ettle_strcase-v0.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ettle/strcase/com_github_ettle_strcase-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_evanphx_json_patch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/evanphx/json-patch",
        sha256 = "dd873b8aea0a9546ae2adf65dd112882ddc06f6c28a9a9eea3512db7dfea3496",
        strip_prefix = "github.com/evanphx/json-patch@v4.12.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/evanphx/json-patch/com_github_evanphx_json_patch-v4.12.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/evanphx/json-patch/com_github_evanphx_json_patch-v4.12.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/evanphx/json-patch/com_github_evanphx_json_patch-v4.12.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/evanphx/json-patch/com_github_evanphx_json_patch-v4.12.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_facebookgo_clock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/facebookgo/clock",
        sha256 = "5d6b671bd5afef8459fb7561d19bcf7c7f378da9943722d36676735b3c6272fa",
        strip_prefix = "github.com/facebookgo/clock@v0.0.0-20150410010913-600d898af40a",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/facebookgo/clock/com_github_facebookgo_clock-v0.0.0-20150410010913-600d898af40a.zip",
            "http://ats.apps.svc/gomod/github.com/facebookgo/clock/com_github_facebookgo_clock-v0.0.0-20150410010913-600d898af40a.zip",
            "https://cache.hawkingrei.com/gomod/github.com/facebookgo/clock/com_github_facebookgo_clock-v0.0.0-20150410010913-600d898af40a.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/facebookgo/clock/com_github_facebookgo_clock-v0.0.0-20150410010913-600d898af40a.zip",
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
        sha256 = "5aa7c7c5e0f12febbeb3f4f57fbca13eabf16caa1cd5a89315e7b8e187652ad9",
        strip_prefix = "github.com/fatih/color@v1.15.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fatih/color/com_github_fatih_color-v1.15.0.zip",
            "http://ats.apps.svc/gomod/github.com/fatih/color/com_github_fatih_color-v1.15.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fatih/color/com_github_fatih_color-v1.15.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fatih/color/com_github_fatih_color-v1.15.0.zip",
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
        name = "com_github_felixge_httpsnoop",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/felixge/httpsnoop",
        sha256 = "b345e22aa5ff8c496e6c5b8aed355ac47eb3ce631361782065e0cfdcab1b54ac",
        strip_prefix = "github.com/felixge/httpsnoop@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/felixge/httpsnoop/com_github_felixge_httpsnoop-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/felixge/httpsnoop/com_github_felixge_httpsnoop-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/felixge/httpsnoop/com_github_felixge_httpsnoop-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/felixge/httpsnoop/com_github_felixge_httpsnoop-v1.0.2.zip",
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
        name = "com_github_form3tech_oss_jwt_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/form3tech-oss/jwt-go",
        sha256 = "ebe8386761761d53fac2de5f8f575ddf66c114ec9835947c761131662f1d38f3",
        strip_prefix = "github.com/form3tech-oss/jwt-go@v3.2.6-0.20210809144907-32ab6a8243d7+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/form3tech-oss/jwt-go/com_github_form3tech_oss_jwt_go-v3.2.5+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/form3tech-oss/jwt-go/com_github_form3tech_oss_jwt_go-v3.2.5+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/form3tech-oss/jwt-go/com_github_form3tech_oss_jwt_go-v3.2.5+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/form3tech-oss/jwt-go/com_github_form3tech_oss_jwt_go-v3.2.5+incompatible.zip",
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
        sha256 = "7d4408f12ffc38106e358244446851a28077ed80e3c0940e98e0e332b3ed43ab",
        strip_prefix = "github.com/fsnotify/fsnotify@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/fsnotify/fsnotify/com_github_fsnotify_fsnotify-v1.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/fsnotify/fsnotify/com_github_fsnotify_fsnotify-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/fsnotify/fsnotify/com_github_fsnotify_fsnotify-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/fsnotify/fsnotify/com_github_fsnotify_fsnotify-v1.6.0.zip",
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
        name = "com_github_gaijinentertainment_go_exhaustruct_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/GaijinEntertainment/go-exhaustruct/v2",
        sha256 = "a986daa48826c4aa5ecf6d8c28e40543029f75e7f295bbce8758f5391ab24c30",
        strip_prefix = "github.com/GaijinEntertainment/go-exhaustruct/v2@v2.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/GaijinEntertainment/go-exhaustruct/v2/com_github_gaijinentertainment_go_exhaustruct_v2-v2.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/GaijinEntertainment/go-exhaustruct/v2/com_github_gaijinentertainment_go_exhaustruct_v2-v2.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/GaijinEntertainment/go-exhaustruct/v2/com_github_gaijinentertainment_go_exhaustruct_v2-v2.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/GaijinEntertainment/go-exhaustruct/v2/com_github_gaijinentertainment_go_exhaustruct_v2-v2.3.0.zip",
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
        name = "com_github_getsentry_raven_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/getsentry/raven-go",
        sha256 = "eaffe69939612cd05f95e1846b8ddb4043655571be34cdb6412a66b41b6826eb",
        strip_prefix = "github.com/getsentry/raven-go@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/getsentry/raven-go/com_github_getsentry_raven_go-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/getsentry/raven-go/com_github_getsentry_raven_go-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/getsentry/raven-go/com_github_getsentry_raven_go-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/getsentry/raven-go/com_github_getsentry_raven_go-v0.2.0.zip",
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
        sha256 = "cf7b586c837066c441d20f8174ec224a935aab15015e09a607b103df0c800f68",
        strip_prefix = "github.com/go-critic/go-critic@v0.8.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-critic/go-critic/com_github_go_critic_go_critic-v0.8.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-critic/go-critic/com_github_go_critic_go_critic-v0.8.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-critic/go-critic/com_github_go_critic_go_critic-v0.8.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-critic/go-critic/com_github_go_critic_go_critic-v0.8.1.zip",
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
        name = "com_github_go_ini_ini",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-ini/ini",
        sha256 = "2ec52de9f1c96133e9f81b8250fdc99ca0729c0d429e318d7c8836b7a6ba5f60",
        strip_prefix = "github.com/go-ini/ini@v1.25.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-ini/ini/com_github_go_ini_ini-v1.25.4.zip",
            "http://ats.apps.svc/gomod/github.com/go-ini/ini/com_github_go_ini_ini-v1.25.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-ini/ini/com_github_go_ini_ini-v1.25.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-ini/ini/com_github_go_ini_ini-v1.25.4.zip",
        ],
    )
    go_repository(
        name = "com_github_go_kit_kit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-kit/kit",
        sha256 = "f3da9b35b100dd32e7b10c37a0630af60d54afa37c61291e7df94bc0ac31ed03",
        strip_prefix = "github.com/go-kit/kit@v0.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-kit/kit/com_github_go_kit_kit-v0.9.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-kit/kit/com_github_go_kit_kit-v0.9.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-kit/kit/com_github_go_kit_kit-v0.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-kit/kit/com_github_go_kit_kit-v0.9.0.zip",
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
        sha256 = "9e030cd09b584e59a2f5baaa24cf600520757d732af0f8993cc412dd3086703a",
        strip_prefix = "github.com/go-logfmt/logfmt@v0.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-logfmt/logfmt/com_github_go_logfmt_logfmt-v0.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-logfmt/logfmt/com_github_go_logfmt_logfmt-v0.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-logfmt/logfmt/com_github_go_logfmt_logfmt-v0.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-logfmt/logfmt/com_github_go_logfmt_logfmt-v0.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_logr_logr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logr/logr",
        sha256 = "4b4b79b5863ab1d35c329d34a9cbba2a58a9b83b4a33c426facd2aa73b132f04",
        strip_prefix = "github.com/go-logr/logr@v1.2.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-logr/logr/com_github_go_logr_logr-v1.2.3.zip",
            "http://ats.apps.svc/gomod/github.com/go-logr/logr/com_github_go_logr_logr-v1.2.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-logr/logr/com_github_go_logr_logr-v1.2.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-logr/logr/com_github_go_logr_logr-v1.2.3.zip",
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
        sha256 = "95b192df81ca16f0fb7d2d98ff6596d70256d73e49e899c55fabd511fd6768ef",
        strip_prefix = "github.com/go-ole/go-ole@v1.2.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-ole/go-ole/com_github_go_ole_go_ole-v1.2.6.zip",
            "http://ats.apps.svc/gomod/github.com/go-ole/go-ole/com_github_go_ole_go_ole-v1.2.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-ole/go-ole/com_github_go_ole_go_ole-v1.2.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-ole/go-ole/com_github_go_ole_go_ole-v1.2.6.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_jsonpointer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonpointer",
        sha256 = "13184359227db1ba359382af560d5c32c8b08c0aabfb13cfcb332700a7f01913",
        strip_prefix = "github.com/go-openapi/jsonpointer@v0.19.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/jsonpointer/com_github_go_openapi_jsonpointer-v0.19.6.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/jsonpointer/com_github_go_openapi_jsonpointer-v0.19.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/jsonpointer/com_github_go_openapi_jsonpointer-v0.19.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/jsonpointer/com_github_go_openapi_jsonpointer-v0.19.6.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_jsonreference",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonreference",
        sha256 = "9bd65d3ea1d52937248fa19dabdf3619f2017328cfd758818668d9694985b32f",
        strip_prefix = "github.com/go-openapi/jsonreference@v0.20.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/jsonreference/com_github_go_openapi_jsonreference-v0.20.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/jsonreference/com_github_go_openapi_jsonreference-v0.20.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/jsonreference/com_github_go_openapi_jsonreference-v0.20.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/jsonreference/com_github_go_openapi_jsonreference-v0.20.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_swag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/swag",
        sha256 = "200aad3e5eed2c395172b822650575d994b84345a2bbc7789e336545634cac09",
        strip_prefix = "github.com/go-openapi/swag@v0.22.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/swag/com_github_go_openapi_swag-v0.22.3.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/swag/com_github_go_openapi_swag-v0.22.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/swag/com_github_go_openapi_swag-v0.22.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/swag/com_github_go_openapi_swag-v0.22.3.zip",
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
        sha256 = "a0bb8b3e4aa7c75e47a3fe505f39aa6b57d339c1adf00149c0193a25f1cc5703",
        strip_prefix = "github.com/go-task/slim-sprig@v0.0.0-20210107165309-348f09dbbbc0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-task/slim-sprig/com_github_go_task_slim_sprig-v0.0.0-20210107165309-348f09dbbbc0.zip",
            "http://ats.apps.svc/gomod/github.com/go-task/slim-sprig/com_github_go_task_slim_sprig-v0.0.0-20210107165309-348f09dbbbc0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-task/slim-sprig/com_github_go_task_slim_sprig-v0.0.0-20210107165309-348f09dbbbc0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-task/slim-sprig/com_github_go_task_slim_sprig-v0.0.0-20210107165309-348f09dbbbc0.zip",
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
        sha256 = "f47a54b0b9f84cbf643c5fdeef57d4c1f0966ec72eb2199121e0f2c6e2b01238",
        strip_prefix = "github.com/go-toolsmith/astequal@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-toolsmith/astequal/com_github_go_toolsmith_astequal-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-toolsmith/astequal/com_github_go_toolsmith_astequal-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-toolsmith/astequal/com_github_go_toolsmith_astequal-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-toolsmith/astequal/com_github_go_toolsmith_astequal-v1.1.0.zip",
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
        sha256 = "5a43ed4a7cd2b063b634f0df5311c0dfa6576683bfc1339f2c5b1b1127fc392b",
        strip_prefix = "github.com/gobwas/httphead@v0.0.0-20180130184737-2c6c146eadee",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gobwas/httphead/com_github_gobwas_httphead-v0.0.0-20180130184737-2c6c146eadee.zip",
            "http://ats.apps.svc/gomod/github.com/gobwas/httphead/com_github_gobwas_httphead-v0.0.0-20180130184737-2c6c146eadee.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gobwas/httphead/com_github_gobwas_httphead-v0.0.0-20180130184737-2c6c146eadee.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gobwas/httphead/com_github_gobwas_httphead-v0.0.0-20180130184737-2c6c146eadee.zip",
        ],
    )
    go_repository(
        name = "com_github_gobwas_pool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/pool",
        sha256 = "52604b1456b92bb310461167a3e6515562f0f4214f01ed6440e3105f78be188f",
        strip_prefix = "github.com/gobwas/pool@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gobwas/pool/com_github_gobwas_pool-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/gobwas/pool/com_github_gobwas_pool-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gobwas/pool/com_github_gobwas_pool-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gobwas/pool/com_github_gobwas_pool-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gobwas_ws",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/ws",
        sha256 = "f9e5c26e83278f19958c68be7b76ad6711c806b6dae766fad7692d2af867bedd",
        strip_prefix = "github.com/gobwas/ws@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gobwas/ws/com_github_gobwas_ws-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/gobwas/ws/com_github_gobwas_ws-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gobwas/ws/com_github_gobwas_ws-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gobwas/ws/com_github_gobwas_ws-v1.0.2.zip",
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
        sha256 = "668beb5dd923378b00fda4ba0d965000f3f259be5ba05ebd341a2949e8f20db6",
        strip_prefix = "github.com/golang/glog@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang/glog/com_github_golang_glog-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/golang/glog/com_github_golang_glog-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang/glog/com_github_golang_glog-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang/glog/com_github_golang_glog-v1.1.0.zip",
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
        sha256 = "bea2e7c045b07f50b60211bee94b62c442322ded7fa893e3fda49dcdce0e2908",
        strip_prefix = "github.com/golang-jwt/jwt/v4@v4.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang-jwt/jwt/v4/com_github_golang_jwt_jwt_v4-v4.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/golang-jwt/jwt/v4/com_github_golang_jwt_jwt_v4-v4.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang-jwt/jwt/v4/com_github_golang_jwt_jwt_v4-v4.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang-jwt/jwt/v4/com_github_golang_jwt_jwt_v4-v4.2.0.zip",
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
        sha256 = "93bda6e88d4a0a493a98b481de67a10000a755d15f16a800b49a6b96d1bd6f81",
        strip_prefix = "github.com/golang/protobuf@v1.5.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golang/protobuf/com_github_golang_protobuf-v1.5.3.zip",
            "http://ats.apps.svc/gomod/github.com/golang/protobuf/com_github_golang_protobuf-v1.5.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golang/protobuf/com_github_golang_protobuf-v1.5.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golang/protobuf/com_github_golang_protobuf-v1.5.3.zip",
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
        sha256 = "a63a454321bbe42e183f26af761619abf8a719f94509a9a45e31a223e9ea0e8b",
        strip_prefix = "github.com/golangci/gofmt@v0.0.0-20220901101216-f2edd75033f2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/gofmt/com_github_golangci_gofmt-v0.0.0-20220901101216-f2edd75033f2.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/gofmt/com_github_golangci_gofmt-v0.0.0-20220901101216-f2edd75033f2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/gofmt/com_github_golangci_gofmt-v0.0.0-20220901101216-f2edd75033f2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/gofmt/com_github_golangci_gofmt-v0.0.0-20220901101216-f2edd75033f2.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_golangci_lint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/golangci-lint",
        sha256 = "3ec6861b33364182de20466c4745f463dfd9ce64f7d0d3d75196d3ad21152e08",
        strip_prefix = "github.com/golangci/golangci-lint@v1.53.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/golangci-lint/com_github_golangci_golangci_lint-v1.53.3.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/golangci-lint/com_github_golangci_golangci_lint-v1.53.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/golangci-lint/com_github_golangci_golangci_lint-v1.53.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/golangci-lint/com_github_golangci_golangci_lint-v1.53.3.zip",
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
        sha256 = "6b48c93d2d09497940671e66dd1793648ce76286fec0f368764422765df7a153",
        strip_prefix = "github.com/golangci/misspell@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/misspell/com_github_golangci_misspell-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/misspell/com_github_golangci_misspell-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/misspell/com_github_golangci_misspell-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/misspell/com_github_golangci_misspell-v0.4.0.zip",
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
        sha256 = "70b471277ad59e3e8393090769ca6f0ab7e670067fdf559fba429404adbdb66f",
        strip_prefix = "github.com/golangci/revgrep@v0.0.0-20220804021717-745bb2f7c2e6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/revgrep/com_github_golangci_revgrep-v0.0.0-20220804021717-745bb2f7c2e6.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/revgrep/com_github_golangci_revgrep-v0.0.0-20220804021717-745bb2f7c2e6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/revgrep/com_github_golangci_revgrep-v0.0.0-20220804021717-745bb2f7c2e6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/revgrep/com_github_golangci_revgrep-v0.0.0-20220804021717-745bb2f7c2e6.zip",
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
        name = "com_github_google_gnostic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/gnostic",
        sha256 = "34d7aa522313f30b48196821dab8a9ec788349d7be9c8d3167d4b9b328cd8ec8",
        strip_prefix = "github.com/google/gnostic@v0.5.7-v3refs",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/gnostic/com_github_google_gnostic-v0.5.7-v3refs.zip",
            "http://ats.apps.svc/gomod/github.com/google/gnostic/com_github_google_gnostic-v0.5.7-v3refs.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/gnostic/com_github_google_gnostic-v0.5.7-v3refs.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/gnostic/com_github_google_gnostic-v0.5.7-v3refs.zip",
        ],
    )
    go_repository(
        name = "com_github_google_go_cmp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-cmp",
        sha256 = "32450874ac756ef5d47f6b819305105304b9819045a16e3f105289b7cf252c51",
        strip_prefix = "github.com/google/go-cmp@v0.5.9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/go-cmp/com_github_google_go_cmp-v0.5.9.zip",
            "http://ats.apps.svc/gomod/github.com/google/go-cmp/com_github_google_go_cmp-v0.5.9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/go-cmp/com_github_google_go_cmp-v0.5.9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/go-cmp/com_github_google_go_cmp-v0.5.9.zip",
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
        sha256 = "5c41453c0e2df199e899097e95d75f19fdda591e977233f47fab15b84e352b04",
        strip_prefix = "github.com/google/gofuzz@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/gofuzz/com_github_google_gofuzz-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/google/gofuzz/com_github_google_gofuzz-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/gofuzz/com_github_google_gofuzz-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/gofuzz/com_github_google_gofuzz-v1.1.0.zip",
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
        sha256 = "8638c11ff6cfa719453d52f3ccc4159a6749548469b59fff7f6f46b81d4ea434",
        strip_prefix = "github.com/google/pprof@v0.0.0-20211122183932-1daafda22083",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/pprof/com_github_google_pprof-v0.0.0-20211122183932-1daafda22083.zip",
            "http://ats.apps.svc/gomod/github.com/google/pprof/com_github_google_pprof-v0.0.0-20211122183932-1daafda22083.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/pprof/com_github_google_pprof-v0.0.0-20211122183932-1daafda22083.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/pprof/com_github_google_pprof-v0.0.0-20211122183932-1daafda22083.zip",
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
        sha256 = "0a5fcc05ea492afeaca984a012485f6a15e2259b32f1206d6f36a88c88afc607",
        strip_prefix = "github.com/google/uuid@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/uuid/com_github_google_uuid-v1.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/google/uuid/com_github_google_uuid-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/uuid/com_github_google_uuid-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/uuid/com_github_google_uuid-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_googleapis_enterprise_certificate_proxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/enterprise-certificate-proxy",
        sha256 = "e3a5b32ca7fc4f8bc36274d87c3547975a2b0603b2a1e4b1129530504d9ddeb7",
        strip_prefix = "github.com/googleapis/enterprise-certificate-proxy@v0.2.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/googleapis/enterprise-certificate-proxy/com_github_googleapis_enterprise_certificate_proxy-v0.2.3.zip",
            "http://ats.apps.svc/gomod/github.com/googleapis/enterprise-certificate-proxy/com_github_googleapis_enterprise_certificate_proxy-v0.2.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/googleapis/enterprise-certificate-proxy/com_github_googleapis_enterprise_certificate_proxy-v0.2.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/googleapis/enterprise-certificate-proxy/com_github_googleapis_enterprise_certificate_proxy-v0.2.3.zip",
        ],
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/gax-go/v2",
        sha256 = "b9bdfe36843cdc62b1eb2ba66ac1410164c2478c88c6bfe16c9ce2859922ee80",
        strip_prefix = "github.com/googleapis/gax-go/v2@v2.7.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/googleapis/gax-go/v2/com_github_googleapis_gax_go_v2-v2.7.1.zip",
            "http://ats.apps.svc/gomod/github.com/googleapis/gax-go/v2/com_github_googleapis_gax_go_v2-v2.7.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/googleapis/gax-go/v2/com_github_googleapis_gax_go_v2-v2.7.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/googleapis/gax-go/v2/com_github_googleapis_gax_go_v2-v2.7.1.zip",
        ],
    )
    go_repository(
        name = "com_github_googleapis_gnostic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/gnostic",
        sha256 = "6a594aca0b27b1618faa8cfe5f2c7b8385258831d409276ee6024ba7a7f70b42",
        strip_prefix = "github.com/googleapis/gnostic@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/googleapis/gnostic/com_github_googleapis_gnostic-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/googleapis/gnostic/com_github_googleapis_gnostic-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/googleapis/gnostic/com_github_googleapis_gnostic-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/googleapis/gnostic/com_github_googleapis_gnostic-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gophercloud_gophercloud",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gophercloud/gophercloud",
        sha256 = "34a191cae40881f94769d10990921db8092af81a123214969f668e9c8a79ecd7",
        strip_prefix = "github.com/gophercloud/gophercloud@v0.0.0-20190301152420-fca40860790e",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gophercloud/gophercloud/com_github_gophercloud_gophercloud-v0.0.0-20190301152420-fca40860790e.zip",
            "http://ats.apps.svc/gomod/github.com/gophercloud/gophercloud/com_github_gophercloud_gophercloud-v0.0.0-20190301152420-fca40860790e.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gophercloud/gophercloud/com_github_gophercloud_gophercloud-v0.0.0-20190301152420-fca40860790e.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gophercloud/gophercloud/com_github_gophercloud_gophercloud-v0.0.0-20190301152420-fca40860790e.zip",
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
        sha256 = "257e52af18f2f34ecbb7ada42a79bea1dd7c3d126b8a2e2c007704d13cda3b29",
        strip_prefix = "github.com/gordonklaus/ineffassign@v0.0.0-20230610083614-0e73809eb601",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gordonklaus/ineffassign/com_github_gordonklaus_ineffassign-v0.0.0-20230610083614-0e73809eb601.zip",
            "http://ats.apps.svc/gomod/github.com/gordonklaus/ineffassign/com_github_gordonklaus_ineffassign-v0.0.0-20230610083614-0e73809eb601.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gordonklaus/ineffassign/com_github_gordonklaus_ineffassign-v0.0.0-20230610083614-0e73809eb601.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gordonklaus/ineffassign/com_github_gordonklaus_ineffassign-v0.0.0-20230610083614-0e73809eb601.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_context",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/context",
        sha256 = "4ec8e01fe741a931edeebdee9348ffb49b5cc565ca245551d0d20b67062e6f0b",
        strip_prefix = "github.com/gorilla/context@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gorilla/context/com_github_gorilla_context-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/gorilla/context/com_github_gorilla_context-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gorilla/context/com_github_gorilla_context-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gorilla/context/com_github_gorilla_context-v1.1.1.zip",
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
        sha256 = "d0d1728deaa06dac190bf4964c9c6395923403eae337cb3305d6dda18ef07337",
        strip_prefix = "github.com/gorilla/websocket@v1.4.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gorilla/websocket/com_github_gorilla_websocket-v1.4.2.zip",
            "http://ats.apps.svc/gomod/github.com/gorilla/websocket/com_github_gorilla_websocket-v1.4.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gorilla/websocket/com_github_gorilla_websocket-v1.4.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gorilla/websocket/com_github_gorilla_websocket-v1.4.2.zip",
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
        name = "com_github_grpc_ecosystem_go_grpc_middleware",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grpc-ecosystem/go-grpc-middleware",
        sha256 = "081d63238be37f9f7fd2688642dc0f2c9c37374f99e7ac1d42c1f9184521723a",
        strip_prefix = "github.com/grpc-ecosystem/go-grpc-middleware@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/grpc-ecosystem/go-grpc-middleware/com_github_grpc_ecosystem_go_grpc_middleware-v1.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/grpc-ecosystem/go-grpc-middleware/com_github_grpc_ecosystem_go_grpc_middleware-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/grpc-ecosystem/go-grpc-middleware/com_github_grpc_ecosystem_go_grpc_middleware-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/grpc-ecosystem/go-grpc-middleware/com_github_grpc_ecosystem_go_grpc_middleware-v1.3.0.zip",
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
        name = "com_github_grpc_ecosystem_grpc_opentracing",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grpc-ecosystem/grpc-opentracing",
        sha256 = "0606bde24e978e9cd91ae45ca9e5222ce695c21a07ae02e77546496bf23b1c62",
        strip_prefix = "github.com/grpc-ecosystem/grpc-opentracing@v0.0.0-20180507213350-8e809c8a8645",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/grpc-ecosystem/grpc-opentracing/com_github_grpc_ecosystem_grpc_opentracing-v0.0.0-20180507213350-8e809c8a8645.zip",
            "http://ats.apps.svc/gomod/github.com/grpc-ecosystem/grpc-opentracing/com_github_grpc_ecosystem_grpc_opentracing-v0.0.0-20180507213350-8e809c8a8645.zip",
            "https://cache.hawkingrei.com/gomod/github.com/grpc-ecosystem/grpc-opentracing/com_github_grpc_ecosystem_grpc_opentracing-v0.0.0-20180507213350-8e809c8a8645.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/grpc-ecosystem/grpc-opentracing/com_github_grpc_ecosystem_grpc_opentracing-v0.0.0-20180507213350-8e809c8a8645.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_consul_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/consul/api",
        sha256 = "3971a179e700f1a839efe3b5a61d782d07124f7a4d2ad290ad37eaa888907a19",
        strip_prefix = "github.com/hashicorp/consul/api@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/consul/api/com_github_hashicorp_consul_api-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/consul/api/com_github_hashicorp_consul_api-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/consul/api/com_github_hashicorp_consul_api-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/consul/api/com_github_hashicorp_consul_api-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_consul_sdk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/consul/sdk",
        sha256 = "85188b2110551574646fcce5aa0a72dbde588596f3ebcf14964a3c4ce9c354ea",
        strip_prefix = "github.com/hashicorp/consul/sdk@v0.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/consul/sdk/com_github_hashicorp_consul_sdk-v0.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/consul/sdk/com_github_hashicorp_consul_sdk-v0.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/consul/sdk/com_github_hashicorp_consul_sdk-v0.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/consul/sdk/com_github_hashicorp_consul_sdk-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_errwrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/errwrap",
        sha256 = "ccdf4c90f894d8a5fde4e79d5828c5d27a13e9f7ce3006dd72ce76e6e17cdeb2",
        strip_prefix = "github.com/hashicorp/errwrap@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/errwrap/com_github_hashicorp_errwrap-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/errwrap/com_github_hashicorp_errwrap-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/errwrap/com_github_hashicorp_errwrap-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/errwrap/com_github_hashicorp_errwrap-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_cleanhttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-cleanhttp",
        sha256 = "e3cc9964b0bc80c6156d6fb064abcb62ff8c00df8be8009b6f6d3aefc2776a23",
        strip_prefix = "github.com/hashicorp/go-cleanhttp@v0.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-cleanhttp/com_github_hashicorp_go_cleanhttp-v0.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-cleanhttp/com_github_hashicorp_go_cleanhttp-v0.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-cleanhttp/com_github_hashicorp_go_cleanhttp-v0.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-cleanhttp/com_github_hashicorp_go_cleanhttp-v0.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_immutable_radix",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-immutable-radix",
        sha256 = "ab5d08582870e7177a74ba2c84c327aece8655cbd94653f801a0551156bb8a9c",
        strip_prefix = "github.com/hashicorp/go-immutable-radix@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-immutable-radix/com_github_hashicorp_go_immutable_radix-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-immutable-radix/com_github_hashicorp_go_immutable_radix-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-immutable-radix/com_github_hashicorp_go_immutable_radix-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-immutable-radix/com_github_hashicorp_go_immutable_radix-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_msgpack",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-msgpack",
        sha256 = "24cb41ed887c67c361bfa7b87f79eff4d07152196a015d29ae2e1e51c14066d7",
        strip_prefix = "github.com/hashicorp/go-msgpack@v0.5.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-msgpack/com_github_hashicorp_go_msgpack-v0.5.4.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-msgpack/com_github_hashicorp_go_msgpack-v0.5.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-msgpack/com_github_hashicorp_go_msgpack-v0.5.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-msgpack/com_github_hashicorp_go_msgpack-v0.5.4.zip",
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
        name = "com_github_hashicorp_go_net",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go.net",
        sha256 = "71564aa3cb6e2820ee31e4d9e264e4ed889c7916f958b2f54c6f3004d4fcd8d2",
        strip_prefix = "github.com/hashicorp/go.net@v0.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go.net/com_github_hashicorp_go_net-v0.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go.net/com_github_hashicorp_go_net-v0.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go.net/com_github_hashicorp_go_net-v0.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go.net/com_github_hashicorp_go_net-v0.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_rootcerts",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-rootcerts",
        sha256 = "4393b0b9cd741e00de5624d5124cf054bf50c57231d4b1caff84c8a4d16c6a47",
        strip_prefix = "github.com/hashicorp/go-rootcerts@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-rootcerts/com_github_hashicorp_go_rootcerts-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-rootcerts/com_github_hashicorp_go_rootcerts-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-rootcerts/com_github_hashicorp_go_rootcerts-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-rootcerts/com_github_hashicorp_go_rootcerts-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_sockaddr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-sockaddr",
        sha256 = "50c1b60863b0cd31d03b26d3975f76cab55466666c067cd1823481a61f19af33",
        strip_prefix = "github.com/hashicorp/go-sockaddr@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-sockaddr/com_github_hashicorp_go_sockaddr-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-sockaddr/com_github_hashicorp_go_sockaddr-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-sockaddr/com_github_hashicorp_go_sockaddr-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-sockaddr/com_github_hashicorp_go_sockaddr-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_syslog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-syslog",
        sha256 = "a0ca8b61ea365e9ecdca513b94f200aef3ff68b4c95d9dabc88ca25fcb33bce6",
        strip_prefix = "github.com/hashicorp/go-syslog@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/go-syslog/com_github_hashicorp_go_syslog-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/go-syslog/com_github_hashicorp_go_syslog-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/go-syslog/com_github_hashicorp_go_syslog-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/go-syslog/com_github_hashicorp_go_syslog-v1.0.0.zip",
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
        sha256 = "0f8aaf311e48fba046920d38b999c066da69997b479f4eca126fe968899717da",
        strip_prefix = "github.com/hashicorp/golang-lru@v0.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/golang-lru/com_github_hashicorp_golang_lru-v0.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/golang-lru/com_github_hashicorp_golang_lru-v0.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/golang-lru/com_github_hashicorp_golang_lru-v0.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/golang-lru/com_github_hashicorp_golang_lru-v0.5.1.zip",
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
        name = "com_github_hashicorp_logutils",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/logutils",
        sha256 = "0e88424578d1d6b7793b63d30c180a353ce8041701d25dc7c3bcd9841c36db5b",
        strip_prefix = "github.com/hashicorp/logutils@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/logutils/com_github_hashicorp_logutils-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/logutils/com_github_hashicorp_logutils-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/logutils/com_github_hashicorp_logutils-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/logutils/com_github_hashicorp_logutils-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_mdns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/mdns",
        sha256 = "a1e1440d9c4189636b6cd30ec7592beab68139a4d87e580f5a1fed029778bdc9",
        strip_prefix = "github.com/hashicorp/mdns@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/mdns/com_github_hashicorp_mdns-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/mdns/com_github_hashicorp_mdns-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/mdns/com_github_hashicorp_mdns-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/mdns/com_github_hashicorp_mdns-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_memberlist",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/memberlist",
        sha256 = "9f83e052b0a5d96f6d8144a40c297aea37137bef7f58aa496bc5eab4e0c54e0b",
        strip_prefix = "github.com/hashicorp/memberlist@v0.1.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/memberlist/com_github_hashicorp_memberlist-v0.1.3.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/memberlist/com_github_hashicorp_memberlist-v0.1.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/memberlist/com_github_hashicorp_memberlist-v0.1.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/memberlist/com_github_hashicorp_memberlist-v0.1.3.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_serf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/serf",
        sha256 = "0f431658e69625f61defd36073e893ce21f04fe5a96484b812d47e32d4154be0",
        strip_prefix = "github.com/hashicorp/serf@v0.8.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hashicorp/serf/com_github_hashicorp_serf-v0.8.2.zip",
            "http://ats.apps.svc/gomod/github.com/hashicorp/serf/com_github_hashicorp_serf-v0.8.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hashicorp/serf/com_github_hashicorp_serf-v0.8.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hashicorp/serf/com_github_hashicorp_serf-v0.8.2.zip",
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
        name = "com_github_iancoleman_strcase",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iancoleman/strcase",
        sha256 = "cb5027fec91d36426f0978a6c42ab52d8735fa3e1711be0127feda70a9a9fd05",
        strip_prefix = "github.com/iancoleman/strcase@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/iancoleman/strcase/com_github_iancoleman_strcase-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/iancoleman/strcase/com_github_iancoleman_strcase-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/iancoleman/strcase/com_github_iancoleman_strcase-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/iancoleman/strcase/com_github_iancoleman_strcase-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ianlancetaylor_demangle",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ianlancetaylor/demangle",
        sha256 = "5bbddd83cb4b8a42d741fb6a2b50826ebbee800c51b7a9e75dfd2bdc373278a1",
        strip_prefix = "github.com/ianlancetaylor/demangle@v0.0.0-20210905161508-09a460cdf81d",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ianlancetaylor/demangle/com_github_ianlancetaylor_demangle-v0.0.0-20210905161508-09a460cdf81d.zip",
            "http://ats.apps.svc/gomod/github.com/ianlancetaylor/demangle/com_github_ianlancetaylor_demangle-v0.0.0-20210905161508-09a460cdf81d.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ianlancetaylor/demangle/com_github_ianlancetaylor_demangle-v0.0.0-20210905161508-09a460cdf81d.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ianlancetaylor/demangle/com_github_ianlancetaylor_demangle-v0.0.0-20210905161508-09a460cdf81d.zip",
        ],
    )
    go_repository(
        name = "com_github_imdario_mergo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/imdario/mergo",
        sha256 = "47332eb559e993749cc31292807b3a639a470032ec603fd3c15fbe46f82192f6",
        strip_prefix = "github.com/imdario/mergo@v0.3.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/imdario/mergo/com_github_imdario_mergo-v0.3.11.zip",
            "http://ats.apps.svc/gomod/github.com/imdario/mergo/com_github_imdario_mergo-v0.3.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/imdario/mergo/com_github_imdario_mergo-v0.3.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/imdario/mergo/com_github_imdario_mergo-v0.3.11.zip",
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
        name = "com_github_influxdata_influxdb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/influxdb",
        sha256 = "a59a6a42828346f125f7d97be36639cda093ce1c311e1e3fd292680b4474ced6",
        strip_prefix = "github.com/influxdata/influxdb@v0.0.0-20170331210902-15e594fc09f1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/influxdata/influxdb/com_github_influxdata_influxdb-v0.0.0-20170331210902-15e594fc09f1.zip",
            "http://ats.apps.svc/gomod/github.com/influxdata/influxdb/com_github_influxdata_influxdb-v0.0.0-20170331210902-15e594fc09f1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/influxdata/influxdb/com_github_influxdata_influxdb-v0.0.0-20170331210902-15e594fc09f1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/influxdata/influxdb/com_github_influxdata_influxdb-v0.0.0-20170331210902-15e594fc09f1.zip",
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
        name = "com_github_jackc_fake",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/fake",
        sha256 = "bf8b5b51ae03f572a70a0582dc663c5733bba9aca785d39bb0367797148e6d64",
        strip_prefix = "github.com/jackc/fake@v0.0.0-20150926172116-812a484cc733",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jackc/fake/com_github_jackc_fake-v0.0.0-20150926172116-812a484cc733.zip",
            "http://ats.apps.svc/gomod/github.com/jackc/fake/com_github_jackc_fake-v0.0.0-20150926172116-812a484cc733.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jackc/fake/com_github_jackc_fake-v0.0.0-20150926172116-812a484cc733.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jackc/fake/com_github_jackc_fake-v0.0.0-20150926172116-812a484cc733.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgx",
        sha256 = "e158f1752893bc638d66e31c0a928cbb96119df8d459d36fcef52b4b31a6d24d",
        strip_prefix = "github.com/jackc/pgx@v3.2.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jackc/pgx/com_github_jackc_pgx-v3.2.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/jackc/pgx/com_github_jackc_pgx-v3.2.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jackc/pgx/com_github_jackc_pgx-v3.2.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jackc/pgx/com_github_jackc_pgx-v3.2.0+incompatible.zip",
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
        name = "com_github_jeffail_gabs_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Jeffail/gabs/v2",
        sha256 = "3ddec4d5488c8505fa384ae20429c0735eda1ddbd0d15beeb0a5747ebded63c5",
        strip_prefix = "github.com/Jeffail/gabs/v2@v2.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Jeffail/gabs/v2/com_github_jeffail_gabs_v2-v2.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/Jeffail/gabs/v2/com_github_jeffail_gabs_v2-v2.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Jeffail/gabs/v2/com_github_jeffail_gabs_v2-v2.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Jeffail/gabs/v2/com_github_jeffail_gabs_v2-v2.5.1.zip",
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
        name = "com_github_jgautheron_goconst",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jgautheron/goconst",
        sha256 = "4876028bfb5f28c607984ac39c4f293f94156ce71fa4ec0e8f7a88d326a80ff3",
        strip_prefix = "github.com/jgautheron/goconst@v1.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jgautheron/goconst/com_github_jgautheron_goconst-v1.5.1.zip",
            "http://ats.apps.svc/gomod/github.com/jgautheron/goconst/com_github_jgautheron_goconst-v1.5.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jgautheron/goconst/com_github_jgautheron_goconst-v1.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jgautheron/goconst/com_github_jgautheron_goconst-v1.5.1.zip",
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
        sha256 = "86860abafcd7332af4b1195b1c092ade503b31b94a6bac3c9140e5ee0d0219f0",
        strip_prefix = "github.com/jonboulle/clockwork@v0.2.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jonboulle/clockwork/com_github_jonboulle_clockwork-v0.2.2.zip",
            "http://ats.apps.svc/gomod/github.com/jonboulle/clockwork/com_github_jonboulle_clockwork-v0.2.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jonboulle/clockwork/com_github_jonboulle_clockwork-v0.2.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jonboulle/clockwork/com_github_jonboulle_clockwork-v0.2.2.zip",
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
        sha256 = "5c80ed1924e67e74da241a02e57f2c8fb2cf6d539b14be4729063f78ea437938",
        strip_prefix = "github.com/kisielk/errcheck@v1.6.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kisielk/errcheck/com_github_kisielk_errcheck-v1.6.3.zip",
            "http://ats.apps.svc/gomod/github.com/kisielk/errcheck/com_github_kisielk_errcheck-v1.6.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kisielk/errcheck/com_github_kisielk_errcheck-v1.6.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kisielk/errcheck/com_github_kisielk_errcheck-v1.6.3.zip",
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
        sha256 = "7e004bb6b71535508bfa9c57256cfb2ca23f09ea281dbecafea217796b712fcd",
        strip_prefix = "github.com/klauspost/compress@v1.16.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/klauspost/compress/com_github_klauspost_compress-v1.16.5.zip",
            "http://ats.apps.svc/gomod/github.com/klauspost/compress/com_github_klauspost_compress-v1.16.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/klauspost/compress/com_github_klauspost_compress-v1.16.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/klauspost/compress/com_github_klauspost_compress-v1.16.5.zip",
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
        name = "com_github_knz_strtime",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/knz/strtime",
        sha256 = "bd562758fa61a744b3b7c5fd3616dece50c6b92bfa11511ed1e1ab8c43831eb8",
        strip_prefix = "github.com/knz/strtime@v0.0.0-20181018220328-af2256ee352c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/knz/strtime/com_github_knz_strtime-v0.0.0-20181018220328-af2256ee352c.zip",
            "http://ats.apps.svc/gomod/github.com/knz/strtime/com_github_knz_strtime-v0.0.0-20181018220328-af2256ee352c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/knz/strtime/com_github_knz_strtime-v0.0.0-20181018220328-af2256ee352c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/knz/strtime/com_github_knz_strtime-v0.0.0-20181018220328-af2256ee352c.zip",
        ],
    )
    go_repository(
        name = "com_github_konsorten_go_windows_terminal_sequences",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/konsorten/go-windows-terminal-sequences",
        sha256 = "429b01413b972b108ea86bbde3d5e660913f3e8099190d07ccfb2f186bc6d837",
        strip_prefix = "github.com/konsorten/go-windows-terminal-sequences@v1.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/konsorten/go-windows-terminal-sequences/com_github_konsorten_go_windows_terminal_sequences-v1.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/konsorten/go-windows-terminal-sequences/com_github_konsorten_go_windows_terminal_sequences-v1.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/konsorten/go-windows-terminal-sequences/com_github_konsorten_go_windows_terminal_sequences-v1.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/konsorten/go-windows-terminal-sequences/com_github_konsorten_go_windows_terminal_sequences-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_kr_logfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/logfmt",
        sha256 = "ebd95653aaca6182184a1b9b309a65d55eb4c7c833c5e790aee11efd73d4722c",
        strip_prefix = "github.com/kr/logfmt@v0.0.0-20140226030751-b84e30acd515",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kr/logfmt/com_github_kr_logfmt-v0.0.0-20140226030751-b84e30acd515.zip",
            "http://ats.apps.svc/gomod/github.com/kr/logfmt/com_github_kr_logfmt-v0.0.0-20140226030751-b84e30acd515.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kr/logfmt/com_github_kr_logfmt-v0.0.0-20140226030751-b84e30acd515.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kr/logfmt/com_github_kr_logfmt-v0.0.0-20140226030751-b84e30acd515.zip",
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
        sha256 = "11ed0c945e877a49c02db7533156b4b193e9ede9bbacc80d25a83aa230397f2e",
        strip_prefix = "github.com/kunwardeep/paralleltest@v1.0.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kunwardeep/paralleltest/com_github_kunwardeep_paralleltest-v1.0.7.zip",
            "http://ats.apps.svc/gomod/github.com/kunwardeep/paralleltest/com_github_kunwardeep_paralleltest-v1.0.7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kunwardeep/paralleltest/com_github_kunwardeep_paralleltest-v1.0.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kunwardeep/paralleltest/com_github_kunwardeep_paralleltest-v1.0.7.zip",
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
        sha256 = "0621ab66f2510093f86f838db09a698027e8cbf08cc0e52bfa7d359b4f1b3745",
        strip_prefix = "github.com/lestrrat-go/blackmagic@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lestrrat-go/blackmagic/com_github_lestrrat_go_blackmagic-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/lestrrat-go/blackmagic/com_github_lestrrat_go_blackmagic-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lestrrat-go/blackmagic/com_github_lestrrat_go_blackmagic-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lestrrat-go/blackmagic/com_github_lestrrat_go_blackmagic-v1.0.1.zip",
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
        sha256 = "fd0658206207ff68f0561d9a681a99bee765d9cc453665d202a01ce860c72a90",
        strip_prefix = "github.com/lestrrat-go/httprc@v1.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lestrrat-go/httprc/com_github_lestrrat_go_httprc-v1.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/lestrrat-go/httprc/com_github_lestrrat_go_httprc-v1.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lestrrat-go/httprc/com_github_lestrrat_go_httprc-v1.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lestrrat-go/httprc/com_github_lestrrat_go_httprc-v1.0.4.zip",
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
        sha256 = "bea73ce04072a52f02af194a18dfd61de16b468eecc4e05c31e497cd03b67bfd",
        strip_prefix = "github.com/lestrrat-go/jwx/v2@v2.0.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lestrrat-go/jwx/v2/com_github_lestrrat_go_jwx_v2-v2.0.11.zip",
            "http://ats.apps.svc/gomod/github.com/lestrrat-go/jwx/v2/com_github_lestrrat_go_jwx_v2-v2.0.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lestrrat-go/jwx/v2/com_github_lestrrat_go_jwx_v2-v2.0.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lestrrat-go/jwx/v2/com_github_lestrrat_go_jwx_v2-v2.0.11.zip",
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
        name = "com_github_lib_pq",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lib/pq",
        sha256 = "8aa4a8870dbd30c8b143fe70f121c3ea917b6483251d1384da1b01fc6c6f6c30",
        strip_prefix = "github.com/lib/pq@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lib/pq/com_github_lib_pq-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/lib/pq/com_github_lib_pq-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lib/pq/com_github_lib_pq-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lib/pq/com_github_lib_pq-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_lightstep_lightstep_tracer_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lightstep/lightstep-tracer-go",
        sha256 = "426bdb6f7cd88747dceddf20745314abb3c568e782fa811faf2f3433c4cfabaa",
        strip_prefix = "github.com/lightstep/lightstep-tracer-go@v0.15.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lightstep/lightstep-tracer-go/com_github_lightstep_lightstep_tracer_go-v0.15.6.zip",
            "http://ats.apps.svc/gomod/github.com/lightstep/lightstep-tracer-go/com_github_lightstep_lightstep_tracer_go-v0.15.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lightstep/lightstep-tracer-go/com_github_lightstep_lightstep_tracer_go-v0.15.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lightstep/lightstep-tracer-go/com_github_lightstep_lightstep_tracer_go-v0.15.6.zip",
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
        sha256 = "5484892c645beb53b7120549baa8ca3297b5cd2fd57158603441e71ea7c3d511",
        strip_prefix = "github.com/mattn/go-isatty@v0.0.18",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mattn/go-isatty/com_github_mattn_go_isatty-v0.0.18.zip",
            "http://ats.apps.svc/gomod/github.com/mattn/go-isatty/com_github_mattn_go_isatty-v0.0.18.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mattn/go-isatty/com_github_mattn_go_isatty-v0.0.18.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mattn/go-isatty/com_github_mattn_go_isatty-v0.0.18.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_runewidth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-runewidth",
        sha256 = "364ef5ed31f6571dad56730305b5c2288a53da06d9832680ade5e21d97a748e7",
        strip_prefix = "github.com/mattn/go-runewidth@v0.0.14",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mattn/go-runewidth/com_github_mattn_go_runewidth-v0.0.14.zip",
            "http://ats.apps.svc/gomod/github.com/mattn/go-runewidth/com_github_mattn_go_runewidth-v0.0.14.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mattn/go-runewidth/com_github_mattn_go_runewidth-v0.0.14.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mattn/go-runewidth/com_github_mattn_go_runewidth-v0.0.14.zip",
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
        sha256 = "0b44aabaa9aea5d28e667849ad4d9821351466c3591dd7beddb2d025db6d55f2",
        strip_prefix = "github.com/matttproud/golang_protobuf_extensions@v1.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/matttproud/golang_protobuf_extensions/com_github_matttproud_golang_protobuf_extensions-v1.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/matttproud/golang_protobuf_extensions/com_github_matttproud_golang_protobuf_extensions-v1.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/matttproud/golang_protobuf_extensions/com_github_matttproud_golang_protobuf_extensions-v1.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/matttproud/golang_protobuf_extensions/com_github_matttproud_golang_protobuf_extensions-v1.0.4.zip",
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
        sha256 = "d75f7d9a082a19d7f6ae734856bd7f74ea2677c646afc73e0ecaf7859a51c5e7",
        strip_prefix = "github.com/mgechev/revive@v1.3.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mgechev/revive/com_github_mgechev_revive-v1.3.2.zip",
            "http://ats.apps.svc/gomod/github.com/mgechev/revive/com_github_mgechev_revive-v1.3.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mgechev/revive/com_github_mgechev_revive-v1.3.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mgechev/revive/com_github_mgechev_revive-v1.3.2.zip",
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
        name = "com_github_miekg_dns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/miekg/dns",
        sha256 = "32fd332c8cea149f29ffb603020548a48773bc44c974465898c938a58ca0c03a",
        strip_prefix = "github.com/miekg/dns@v1.1.10",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/miekg/dns/com_github_miekg_dns-v1.1.10.zip",
            "http://ats.apps.svc/gomod/github.com/miekg/dns/com_github_miekg_dns-v1.1.10.zip",
            "https://cache.hawkingrei.com/gomod/github.com/miekg/dns/com_github_miekg_dns-v1.1.10.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/miekg/dns/com_github_miekg_dns-v1.1.10.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_cli",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/cli",
        sha256 = "74199f2c2e1735a45e9f5c2ca049d352b0cc73d945823540e54ca9975ce35752",
        strip_prefix = "github.com/mitchellh/cli@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/cli/com_github_mitchellh_cli-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/cli/com_github_mitchellh_cli-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/cli/com_github_mitchellh_cli-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/cli/com_github_mitchellh_cli-v1.0.0.zip",
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
        name = "com_github_mitchellh_go_testing_interface",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-testing-interface",
        sha256 = "255871a399420cd3513b12f50738d290e251637deb23e21a4332192584ecf9c7",
        strip_prefix = "github.com/mitchellh/go-testing-interface@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/go-testing-interface/com_github_mitchellh_go_testing_interface-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/go-testing-interface/com_github_mitchellh_go_testing_interface-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/go-testing-interface/com_github_mitchellh_go_testing_interface-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/go-testing-interface/com_github_mitchellh_go_testing_interface-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_go_wordwrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-wordwrap",
        sha256 = "9ea185f97dfe616da351b63b229a5a212b14ac0e23bd3f943e39590eadb38031",
        strip_prefix = "github.com/mitchellh/go-wordwrap@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/go-wordwrap/com_github_mitchellh_go_wordwrap-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/go-wordwrap/com_github_mitchellh_go_wordwrap-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/go-wordwrap/com_github_mitchellh_go_wordwrap-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/go-wordwrap/com_github_mitchellh_go_wordwrap-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_gox",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/gox",
        sha256 = "70c976edc82b069d55c4b05409be9e91d85c20238a5e38c60fbb0b03b43c9550",
        strip_prefix = "github.com/mitchellh/gox@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/gox/com_github_mitchellh_gox-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/gox/com_github_mitchellh_gox-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/gox/com_github_mitchellh_gox-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/gox/com_github_mitchellh_gox-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_iochan",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/iochan",
        sha256 = "f3eede01adb24c22945bf71b4f84ae25e3744a12b9d8bd7c016705adc0d778b8",
        strip_prefix = "github.com/mitchellh/iochan@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mitchellh/iochan/com_github_mitchellh_iochan-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/mitchellh/iochan/com_github_mitchellh_iochan-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mitchellh/iochan/com_github_mitchellh_iochan-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mitchellh/iochan/com_github_mitchellh_iochan-v1.0.0.zip",
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
        sha256 = "fac4308cc66d568256e7aafe694ae58603ddeb9bb39965caa550dbe3fbd77ddc",
        strip_prefix = "github.com/montanaflynn/stats@v0.6.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/montanaflynn/stats/com_github_montanaflynn_stats-v0.6.6.zip",
            "http://ats.apps.svc/gomod/github.com/montanaflynn/stats/com_github_montanaflynn_stats-v0.6.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/montanaflynn/stats/com_github_montanaflynn_stats-v0.6.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/montanaflynn/stats/com_github_montanaflynn_stats-v0.6.6.zip",
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
        sha256 = "91e38295b4cd805adeeea414f0ed21037f0fd6a371cf45f8d12c872ef909170b",
        strip_prefix = "github.com/nishanths/exhaustive@v0.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nishanths/exhaustive/com_github_nishanths_exhaustive-v0.11.0.zip",
            "http://ats.apps.svc/gomod/github.com/nishanths/exhaustive/com_github_nishanths_exhaustive-v0.11.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nishanths/exhaustive/com_github_nishanths_exhaustive-v0.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nishanths/exhaustive/com_github_nishanths_exhaustive-v0.11.0.zip",
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
        sha256 = "dc7a4a696f28be478f04811b033507e3801f0ad1d8e6f9fb20f2498cbe2bbe92",
        strip_prefix = "github.com/nunnatsa/ginkgolinter@v0.12.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.12.1.zip",
            "http://ats.apps.svc/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.12.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.12.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.12.1.zip",
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
        sha256 = "108d409b7d235d61b82cfb6e1df139501123fcd8fa68fe94ddb024b53335cb48",
        strip_prefix = "github.com/oklog/run@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/oklog/run/com_github_oklog_run-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/oklog/run/com_github_oklog_run-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/oklog/run/com_github_oklog_run-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/oklog/run/com_github_oklog_run-v1.0.0.zip",
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
        name = "com_github_oneofone_xxhash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/OneOfOne/xxhash",
        sha256 = "7ab3c6a0e7c16c987a589e50a9a353e8877cfffea02bf9e04e370fd26a0c85e1",
        strip_prefix = "github.com/OneOfOne/xxhash@v1.2.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/OneOfOne/xxhash/com_github_oneofone_xxhash-v1.2.5.zip",
            "http://ats.apps.svc/gomod/github.com/OneOfOne/xxhash/com_github_oneofone_xxhash-v1.2.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/OneOfOne/xxhash/com_github_oneofone_xxhash-v1.2.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/OneOfOne/xxhash/com_github_oneofone_xxhash-v1.2.5.zip",
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
        sha256 = "c5cdb980ec4d450f3df8a471718494fd9192a5751cbeff14b9025fa9c0c86b16",
        strip_prefix = "github.com/onsi/ginkgo/v2@v2.9.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/onsi/ginkgo/v2/com_github_onsi_ginkgo_v2-v2.9.1.zip",
            "http://ats.apps.svc/gomod/github.com/onsi/ginkgo/v2/com_github_onsi_ginkgo_v2-v2.9.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/onsi/ginkgo/v2/com_github_onsi_ginkgo_v2-v2.9.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/onsi/ginkgo/v2/com_github_onsi_ginkgo_v2-v2.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_onsi_gomega",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/onsi/gomega",
        sha256 = "c7c39c6aa6a544939044a2a51ff86cd4d911a3801358d83ee48278fdbe5fe42c",
        strip_prefix = "github.com/onsi/gomega@v1.27.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/onsi/gomega/com_github_onsi_gomega-v1.27.4.zip",
            "http://ats.apps.svc/gomod/github.com/onsi/gomega/com_github_onsi_gomega-v1.27.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/onsi/gomega/com_github_onsi_gomega-v1.27.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/onsi/gomega/com_github_onsi_gomega-v1.27.4.zip",
        ],
    )
    go_repository(
        name = "com_github_openpeedeep_depguard_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/OpenPeeDeeP/depguard/v2",
        sha256 = "0b046baf283f5b30fde2c321523a2b1b91b64b6e86bc986e069d1191ab6c1ec6",
        strip_prefix = "github.com/OpenPeeDeeP/depguard/v2@v2.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/OpenPeeDeeP/depguard/v2/com_github_openpeedeep_depguard_v2-v2.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/OpenPeeDeeP/depguard/v2/com_github_openpeedeep_depguard_v2-v2.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/OpenPeeDeeP/depguard/v2/com_github_openpeedeep_depguard_v2-v2.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/OpenPeeDeeP/depguard/v2/com_github_openpeedeep_depguard_v2-v2.1.0.zip",
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
        name = "com_github_opentracing_contrib_go_stdlib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing-contrib/go-stdlib",
        sha256 = "b12d4649ede78423ab6d147161dfe160daaeb02a77dca0b488b7ffad51cc49c1",
        strip_prefix = "github.com/opentracing-contrib/go-stdlib@v0.0.0-20170113013457-1de4cc2120e7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/opentracing-contrib/go-stdlib/com_github_opentracing_contrib_go_stdlib-v0.0.0-20170113013457-1de4cc2120e7.zip",
            "http://ats.apps.svc/gomod/github.com/opentracing-contrib/go-stdlib/com_github_opentracing_contrib_go_stdlib-v0.0.0-20170113013457-1de4cc2120e7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/opentracing-contrib/go-stdlib/com_github_opentracing_contrib_go_stdlib-v0.0.0-20170113013457-1de4cc2120e7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/opentracing-contrib/go-stdlib/com_github_opentracing_contrib_go_stdlib-v0.0.0-20170113013457-1de4cc2120e7.zip",
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
        name = "com_github_openzipkin_zipkin_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/openzipkin/zipkin-go",
        sha256 = "36fd67db687108f4dc2b2a8607c3ad6ca226228a7a307897105d7d3f3ea28ccb",
        strip_prefix = "github.com/openzipkin/zipkin-go@v0.1.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/openzipkin/zipkin-go/com_github_openzipkin_zipkin_go-v0.1.6.zip",
            "http://ats.apps.svc/gomod/github.com/openzipkin/zipkin-go/com_github_openzipkin_zipkin_go-v0.1.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/openzipkin/zipkin-go/com_github_openzipkin_zipkin_go-v0.1.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/openzipkin/zipkin-go/com_github_openzipkin_zipkin_go-v0.1.6.zip",
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
        name = "com_github_pascaldekloe_goe",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pascaldekloe/goe",
        sha256 = "fa1b653a2e460194150393e186af967c8b1d24811252aac12f9ab4474beefdc6",
        strip_prefix = "github.com/pascaldekloe/goe@v0.0.0-20180627143212-57f6aae5913c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pascaldekloe/goe/com_github_pascaldekloe_goe-v0.0.0-20180627143212-57f6aae5913c.zip",
            "http://ats.apps.svc/gomod/github.com/pascaldekloe/goe/com_github_pascaldekloe_goe-v0.0.0-20180627143212-57f6aae5913c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pascaldekloe/goe/com_github_pascaldekloe_goe-v0.0.0-20180627143212-57f6aae5913c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pascaldekloe/goe/com_github_pascaldekloe_goe-v0.0.0-20180627143212-57f6aae5913c.zip",
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
        name = "com_github_peterbourgon_g2s",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/peterbourgon/g2s",
        sha256 = "41526f42b4fe3019581ab3745afea18271d7f037eb55a6e9fb3e32fd09ff9b8d",
        strip_prefix = "github.com/peterbourgon/g2s@v0.0.0-20170223122336-d4e7ad98afea",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/peterbourgon/g2s/com_github_peterbourgon_g2s-v0.0.0-20170223122336-d4e7ad98afea.zip",
            "http://ats.apps.svc/gomod/github.com/peterbourgon/g2s/com_github_peterbourgon_g2s-v0.0.0-20170223122336-d4e7ad98afea.zip",
            "https://cache.hawkingrei.com/gomod/github.com/peterbourgon/g2s/com_github_peterbourgon_g2s-v0.0.0-20170223122336-d4e7ad98afea.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/peterbourgon/g2s/com_github_peterbourgon_g2s-v0.0.0-20170223122336-d4e7ad98afea.zip",
        ],
    )
    go_repository(
        name = "com_github_petermattis_goid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/petermattis/goid",
        sha256 = "9f536c5d39d6a3c851670ec585e1c876fe31f3402556d215ebbaffcecbacb30a",
        strip_prefix = "github.com/petermattis/goid@v0.0.0-20211229010228-4d14c490ee36",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/petermattis/goid/com_github_petermattis_goid-v0.0.0-20211229010228-4d14c490ee36.zip",
            "http://ats.apps.svc/gomod/github.com/petermattis/goid/com_github_petermattis_goid-v0.0.0-20211229010228-4d14c490ee36.zip",
            "https://cache.hawkingrei.com/gomod/github.com/petermattis/goid/com_github_petermattis_goid-v0.0.0-20211229010228-4d14c490ee36.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/petermattis/goid/com_github_petermattis_goid-v0.0.0-20211229010228-4d14c490ee36.zip",
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
        sha256 = "4dadf9dc507b4187a70b78e49d572bc0e8f89a7b4a8974d6a978f72620526996",
        strip_prefix = "github.com/pingcap/errors@v0.11.5-0.20221009092201-b66cddb77c32",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/errors/com_github_pingcap_errors-v0.11.5-0.20221009092201-b66cddb77c32.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/errors/com_github_pingcap_errors-v0.11.5-0.20221009092201-b66cddb77c32.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/errors/com_github_pingcap_errors-v0.11.5-0.20221009092201-b66cddb77c32.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/errors/com_github_pingcap_errors-v0.11.5-0.20221009092201-b66cddb77c32.zip",
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
        sha256 = "e5ed8f109f8c7ef94d89d9006c27b6744beabbb8c785479fd8c0673680b301c5",
        strip_prefix = "github.com/pingcap/kvproto@v0.0.0-20230825101459-934e842bfd6e",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/kvproto/com_github_pingcap_kvproto-v0.0.0-20230825101459-934e842bfd6e.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/kvproto/com_github_pingcap_kvproto-v0.0.0-20230825101459-934e842bfd6e.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/kvproto/com_github_pingcap_kvproto-v0.0.0-20230825101459-934e842bfd6e.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/kvproto/com_github_pingcap_kvproto-v0.0.0-20230825101459-934e842bfd6e.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_log",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/log",
        sha256 = "9b0ae182fcc611cc535eb18a6f332846f0744b905e48888c06c1f6aeda8036d5",
        strip_prefix = "github.com/pingcap/log@v1.1.1-0.20230317032135-a0d097d16e22",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/log/com_github_pingcap_log-v1.1.1-0.20230317032135-a0d097d16e22.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/log/com_github_pingcap_log-v1.1.1-0.20230317032135-a0d097d16e22.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/log/com_github_pingcap_log-v1.1.1-0.20230317032135-a0d097d16e22.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/log/com_github_pingcap_log-v1.1.1-0.20230317032135-a0d097d16e22.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_sysutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/sysutil",
        sha256 = "0540f3dfd25706302c0f484baae0b6bb6a6382893bbf8b14720debcb9756f1d0",
        strip_prefix = "github.com/pingcap/sysutil@v1.0.1-0.20230407040306-fb007c5aff21",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/sysutil/com_github_pingcap_sysutil-v1.0.1-0.20230407040306-fb007c5aff21.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/sysutil/com_github_pingcap_sysutil-v1.0.1-0.20230407040306-fb007c5aff21.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/sysutil/com_github_pingcap_sysutil-v1.0.1-0.20230407040306-fb007c5aff21.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/sysutil/com_github_pingcap_sysutil-v1.0.1-0.20230407040306-fb007c5aff21.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_tipb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/tipb",
        sha256 = "d6e086c68505edca6fba1a6264dadbab29abc85722d520355c485da901dfdb41",
        strip_prefix = "github.com/pingcap/tipb@v0.0.0-20230822064221-711da6fede03",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pingcap/tipb/com_github_pingcap_tipb-v0.0.0-20230822064221-711da6fede03.zip",
            "http://ats.apps.svc/gomod/github.com/pingcap/tipb/com_github_pingcap_tipb-v0.0.0-20230822064221-711da6fede03.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pingcap/tipb/com_github_pingcap_tipb-v0.0.0-20230822064221-711da6fede03.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pingcap/tipb/com_github_pingcap_tipb-v0.0.0-20230822064221-711da6fede03.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_browser",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/browser",
        sha256 = "84db38d8db553ccc34c75f867396126eac07774b979c470f97a20854d3a3af6d",
        strip_prefix = "github.com/pkg/browser@v0.0.0-20210115035449-ce105d075bb4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pkg/browser/com_github_pkg_browser-v0.0.0-20210115035449-ce105d075bb4.zip",
            "http://ats.apps.svc/gomod/github.com/pkg/browser/com_github_pkg_browser-v0.0.0-20210115035449-ce105d075bb4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pkg/browser/com_github_pkg_browser-v0.0.0-20210115035449-ce105d075bb4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pkg/browser/com_github_pkg_browser-v0.0.0-20210115035449-ce105d075bb4.zip",
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
        sha256 = "de04cecc1a4b8d53e4357051026794bcbc54f2e6a260cfac508ce69d5d6457a0",
        strip_prefix = "github.com/pmezard/go-difflib@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pmezard/go-difflib/com_github_pmezard_go_difflib-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/pmezard/go-difflib/com_github_pmezard_go_difflib-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pmezard/go-difflib/com_github_pmezard_go_difflib-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pmezard/go-difflib/com_github_pmezard_go_difflib-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_polyfloyd_go_errorlint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/polyfloyd/go-errorlint",
        sha256 = "c0eeb2ec934ce4ded0816a26a3ec47b5e47bc91b4f69e348fdc0d7a8b8acdd01",
        strip_prefix = "github.com/polyfloyd/go-errorlint@v1.4.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/polyfloyd/go-errorlint/com_github_polyfloyd_go_errorlint-v1.4.2.zip",
            "http://ats.apps.svc/gomod/github.com/polyfloyd/go-errorlint/com_github_polyfloyd_go_errorlint-v1.4.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/polyfloyd/go-errorlint/com_github_polyfloyd_go_errorlint-v1.4.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/polyfloyd/go-errorlint/com_github_polyfloyd_go_errorlint-v1.4.2.zip",
        ],
    )
    go_repository(
        name = "com_github_posener_complete",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/posener/complete",
        sha256 = "828ec8cd2a7a4f57b238d7475bce89dcccf8f5dc9f55008fdc435bceeb83d927",
        strip_prefix = "github.com/posener/complete@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/posener/complete/com_github_posener_complete-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/posener/complete/com_github_posener_complete-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/posener/complete/com_github_posener_complete-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/posener/complete/com_github_posener_complete-v1.1.1.zip",
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
        name = "com_github_prometheus_client_golang",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/client_golang",
        sha256 = "0167cee686b836da39815e4a7ea64ecc245f6a3fb9b3c3f729941ed55da7dd4f",
        strip_prefix = "github.com/prometheus/client_golang@v1.16.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/client_golang/com_github_prometheus_client_golang-v1.16.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/client_golang/com_github_prometheus_client_golang-v1.16.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/client_golang/com_github_prometheus_client_golang-v1.16.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/client_golang/com_github_prometheus_client_golang-v1.16.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_client_model",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/client_model",
        sha256 = "93e312cfd357b420743c5f5534be6aed04fc860e7adb520dc2e879e055dba272",
        strip_prefix = "github.com/prometheus/client_model@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/client_model/com_github_prometheus_client_model-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/client_model/com_github_prometheus_client_model-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/client_model/com_github_prometheus_client_model-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/client_model/com_github_prometheus_client_model-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_common",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/common",
        sha256 = "a89fdf5f749cf97c576c753f9cda6f05586376843706dcf1f6c0715b58d11cc6",
        strip_prefix = "github.com/prometheus/common@v0.44.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/common/com_github_prometheus_common-v0.44.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/common/com_github_prometheus_common-v0.44.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/common/com_github_prometheus_common-v0.44.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/common/com_github_prometheus_common-v0.44.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_procfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/procfs",
        sha256 = "84e2ac7ef9d0b4dcc6090fe8fe33bab4d19de32e43d1428b77e73333a5a8f226",
        strip_prefix = "github.com/prometheus/procfs@v0.11.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/procfs/com_github_prometheus_procfs-v0.11.1.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/procfs/com_github_prometheus_procfs-v0.11.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/procfs/com_github_prometheus_procfs-v0.11.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/procfs/com_github_prometheus_procfs-v0.11.1.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_prometheus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/prometheus",
        sha256 = "de87fe7382f3fcea38548f0e8b636faffa4104264c41d7cbcb4ec243d54a898d",
        strip_prefix = "github.com/prometheus/prometheus@v0.0.0-20190525122359-d20e84d0fb64",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/prometheus/com_github_prometheus_prometheus-v0.0.0-20190525122359-d20e84d0fb64.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/prometheus/com_github_prometheus_prometheus-v0.0.0-20190525122359-d20e84d0fb64.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/prometheus/com_github_prometheus_prometheus-v0.0.0-20190525122359-d20e84d0fb64.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/prometheus/com_github_prometheus_prometheus-v0.0.0-20190525122359-d20e84d0fb64.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_tsdb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/tsdb",
        sha256 = "34e98f0e9ba55e7290774ee40569737745b395e32811e5940d2ed124a20f927c",
        strip_prefix = "github.com/prometheus/tsdb@v0.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/tsdb/com_github_prometheus_tsdb-v0.10.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/tsdb/com_github_prometheus_tsdb-v0.10.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/tsdb/com_github_prometheus_tsdb-v0.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/tsdb/com_github_prometheus_tsdb-v0.10.0.zip",
        ],
    )
    go_repository(
        name = "com_github_quasilyte_go_ruleguard",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/go-ruleguard",
        sha256 = "5b7e20b885f36c87b33a204a91b2ccbac00878f34c9ae98a4ad8c09328e920d8",
        strip_prefix = "github.com/quasilyte/go-ruleguard@v0.3.19",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/quasilyte/go-ruleguard/com_github_quasilyte_go_ruleguard-v0.3.19.zip",
            "http://ats.apps.svc/gomod/github.com/quasilyte/go-ruleguard/com_github_quasilyte_go_ruleguard-v0.3.19.zip",
            "https://cache.hawkingrei.com/gomod/github.com/quasilyte/go-ruleguard/com_github_quasilyte_go_ruleguard-v0.3.19.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/quasilyte/go-ruleguard/com_github_quasilyte_go_ruleguard-v0.3.19.zip",
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
        sha256 = "740cd5803104efadf277a0f3519f0ead97e321dbd909ceeebd7c8b6b36b44754",
        strip_prefix = "github.com/rivo/uniseg@v0.4.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/rivo/uniseg/com_github_rivo_uniseg-v0.4.4.zip",
            "http://ats.apps.svc/gomod/github.com/rivo/uniseg/com_github_rivo_uniseg-v0.4.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/rivo/uniseg/com_github_rivo_uniseg-v0.4.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/rivo/uniseg/com_github_rivo_uniseg-v0.4.4.zip",
        ],
    )
    go_repository(
        name = "com_github_rlmcpherson_s3gof3r",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rlmcpherson/s3gof3r",
        sha256 = "570e59b69f0b3a33b0c382e19c6674fc17d981dc7d2c41db2fe42510131f1423",
        strip_prefix = "github.com/rlmcpherson/s3gof3r@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/rlmcpherson/s3gof3r/com_github_rlmcpherson_s3gof3r-v0.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/rlmcpherson/s3gof3r/com_github_rlmcpherson_s3gof3r-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/rlmcpherson/s3gof3r/com_github_rlmcpherson_s3gof3r-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/rlmcpherson/s3gof3r/com_github_rlmcpherson_s3gof3r-v0.5.0.zip",
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
        name = "com_github_rubyist_circuitbreaker",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rubyist/circuitbreaker",
        sha256 = "fc1125d9260a471d349c94a251340c437f98743b42324706482596f303c28b11",
        strip_prefix = "github.com/rubyist/circuitbreaker@v2.2.1+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/rubyist/circuitbreaker/com_github_rubyist_circuitbreaker-v2.2.1+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/rubyist/circuitbreaker/com_github_rubyist_circuitbreaker-v2.2.1+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/rubyist/circuitbreaker/com_github_rubyist_circuitbreaker-v2.2.1+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/rubyist/circuitbreaker/com_github_rubyist_circuitbreaker-v2.2.1+incompatible.zip",
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
        sha256 = "eca7ce317f436d13d84c255883e6d25ccc581aa1db1cffd368fc8ec0f86473ca",
        strip_prefix = "github.com/ryanrolds/sqlclosecheck@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ryanrolds/sqlclosecheck/com_github_ryanrolds_sqlclosecheck-v0.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/ryanrolds/sqlclosecheck/com_github_ryanrolds_sqlclosecheck-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ryanrolds/sqlclosecheck/com_github_ryanrolds_sqlclosecheck-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ryanrolds/sqlclosecheck/com_github_ryanrolds_sqlclosecheck-v0.4.0.zip",
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
        name = "com_github_samuel_go_zookeeper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/samuel/go-zookeeper",
        sha256 = "229ebba6824b318d379a00d4cbaff13143ea1b93f916bf36d11054da36f39239",
        strip_prefix = "github.com/samuel/go-zookeeper@v0.0.0-20161028232340-1d7be4effb13",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/samuel/go-zookeeper/com_github_samuel_go_zookeeper-v0.0.0-20161028232340-1d7be4effb13.zip",
            "http://ats.apps.svc/gomod/github.com/samuel/go-zookeeper/com_github_samuel_go_zookeeper-v0.0.0-20161028232340-1d7be4effb13.zip",
            "https://cache.hawkingrei.com/gomod/github.com/samuel/go-zookeeper/com_github_samuel_go_zookeeper-v0.0.0-20161028232340-1d7be4effb13.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/samuel/go-zookeeper/com_github_samuel_go_zookeeper-v0.0.0-20161028232340-1d7be4effb13.zip",
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
        sha256 = "6c3f90c7947da1090f545438f4b3fd461cfeec79ee1c6e5e83a0eed7258622b1",
        strip_prefix = "github.com/sasha-s/go-deadlock@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sasha-s/go-deadlock/com_github_sasha_s_go_deadlock-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/sasha-s/go-deadlock/com_github_sasha_s_go_deadlock-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sasha-s/go-deadlock/com_github_sasha_s_go_deadlock-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sasha-s/go-deadlock/com_github_sasha_s_go_deadlock-v0.2.0.zip",
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
        sha256 = "8f67b4623bc0bf265f62fa54fdadaf7aea7796910747d20236735aca5961e60e",
        strip_prefix = "github.com/sashamelentyev/usestdlibvars@v1.23.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sashamelentyev/usestdlibvars/com_github_sashamelentyev_usestdlibvars-v1.23.0.zip",
            "http://ats.apps.svc/gomod/github.com/sashamelentyev/usestdlibvars/com_github_sashamelentyev_usestdlibvars-v1.23.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sashamelentyev/usestdlibvars/com_github_sashamelentyev_usestdlibvars-v1.23.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sashamelentyev/usestdlibvars/com_github_sashamelentyev_usestdlibvars-v1.23.0.zip",
        ],
    )
    go_repository(
        name = "com_github_satori_go_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/satori/go.uuid",
        sha256 = "4f741306a0cbe97581e34a638531bcafe3c2848150539a2ec2ba12c5e3e6cbdd",
        strip_prefix = "github.com/satori/go.uuid@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/satori/go.uuid/com_github_satori_go_uuid-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/satori/go.uuid/com_github_satori_go_uuid-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/satori/go.uuid/com_github_satori_go_uuid-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/satori/go.uuid/com_github_satori_go_uuid-v1.2.0.zip",
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
        name = "com_github_sean__seed",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sean-/seed",
        sha256 = "0bc8e6e0a07e554674b0bb92ef4eb7de1650056b50878eed8d5d631aec9b6362",
        strip_prefix = "github.com/sean-/seed@v0.0.0-20170313163322-e2103e2c3529",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/sean-/seed/com_github_sean__seed-v0.0.0-20170313163322-e2103e2c3529.zip",
            "http://ats.apps.svc/gomod/github.com/sean-/seed/com_github_sean__seed-v0.0.0-20170313163322-e2103e2c3529.zip",
            "https://cache.hawkingrei.com/gomod/github.com/sean-/seed/com_github_sean__seed-v0.0.0-20170313163322-e2103e2c3529.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/sean-/seed/com_github_sean__seed-v0.0.0-20170313163322-e2103e2c3529.zip",
        ],
    )
    go_repository(
        name = "com_github_securego_gosec_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/securego/gosec/v2",
        sha256 = "d05b561c6d3d4882fb6ccab23b5e54577ba08c56d11fffd866e8ca24e6247dd7",
        strip_prefix = "github.com/securego/gosec/v2@v2.16.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/securego/gosec/v2/com_github_securego_gosec_v2-v2.16.0.zip",
            "http://ats.apps.svc/gomod/github.com/securego/gosec/v2/com_github_securego_gosec_v2-v2.16.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/securego/gosec/v2/com_github_securego_gosec_v2-v2.16.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/securego/gosec/v2/com_github_securego_gosec_v2-v2.16.0.zip",
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
        sha256 = "2203efec94b21893ce27d09aa407deef06968f646d72c9c35b337d6a06697a17",
        strip_prefix = "github.com/shirou/gopsutil/v3@v3.23.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shirou/gopsutil/v3/com_github_shirou_gopsutil_v3-v3.23.5.zip",
            "http://ats.apps.svc/gomod/github.com/shirou/gopsutil/v3/com_github_shirou_gopsutil_v3-v3.23.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shirou/gopsutil/v3/com_github_shirou_gopsutil_v3-v3.23.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shirou/gopsutil/v3/com_github_shirou_gopsutil_v3-v3.23.5.zip",
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
        sha256 = "a2079dbd8c236262ecbb22312467265fbbddd9b5ee789531c5f7f24fbdda174b",
        strip_prefix = "github.com/shurcooL/httpfs@v0.0.0-20190707220628-8d4bc4ba7749",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/shurcooL/httpfs/com_github_shurcool_httpfs-v0.0.0-20190707220628-8d4bc4ba7749.zip",
            "http://ats.apps.svc/gomod/github.com/shurcooL/httpfs/com_github_shurcool_httpfs-v0.0.0-20190707220628-8d4bc4ba7749.zip",
            "https://cache.hawkingrei.com/gomod/github.com/shurcooL/httpfs/com_github_shurcool_httpfs-v0.0.0-20190707220628-8d4bc4ba7749.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/shurcooL/httpfs/com_github_shurcool_httpfs-v0.0.0-20190707220628-8d4bc4ba7749.zip",
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
        name = "com_github_spaolacci_murmur3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spaolacci/murmur3",
        sha256 = "60bd43ada88cc70823b31fd678a8b906d48631b47145300544d45219ee6a17bc",
        strip_prefix = "github.com/spaolacci/murmur3@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spaolacci/murmur3/com_github_spaolacci_murmur3-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/spaolacci/murmur3/com_github_spaolacci_murmur3-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spaolacci/murmur3/com_github_spaolacci_murmur3-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spaolacci/murmur3/com_github_spaolacci_murmur3-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_afero",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/afero",
        sha256 = "bb6102798d19eea4d80f8d3279a6b8fe4ec4cad46d2c90c36817ed969c2115e1",
        strip_prefix = "github.com/spf13/afero@v1.8.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spf13/afero/com_github_spf13_afero-v1.8.2.zip",
            "http://ats.apps.svc/gomod/github.com/spf13/afero/com_github_spf13_afero-v1.8.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spf13/afero/com_github_spf13_afero-v1.8.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spf13/afero/com_github_spf13_afero-v1.8.2.zip",
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
        sha256 = "9c16bb89286a9360eee6ba2c2393c38977db76ebd9a7f5d6439f3ff980315052",
        strip_prefix = "github.com/spf13/cobra@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/spf13/cobra/com_github_spf13_cobra-v1.7.0.zip",
            "http://ats.apps.svc/gomod/github.com/spf13/cobra/com_github_spf13_cobra-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/spf13/cobra/com_github_spf13_cobra-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/spf13/cobra/com_github_spf13_cobra-v1.7.0.zip",
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
        name = "com_github_stackexchange_wmi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/StackExchange/wmi",
        sha256 = "78bee244eb43b1114204ae736f28c45fade2a60dd5c84e20117939787e3cb14b",
        strip_prefix = "github.com/StackExchange/wmi@v0.0.0-20180725035823-b12b22c5341f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/StackExchange/wmi/com_github_stackexchange_wmi-v0.0.0-20180725035823-b12b22c5341f.zip",
            "http://ats.apps.svc/gomod/github.com/StackExchange/wmi/com_github_stackexchange_wmi-v0.0.0-20180725035823-b12b22c5341f.zip",
            "https://cache.hawkingrei.com/gomod/github.com/StackExchange/wmi/com_github_stackexchange_wmi-v0.0.0-20180725035823-b12b22c5341f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/StackExchange/wmi/com_github_stackexchange_wmi-v0.0.0-20180725035823-b12b22c5341f.zip",
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
        sha256 = "1a00b3bb5ad41cb72634ace06b7eb7df857404d77a7cab4e401a7c729561fe4c",
        strip_prefix = "github.com/stretchr/objx@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/stretchr/objx/com_github_stretchr_objx-v0.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/stretchr/objx/com_github_stretchr_objx-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/stretchr/objx/com_github_stretchr_objx-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/stretchr/objx/com_github_stretchr_objx-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_stretchr_testify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stretchr/testify",
        sha256 = "e206daaede0bd03de060bdfbeb984ac2c49b83058753fffc93fe0c220ea87532",
        strip_prefix = "github.com/stretchr/testify@v1.8.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/stretchr/testify/com_github_stretchr_testify-v1.8.4.zip",
            "http://ats.apps.svc/gomod/github.com/stretchr/testify/com_github_stretchr_testify-v1.8.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/stretchr/testify/com_github_stretchr_testify-v1.8.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/stretchr/testify/com_github_stretchr_testify-v1.8.4.zip",
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
        sha256 = "f207e54086db6ac4874ecb43ab6a2303e147f159485512c8b9f1915bff190174",
        strip_prefix = "github.com/tetafro/godot@v1.4.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tetafro/godot/com_github_tetafro_godot-v1.4.11.zip",
            "http://ats.apps.svc/gomod/github.com/tetafro/godot/com_github_tetafro_godot-v1.4.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tetafro/godot/com_github_tetafro_godot-v1.4.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tetafro/godot/com_github_tetafro_godot-v1.4.11.zip",
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
        name = "com_github_tikv_client_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tikv/client-go/v2",
        sha256 = "608e5c393dcf7fa07a7a360333816dc479b05bad6ad489a4643c9a096e47f5d9",
        strip_prefix = "github.com/tikv/client-go/v2@v2.0.8-0.20230811033710-8a214402da13",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tikv/client-go/v2/com_github_tikv_client_go_v2-v2.0.8-0.20230811033710-8a214402da13.zip",
            "http://ats.apps.svc/gomod/github.com/tikv/client-go/v2/com_github_tikv_client_go_v2-v2.0.8-0.20230811033710-8a214402da13.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tikv/client-go/v2/com_github_tikv_client_go_v2-v2.0.8-0.20230811033710-8a214402da13.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tikv/client-go/v2/com_github_tikv_client_go_v2-v2.0.8-0.20230811033710-8a214402da13.zip",
        ],
    )
    go_repository(
        name = "com_github_tikv_pd_client",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tikv/pd/client",
        sha256 = "840e8de75f3acfbfaebea975ec2d7f8db2aa1150c9ba1f4c37e9aa60d2f90480",
        strip_prefix = "github.com/tikv/pd/client@v0.0.0-20230728033905-31343e006842",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tikv/pd/client/com_github_tikv_pd_client-v0.0.0-20230728033905-31343e006842.zip",
            "http://ats.apps.svc/gomod/github.com/tikv/pd/client/com_github_tikv_pd_client-v0.0.0-20230728033905-31343e006842.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tikv/pd/client/com_github_tikv_pd_client-v0.0.0-20230728033905-31343e006842.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tikv/pd/client/com_github_tikv_pd_client-v0.0.0-20230728033905-31343e006842.zip",
        ],
    )
    go_repository(
        name = "com_github_timakin_bodyclose",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/timakin/bodyclose",
        sha256 = "0d3122d8d014d1d279f064b8affbaa0c4b24b196111cdd889221d57f7beda7a9",
        strip_prefix = "github.com/timakin/bodyclose@v0.0.0-20230421092635-574207250966",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/timakin/bodyclose/com_github_timakin_bodyclose-v0.0.0-20230421092635-574207250966.zip",
            "http://ats.apps.svc/gomod/github.com/timakin/bodyclose/com_github_timakin_bodyclose-v0.0.0-20230421092635-574207250966.zip",
            "https://cache.hawkingrei.com/gomod/github.com/timakin/bodyclose/com_github_timakin_bodyclose-v0.0.0-20230421092635-574207250966.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/timakin/bodyclose/com_github_timakin_bodyclose-v0.0.0-20230421092635-574207250966.zip",
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
        sha256 = "89b4468aaa5e5ae91cfb1638df618f1fe8aa92354c70dad1af9d82c6fd0c51cd",
        strip_prefix = "github.com/tklauser/go-sysconf@v0.3.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tklauser/go-sysconf/com_github_tklauser_go_sysconf-v0.3.11.zip",
            "http://ats.apps.svc/gomod/github.com/tklauser/go-sysconf/com_github_tklauser_go_sysconf-v0.3.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tklauser/go-sysconf/com_github_tklauser_go_sysconf-v0.3.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tklauser/go-sysconf/com_github_tklauser_go_sysconf-v0.3.11.zip",
        ],
    )
    go_repository(
        name = "com_github_tklauser_numcpus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tklauser/numcpus",
        sha256 = "b9c629ac8c472aeb85c5020141c23a1866927209ff9c9be867cac31e8987b451",
        strip_prefix = "github.com/tklauser/numcpus@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tklauser/numcpus/com_github_tklauser_numcpus-v0.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/tklauser/numcpus/com_github_tklauser_numcpus-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tklauser/numcpus/com_github_tklauser_numcpus-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tklauser/numcpus/com_github_tklauser_numcpus-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_tmc_grpc_websocket_proxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tmc/grpc-websocket-proxy",
        sha256 = "51b0ffa58e0c4daa5048086d39bf8cad93d4af2c65aae229c6d8912210668164",
        strip_prefix = "github.com/tmc/grpc-websocket-proxy@v0.0.0-20201229170055-e5319fda7802",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tmc/grpc-websocket-proxy/com_github_tmc_grpc_websocket_proxy-v0.0.0-20201229170055-e5319fda7802.zip",
            "http://ats.apps.svc/gomod/github.com/tmc/grpc-websocket-proxy/com_github_tmc_grpc_websocket_proxy-v0.0.0-20201229170055-e5319fda7802.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tmc/grpc-websocket-proxy/com_github_tmc_grpc_websocket_proxy-v0.0.0-20201229170055-e5319fda7802.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tmc/grpc-websocket-proxy/com_github_tmc_grpc_websocket_proxy-v0.0.0-20201229170055-e5319fda7802.zip",
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
        sha256 = "47979840aa9e7e0c39b1572364a1b0da4dd3b33e65e4d80ef9892e80dbd717ff",
        strip_prefix = "github.com/ultraware/funlen@v0.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ultraware/funlen/com_github_ultraware_funlen-v0.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/ultraware/funlen/com_github_ultraware_funlen-v0.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ultraware/funlen/com_github_ultraware_funlen-v0.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ultraware/funlen/com_github_ultraware_funlen-v0.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_ultraware_whitespace",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ultraware/whitespace",
        sha256 = "9cc8a20084d1fdea52b1177268edeb0f61f188365307ed720bab9ef5b04267f8",
        strip_prefix = "github.com/ultraware/whitespace@v0.0.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ultraware/whitespace/com_github_ultraware_whitespace-v0.0.5.zip",
            "http://ats.apps.svc/gomod/github.com/ultraware/whitespace/com_github_ultraware_whitespace-v0.0.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ultraware/whitespace/com_github_ultraware_whitespace-v0.0.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ultraware/whitespace/com_github_ultraware_whitespace-v0.0.5.zip",
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
        sha256 = "08c44ae19834c348c6e3b56c0b70ce50bfbf0030f9d2b6d796e4de7619268ae9",
        strip_prefix = "github.com/uudashr/gocognit@v1.0.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/uudashr/gocognit/com_github_uudashr_gocognit-v1.0.6.zip",
            "http://ats.apps.svc/gomod/github.com/uudashr/gocognit/com_github_uudashr_gocognit-v1.0.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/uudashr/gocognit/com_github_uudashr_gocognit-v1.0.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/uudashr/gocognit/com_github_uudashr_gocognit-v1.0.6.zip",
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
        sha256 = "af0cb23fbeefd7a4982cf45ba15f658c187a4050391eb2a2d9e2d4677af87042",
        strip_prefix = "github.com/xen0n/gosmopolitan@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xen0n/gosmopolitan/com_github_xen0n_gosmopolitan-v1.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/xen0n/gosmopolitan/com_github_xen0n_gosmopolitan-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xen0n/gosmopolitan/com_github_xen0n_gosmopolitan-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xen0n/gosmopolitan/com_github_xen0n_gosmopolitan-v1.2.1.zip",
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
        sha256 = "437bdc666239fda4581b592b068001f08269c68c70699a721bff9334412d4181",
        strip_prefix = "github.com/xiang90/probing@v0.0.0-20190116061207-43a291ad63a2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/xiang90/probing/com_github_xiang90_probing-v0.0.0-20190116061207-43a291ad63a2.zip",
            "http://ats.apps.svc/gomod/github.com/xiang90/probing/com_github_xiang90_probing-v0.0.0-20190116061207-43a291ad63a2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/xiang90/probing/com_github_xiang90_probing-v0.0.0-20190116061207-43a291ad63a2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/xiang90/probing/com_github_xiang90_probing-v0.0.0-20190116061207-43a291ad63a2.zip",
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
        sha256 = "6c7de865179b66a78d61441c93a103af616971ec88df50b55e8ab08379bb35ad",
        strip_prefix = "github.com/ykadowak/zerologlint@v0.1.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ykadowak/zerologlint/com_github_ykadowak_zerologlint-v0.1.2.zip",
            "http://ats.apps.svc/gomod/github.com/ykadowak/zerologlint/com_github_ykadowak_zerologlint-v0.1.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ykadowak/zerologlint/com_github_ykadowak_zerologlint-v0.1.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ykadowak/zerologlint/com_github_ykadowak_zerologlint-v0.1.2.zip",
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
        sha256 = "beb676d2dd396a7634c96a7b93f09198a8463075b65d290edef1c55f52855568",
        strip_prefix = "gitlab.com/bosi/decorder@v0.2.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gitlab.com/bosi/decorder/com_gitlab_bosi_decorder-v0.2.3.zip",
            "http://ats.apps.svc/gomod/gitlab.com/bosi/decorder/com_gitlab_bosi_decorder-v0.2.3.zip",
            "https://cache.hawkingrei.com/gomod/gitlab.com/bosi/decorder/com_gitlab_bosi_decorder-v0.2.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gitlab.com/bosi/decorder/com_gitlab_bosi_decorder-v0.2.3.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go",
        sha256 = "8bdce0d7bfc07e71cebbbd7df2d93d1418a35eed09211bb21e3c1ee8d2fabf7c",
        strip_prefix = "cloud.google.com/go@v0.110.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/com_google_cloud_go-v0.110.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/com_google_cloud_go-v0.110.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/com_google_cloud_go-v0.110.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/com_google_cloud_go-v0.110.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_accessapproval",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/accessapproval",
        sha256 = "4fd31c02273e95e4032c7652822e740dbf074d77d66002df0fb96c1222fd0d1e",
        strip_prefix = "cloud.google.com/go/accessapproval@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/accessapproval/com_google_cloud_go_accessapproval-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/accessapproval/com_google_cloud_go_accessapproval-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/accessapproval/com_google_cloud_go_accessapproval-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/accessapproval/com_google_cloud_go_accessapproval-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_accesscontextmanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/accesscontextmanager",
        sha256 = "90230ccc20b02821de0ef578914c7c32ac3189ebcce539da521228df768fa4f1",
        strip_prefix = "cloud.google.com/go/accesscontextmanager@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/accesscontextmanager/com_google_cloud_go_accesscontextmanager-v1.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/accesscontextmanager/com_google_cloud_go_accesscontextmanager-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/accesscontextmanager/com_google_cloud_go_accesscontextmanager-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/accesscontextmanager/com_google_cloud_go_accesscontextmanager-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_aiplatform",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/aiplatform",
        sha256 = "e61385ceceb7eb9ef93c80daf51787f083470f104d113c8460794744a853c927",
        strip_prefix = "cloud.google.com/go/aiplatform@v1.37.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/aiplatform/com_google_cloud_go_aiplatform-v1.37.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/aiplatform/com_google_cloud_go_aiplatform-v1.37.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/aiplatform/com_google_cloud_go_aiplatform-v1.37.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/aiplatform/com_google_cloud_go_aiplatform-v1.37.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_analytics",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/analytics",
        sha256 = "b2c08e99d317393ea9102cbb4f309d16170790a793b95eeafd026f8263281b3f",
        strip_prefix = "cloud.google.com/go/analytics@v0.19.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/analytics/com_google_cloud_go_analytics-v0.19.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/analytics/com_google_cloud_go_analytics-v0.19.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/analytics/com_google_cloud_go_analytics-v0.19.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/analytics/com_google_cloud_go_analytics-v0.19.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_apigateway",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigateway",
        sha256 = "81f9cf7d46093a4cf3bb6dfb7ea942784295f093261c45698656dd844bdfa163",
        strip_prefix = "cloud.google.com/go/apigateway@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/apigateway/com_google_cloud_go_apigateway-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/apigateway/com_google_cloud_go_apigateway-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/apigateway/com_google_cloud_go_apigateway-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/apigateway/com_google_cloud_go_apigateway-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_apigeeconnect",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigeeconnect",
        sha256 = "a0ae141afd9c762b722778b3508dcc459e18c6890a22586235dafc0f436532a2",
        strip_prefix = "cloud.google.com/go/apigeeconnect@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/apigeeconnect/com_google_cloud_go_apigeeconnect-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/apigeeconnect/com_google_cloud_go_apigeeconnect-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/apigeeconnect/com_google_cloud_go_apigeeconnect-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/apigeeconnect/com_google_cloud_go_apigeeconnect-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_apigeeregistry",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigeeregistry",
        sha256 = "1cf7728c1b8d31247d5c2ec10b4b252d6224e9549c2ee7d2222b482dec8aeba4",
        strip_prefix = "cloud.google.com/go/apigeeregistry@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/apigeeregistry/com_google_cloud_go_apigeeregistry-v0.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/apigeeregistry/com_google_cloud_go_apigeeregistry-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/apigeeregistry/com_google_cloud_go_apigeeregistry-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/apigeeregistry/com_google_cloud_go_apigeeregistry-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_apikeys",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apikeys",
        sha256 = "511ba83f3837459a9e553026ecf556ebec9007403054635d90f065f7d735ddbe",
        strip_prefix = "cloud.google.com/go/apikeys@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/apikeys/com_google_cloud_go_apikeys-v0.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/apikeys/com_google_cloud_go_apikeys-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/apikeys/com_google_cloud_go_apikeys-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/apikeys/com_google_cloud_go_apikeys-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_appengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/appengine",
        sha256 = "09f35ee5b9d8782bced76b733c7c3a2a5f3b9e41630236a47854b4a92567e646",
        strip_prefix = "cloud.google.com/go/appengine@v1.7.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/appengine/com_google_cloud_go_appengine-v1.7.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/appengine/com_google_cloud_go_appengine-v1.7.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/appengine/com_google_cloud_go_appengine-v1.7.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/appengine/com_google_cloud_go_appengine-v1.7.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_area120",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/area120",
        sha256 = "7dcfdf365eb9f29fcedf29b8e32f0023b829732869dc7ad9a2cd8450cbdea8df",
        strip_prefix = "cloud.google.com/go/area120@v0.7.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/area120/com_google_cloud_go_area120-v0.7.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/area120/com_google_cloud_go_area120-v0.7.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/area120/com_google_cloud_go_area120-v0.7.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/area120/com_google_cloud_go_area120-v0.7.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_artifactregistry",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/artifactregistry",
        sha256 = "abf73586bdced0f590918b37f19643646c3aa04a651480cbdbfad86171f03d98",
        strip_prefix = "cloud.google.com/go/artifactregistry@v1.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/artifactregistry/com_google_cloud_go_artifactregistry-v1.13.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/artifactregistry/com_google_cloud_go_artifactregistry-v1.13.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/artifactregistry/com_google_cloud_go_artifactregistry-v1.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/artifactregistry/com_google_cloud_go_artifactregistry-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_asset",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/asset",
        sha256 = "dcaee2c49835e7f9c53d77b21738d4d803e25b2b52dc4c71c5e145332fead841",
        strip_prefix = "cloud.google.com/go/asset@v1.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/asset/com_google_cloud_go_asset-v1.13.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/asset/com_google_cloud_go_asset-v1.13.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/asset/com_google_cloud_go_asset-v1.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/asset/com_google_cloud_go_asset-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_assuredworkloads",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/assuredworkloads",
        sha256 = "f82b2f4ba2d692deff3ccf7dacfc23e744d70804f55fbb34affee7552da4f730",
        strip_prefix = "cloud.google.com/go/assuredworkloads@v1.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/assuredworkloads/com_google_cloud_go_assuredworkloads-v1.10.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/assuredworkloads/com_google_cloud_go_assuredworkloads-v1.10.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/assuredworkloads/com_google_cloud_go_assuredworkloads-v1.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/assuredworkloads/com_google_cloud_go_assuredworkloads-v1.10.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_automl",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/automl",
        sha256 = "e8a1b910ab247a441ad74592d93d4c37721d7ecfde2dcd7afceeaffab0505574",
        strip_prefix = "cloud.google.com/go/automl@v1.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/automl/com_google_cloud_go_automl-v1.12.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/automl/com_google_cloud_go_automl-v1.12.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/automl/com_google_cloud_go_automl-v1.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/automl/com_google_cloud_go_automl-v1.12.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_baremetalsolution",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/baremetalsolution",
        sha256 = "f3bdfc95c4743654198599087e86063428d823b10c8f4b59260376255403d3a6",
        strip_prefix = "cloud.google.com/go/baremetalsolution@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/baremetalsolution/com_google_cloud_go_baremetalsolution-v0.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/baremetalsolution/com_google_cloud_go_baremetalsolution-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/baremetalsolution/com_google_cloud_go_baremetalsolution-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/baremetalsolution/com_google_cloud_go_baremetalsolution-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_batch",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/batch",
        sha256 = "9b7fda9ddd263f3cb57afe020014bb4153736e13656dd39896088bda972b3f8c",
        strip_prefix = "cloud.google.com/go/batch@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/batch/com_google_cloud_go_batch-v0.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/batch/com_google_cloud_go_batch-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/batch/com_google_cloud_go_batch-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/batch/com_google_cloud_go_batch-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_beyondcorp",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/beyondcorp",
        sha256 = "6ff3ee86f910355281d4fccbf476922447ea6ba33579e5d40c7dcec407dfdf1a",
        strip_prefix = "cloud.google.com/go/beyondcorp@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/beyondcorp/com_google_cloud_go_beyondcorp-v0.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/beyondcorp/com_google_cloud_go_beyondcorp-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/beyondcorp/com_google_cloud_go_beyondcorp-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/beyondcorp/com_google_cloud_go_beyondcorp-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/bigquery",
        sha256 = "3866e7d059fb9fb91f5323bc2061aded6834162d76e476da27ab64e48c2a6755",
        strip_prefix = "cloud.google.com/go/bigquery@v1.50.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/bigquery/com_google_cloud_go_bigquery-v1.50.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/bigquery/com_google_cloud_go_bigquery-v1.50.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/bigquery/com_google_cloud_go_bigquery-v1.50.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/bigquery/com_google_cloud_go_bigquery-v1.50.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_billing",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/billing",
        sha256 = "6a1422bb60b43683d1b5d1be3eacd1992b1bb656e187cec3e398c9d27299eadb",
        strip_prefix = "cloud.google.com/go/billing@v1.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/billing/com_google_cloud_go_billing-v1.13.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/billing/com_google_cloud_go_billing-v1.13.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/billing/com_google_cloud_go_billing-v1.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/billing/com_google_cloud_go_billing-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_binaryauthorization",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/binaryauthorization",
        sha256 = "4a5d9c61a748d7b2dc14542c66f033701694e537b954619fb70f53aa1f31263f",
        strip_prefix = "cloud.google.com/go/binaryauthorization@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/binaryauthorization/com_google_cloud_go_binaryauthorization-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/binaryauthorization/com_google_cloud_go_binaryauthorization-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/binaryauthorization/com_google_cloud_go_binaryauthorization-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/binaryauthorization/com_google_cloud_go_binaryauthorization-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_certificatemanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/certificatemanager",
        sha256 = "28c924f5edcc34f79ae7e7542a0179b0f49457f9ce6e89c86336fe5be2fdb8ac",
        strip_prefix = "cloud.google.com/go/certificatemanager@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/certificatemanager/com_google_cloud_go_certificatemanager-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/certificatemanager/com_google_cloud_go_certificatemanager-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/certificatemanager/com_google_cloud_go_certificatemanager-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/certificatemanager/com_google_cloud_go_certificatemanager-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_channel",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/channel",
        sha256 = "097f8225139cc2f3d4676e6b78d1d4cdbfd0f5558e1ab3a66ded9a085700d4b2",
        strip_prefix = "cloud.google.com/go/channel@v1.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/channel/com_google_cloud_go_channel-v1.12.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/channel/com_google_cloud_go_channel-v1.12.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/channel/com_google_cloud_go_channel-v1.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/channel/com_google_cloud_go_channel-v1.12.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_cloudbuild",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/cloudbuild",
        sha256 = "80d00c57b4b55e71e45e4c7427ee0da0aae082fc0b7be0fcdc2d756a71b9d8b3",
        strip_prefix = "cloud.google.com/go/cloudbuild@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/cloudbuild/com_google_cloud_go_cloudbuild-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/cloudbuild/com_google_cloud_go_cloudbuild-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/cloudbuild/com_google_cloud_go_cloudbuild-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/cloudbuild/com_google_cloud_go_cloudbuild-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_clouddms",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/clouddms",
        sha256 = "9a9488b44e7a18811c0fcb13beb1fe9c3c5f7613b3109734af6f88af19843d90",
        strip_prefix = "cloud.google.com/go/clouddms@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/clouddms/com_google_cloud_go_clouddms-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/clouddms/com_google_cloud_go_clouddms-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/clouddms/com_google_cloud_go_clouddms-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/clouddms/com_google_cloud_go_clouddms-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_cloudtasks",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/cloudtasks",
        sha256 = "9219724339007e7278d19a293285dcb45f4a38addc31d9722c98ce0b8095efe5",
        strip_prefix = "cloud.google.com/go/cloudtasks@v1.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/cloudtasks/com_google_cloud_go_cloudtasks-v1.10.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/cloudtasks/com_google_cloud_go_cloudtasks-v1.10.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/cloudtasks/com_google_cloud_go_cloudtasks-v1.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/cloudtasks/com_google_cloud_go_cloudtasks-v1.10.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/compute",
        sha256 = "789696687da53dd22d22c5c49e0cc0636a44703459992236d18495e79d9b9c03",
        strip_prefix = "cloud.google.com/go/compute@v1.19.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/compute/com_google_cloud_go_compute-v1.19.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/compute/com_google_cloud_go_compute-v1.19.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/compute/com_google_cloud_go_compute-v1.19.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/compute/com_google_cloud_go_compute-v1.19.0.zip",
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
        sha256 = "e06630e09b6ee01e3693ff079ee6279de32566ae29fefeacdd410c61e1a1a5fe",
        strip_prefix = "cloud.google.com/go/contactcenterinsights@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/contactcenterinsights/com_google_cloud_go_contactcenterinsights-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/contactcenterinsights/com_google_cloud_go_contactcenterinsights-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/contactcenterinsights/com_google_cloud_go_contactcenterinsights-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/contactcenterinsights/com_google_cloud_go_contactcenterinsights-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_container",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/container",
        sha256 = "2dfba11e311b5dc9ea7e8b60cfd2dff3b060564a845bdac98945173dc3ef12ac",
        strip_prefix = "cloud.google.com/go/container@v1.15.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/container/com_google_cloud_go_container-v1.15.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/container/com_google_cloud_go_container-v1.15.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/container/com_google_cloud_go_container-v1.15.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/container/com_google_cloud_go_container-v1.15.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_containeranalysis",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/containeranalysis",
        sha256 = "6319d5102b56fa4c4576fb3aa9b4aeb30f1c3f5e45bccd747d0da27ccfceb147",
        strip_prefix = "cloud.google.com/go/containeranalysis@v0.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/containeranalysis/com_google_cloud_go_containeranalysis-v0.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/containeranalysis/com_google_cloud_go_containeranalysis-v0.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/containeranalysis/com_google_cloud_go_containeranalysis-v0.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/containeranalysis/com_google_cloud_go_containeranalysis-v0.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datacatalog",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datacatalog",
        sha256 = "2e79aaa321c13a3cd5d536aa5d8d295afacb03752862c4e78bcfc8ce99501ca6",
        strip_prefix = "cloud.google.com/go/datacatalog@v1.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datacatalog/com_google_cloud_go_datacatalog-v1.13.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datacatalog/com_google_cloud_go_datacatalog-v1.13.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datacatalog/com_google_cloud_go_datacatalog-v1.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datacatalog/com_google_cloud_go_datacatalog-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataflow",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataflow",
        sha256 = "f20f98ca4fb97f9c027f2e56edf7effe2c95f59d7d5a230dfa3be525fa130595",
        strip_prefix = "cloud.google.com/go/dataflow@v0.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataflow/com_google_cloud_go_dataflow-v0.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataflow/com_google_cloud_go_dataflow-v0.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataflow/com_google_cloud_go_dataflow-v0.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataflow/com_google_cloud_go_dataflow-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataform",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataform",
        sha256 = "2867f6d78bb34adf8e295fb2158ad2df352cd28d79aa0c6e509dd5a389e04692",
        strip_prefix = "cloud.google.com/go/dataform@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataform/com_google_cloud_go_dataform-v0.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataform/com_google_cloud_go_dataform-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataform/com_google_cloud_go_dataform-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataform/com_google_cloud_go_dataform-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datafusion",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datafusion",
        sha256 = "9d12d5f177f6db6980afa69a9547e7653276bbb85821404d8856d432c56706bb",
        strip_prefix = "cloud.google.com/go/datafusion@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datafusion/com_google_cloud_go_datafusion-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datafusion/com_google_cloud_go_datafusion-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datafusion/com_google_cloud_go_datafusion-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datafusion/com_google_cloud_go_datafusion-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datalabeling",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datalabeling",
        sha256 = "9a7084aa65112251f45ed12f3118a33667fb5e90bbd14ddc64c9c64655aee9f0",
        strip_prefix = "cloud.google.com/go/datalabeling@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datalabeling/com_google_cloud_go_datalabeling-v0.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datalabeling/com_google_cloud_go_datalabeling-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datalabeling/com_google_cloud_go_datalabeling-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datalabeling/com_google_cloud_go_datalabeling-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataplex",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataplex",
        sha256 = "047519cc76aedf7b0ddb4e3145d9e96d88bc10776ef9252daa43acd25c367911",
        strip_prefix = "cloud.google.com/go/dataplex@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataplex/com_google_cloud_go_dataplex-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataplex/com_google_cloud_go_dataplex-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataplex/com_google_cloud_go_dataplex-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataplex/com_google_cloud_go_dataplex-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataproc",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataproc",
        sha256 = "f4adc94c30406a2bd04b62f2a0c8c33ddb605ffda53024b034e5c136407f0c73",
        strip_prefix = "cloud.google.com/go/dataproc@v1.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataproc/com_google_cloud_go_dataproc-v1.12.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataproc/com_google_cloud_go_dataproc-v1.12.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataproc/com_google_cloud_go_dataproc-v1.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataproc/com_google_cloud_go_dataproc-v1.12.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dataqna",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataqna",
        sha256 = "20e60cfe78e1b2f72122cf44184d8e9a9af7bdfc9e44a2c33e4b782dee477d25",
        strip_prefix = "cloud.google.com/go/dataqna@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dataqna/com_google_cloud_go_dataqna-v0.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dataqna/com_google_cloud_go_dataqna-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dataqna/com_google_cloud_go_dataqna-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dataqna/com_google_cloud_go_dataqna-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datastore",
        sha256 = "6b81cf09ce8daee02c880343ff82acfefbd3c7b67ff2b93bf9f1479c5e25f627",
        strip_prefix = "cloud.google.com/go/datastore@v1.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datastore/com_google_cloud_go_datastore-v1.11.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datastore/com_google_cloud_go_datastore-v1.11.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datastore/com_google_cloud_go_datastore-v1.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datastore/com_google_cloud_go_datastore-v1.11.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datastream",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datastream",
        sha256 = "02571fbbe7aa5052c91c2b99f3c799dc278bbe001871036101959338e789800c",
        strip_prefix = "cloud.google.com/go/datastream@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/datastream/com_google_cloud_go_datastream-v1.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/datastream/com_google_cloud_go_datastream-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/datastream/com_google_cloud_go_datastream-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/datastream/com_google_cloud_go_datastream-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_deploy",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/deploy",
        sha256 = "9bf6d2ad426d9d80636ca5b7c1486b91a8e31c61a50a79856195fdad65bda004",
        strip_prefix = "cloud.google.com/go/deploy@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/deploy/com_google_cloud_go_deploy-v1.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/deploy/com_google_cloud_go_deploy-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/deploy/com_google_cloud_go_deploy-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/deploy/com_google_cloud_go_deploy-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dialogflow",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dialogflow",
        sha256 = "de2009a08b3db53b7292852a7c28dd52218c8fcb7937fc0049b0219e429bafdb",
        strip_prefix = "cloud.google.com/go/dialogflow@v1.32.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dialogflow/com_google_cloud_go_dialogflow-v1.32.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dialogflow/com_google_cloud_go_dialogflow-v1.32.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dialogflow/com_google_cloud_go_dialogflow-v1.32.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dialogflow/com_google_cloud_go_dialogflow-v1.32.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_dlp",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dlp",
        sha256 = "a32c4dbda0445a401ec25e9faf3f10b25b6fd264917825a0d053e6e297cdfc61",
        strip_prefix = "cloud.google.com/go/dlp@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/dlp/com_google_cloud_go_dlp-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/dlp/com_google_cloud_go_dlp-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/dlp/com_google_cloud_go_dlp-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/dlp/com_google_cloud_go_dlp-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_documentai",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/documentai",
        sha256 = "9806274a2a5af71b115ddc7357be24757b0331b1661cac642f7d0eb6b6894a7b",
        strip_prefix = "cloud.google.com/go/documentai@v1.18.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/documentai/com_google_cloud_go_documentai-v1.18.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/documentai/com_google_cloud_go_documentai-v1.18.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/documentai/com_google_cloud_go_documentai-v1.18.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/documentai/com_google_cloud_go_documentai-v1.18.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_domains",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/domains",
        sha256 = "26ed447b319c064d0ce19d85c6de127af1aa87c727af6202b1f7a3b95d35bd0a",
        strip_prefix = "cloud.google.com/go/domains@v0.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/domains/com_google_cloud_go_domains-v0.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/domains/com_google_cloud_go_domains-v0.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/domains/com_google_cloud_go_domains-v0.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/domains/com_google_cloud_go_domains-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_edgecontainer",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/edgecontainer",
        sha256 = "c22e2f212fcfcf9f0af32c43c47b4311fc07c382e78810a34afe273ba363429c",
        strip_prefix = "cloud.google.com/go/edgecontainer@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/edgecontainer/com_google_cloud_go_edgecontainer-v1.0.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/edgecontainer/com_google_cloud_go_edgecontainer-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/edgecontainer/com_google_cloud_go_edgecontainer-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/edgecontainer/com_google_cloud_go_edgecontainer-v1.0.0.zip",
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
        sha256 = "b595846269076fbabcee96eda6718c41c1b94c2758edc42537f490accaa40b19",
        strip_prefix = "cloud.google.com/go/essentialcontacts@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/essentialcontacts/com_google_cloud_go_essentialcontacts-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/essentialcontacts/com_google_cloud_go_essentialcontacts-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/essentialcontacts/com_google_cloud_go_essentialcontacts-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/essentialcontacts/com_google_cloud_go_essentialcontacts-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_eventarc",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/eventarc",
        sha256 = "6bdda029e620653f4dcdc10fa1099ec6b28c0e5ecbb5c1b34b58374efcc1beec",
        strip_prefix = "cloud.google.com/go/eventarc@v1.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/eventarc/com_google_cloud_go_eventarc-v1.11.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/eventarc/com_google_cloud_go_eventarc-v1.11.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/eventarc/com_google_cloud_go_eventarc-v1.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/eventarc/com_google_cloud_go_eventarc-v1.11.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_filestore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/filestore",
        sha256 = "77c99a79955f99b33988d4ce7d4656ab3bbeaef794d788ae295eccdecf799839",
        strip_prefix = "cloud.google.com/go/filestore@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/filestore/com_google_cloud_go_filestore-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/filestore/com_google_cloud_go_filestore-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/filestore/com_google_cloud_go_filestore-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/filestore/com_google_cloud_go_filestore-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/firestore",
        sha256 = "f4bd0f35095358181574ae03a8bed7618fe8f50a63d54b2e49a85d71c47104c7",
        strip_prefix = "cloud.google.com/go/firestore@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/firestore/com_google_cloud_go_firestore-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/firestore/com_google_cloud_go_firestore-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/firestore/com_google_cloud_go_firestore-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/firestore/com_google_cloud_go_firestore-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_functions",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/functions",
        sha256 = "9635cbe16b0bf748108ce30c4686a909227d342e2ed47c1c1c45cfaa44be6d89",
        strip_prefix = "cloud.google.com/go/functions@v1.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/functions/com_google_cloud_go_functions-v1.13.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/functions/com_google_cloud_go_functions-v1.13.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/functions/com_google_cloud_go_functions-v1.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/functions/com_google_cloud_go_functions-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gaming",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gaming",
        sha256 = "5a0680fb577f1ea1d3e815ff2e7fa22931e2c9e492e151087cdef34b1f9ece97",
        strip_prefix = "cloud.google.com/go/gaming@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gaming/com_google_cloud_go_gaming-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gaming/com_google_cloud_go_gaming-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gaming/com_google_cloud_go_gaming-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gaming/com_google_cloud_go_gaming-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gkebackup",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkebackup",
        sha256 = "d7a06be74c96d73dc3f032431cffd1e01656c670ed85d70da916933b4a91d85d",
        strip_prefix = "cloud.google.com/go/gkebackup@v0.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gkebackup/com_google_cloud_go_gkebackup-v0.4.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gkebackup/com_google_cloud_go_gkebackup-v0.4.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gkebackup/com_google_cloud_go_gkebackup-v0.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gkebackup/com_google_cloud_go_gkebackup-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gkeconnect",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkeconnect",
        sha256 = "37fe8da6dd9a04e90a245093f72b30dae67d511ab13a6c24db25b3ee8c547d25",
        strip_prefix = "cloud.google.com/go/gkeconnect@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gkeconnect/com_google_cloud_go_gkeconnect-v0.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gkeconnect/com_google_cloud_go_gkeconnect-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gkeconnect/com_google_cloud_go_gkeconnect-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gkeconnect/com_google_cloud_go_gkeconnect-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gkehub",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkehub",
        sha256 = "e44073c24ed21976762f6a13f0adad46863eec5ac1dbaa20045fc0b63e1fd2ce",
        strip_prefix = "cloud.google.com/go/gkehub@v0.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gkehub/com_google_cloud_go_gkehub-v0.12.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gkehub/com_google_cloud_go_gkehub-v0.12.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gkehub/com_google_cloud_go_gkehub-v0.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gkehub/com_google_cloud_go_gkehub-v0.12.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gkemulticloud",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkemulticloud",
        sha256 = "9c851d037561d6cc67c20b247c505ca9c0697dc7e85251bd756f478f473483b1",
        strip_prefix = "cloud.google.com/go/gkemulticloud@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gkemulticloud/com_google_cloud_go_gkemulticloud-v0.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gkemulticloud/com_google_cloud_go_gkemulticloud-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gkemulticloud/com_google_cloud_go_gkemulticloud-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gkemulticloud/com_google_cloud_go_gkemulticloud-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_gsuiteaddons",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gsuiteaddons",
        sha256 = "911963d78ba7974bd3e807888fde1879a5c871cdf3c43369eebb9778a3fdc4c1",
        strip_prefix = "cloud.google.com/go/gsuiteaddons@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/gsuiteaddons/com_google_cloud_go_gsuiteaddons-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/gsuiteaddons/com_google_cloud_go_gsuiteaddons-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/gsuiteaddons/com_google_cloud_go_gsuiteaddons-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/gsuiteaddons/com_google_cloud_go_gsuiteaddons-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iam",
        sha256 = "a8236c53eb06cc21c5c972fcfc4153fbce5a44eb7a1b7c88cadc307b8768328a",
        strip_prefix = "cloud.google.com/go/iam@v0.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/iam/com_google_cloud_go_iam-v0.13.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/iam/com_google_cloud_go_iam-v0.13.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/iam/com_google_cloud_go_iam-v0.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/iam/com_google_cloud_go_iam-v0.13.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_iap",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iap",
        sha256 = "c2e76b45c74ecebad179dca0398a5279bcf47d30c35d8c347c8d59d98f944f90",
        strip_prefix = "cloud.google.com/go/iap@v1.7.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/iap/com_google_cloud_go_iap-v1.7.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/iap/com_google_cloud_go_iap-v1.7.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/iap/com_google_cloud_go_iap-v1.7.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/iap/com_google_cloud_go_iap-v1.7.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_ids",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/ids",
        sha256 = "8a684da48da978ae35937cb3b9a84da1a7673789e8363501ccc317108b712913",
        strip_prefix = "cloud.google.com/go/ids@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/ids/com_google_cloud_go_ids-v1.3.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/ids/com_google_cloud_go_ids-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/ids/com_google_cloud_go_ids-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/ids/com_google_cloud_go_ids-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_iot",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iot",
        sha256 = "960bf7d2c22c0c31d9d903343672d1e949d2bb1442264c15d9de57659b51e126",
        strip_prefix = "cloud.google.com/go/iot@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/iot/com_google_cloud_go_iot-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/iot/com_google_cloud_go_iot-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/iot/com_google_cloud_go_iot-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/iot/com_google_cloud_go_iot-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/kms",
        sha256 = "7f54a8218570636a93ea8b33843ed179b4b881f7d5aa8982912ddfdf7090ba38",
        strip_prefix = "cloud.google.com/go/kms@v1.10.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/kms/com_google_cloud_go_kms-v1.10.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/kms/com_google_cloud_go_kms-v1.10.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/kms/com_google_cloud_go_kms-v1.10.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/kms/com_google_cloud_go_kms-v1.10.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_language",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/language",
        sha256 = "c66908967b2558c00ca79b31f6788a1cd5f7ba9ee24ebe109ea3b4ac1ab372a1",
        strip_prefix = "cloud.google.com/go/language@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/language/com_google_cloud_go_language-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/language/com_google_cloud_go_language-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/language/com_google_cloud_go_language-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/language/com_google_cloud_go_language-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_lifesciences",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/lifesciences",
        sha256 = "8638174541f6d1b8d03cce39e94d5ba7b85def5550151e69c4d54e61d60101e3",
        strip_prefix = "cloud.google.com/go/lifesciences@v0.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/lifesciences/com_google_cloud_go_lifesciences-v0.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/lifesciences/com_google_cloud_go_lifesciences-v0.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/lifesciences/com_google_cloud_go_lifesciences-v0.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/lifesciences/com_google_cloud_go_lifesciences-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/logging",
        sha256 = "1b56716e7440c5064ed17af2c40bbba0c2e0f1d628f9f4864e81b7bd2958a2f3",
        strip_prefix = "cloud.google.com/go/logging@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/logging/com_google_cloud_go_logging-v1.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/logging/com_google_cloud_go_logging-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/logging/com_google_cloud_go_logging-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/logging/com_google_cloud_go_logging-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_longrunning",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/longrunning",
        sha256 = "6cb4e4a6b80435cb12ab0192ca281893e750f20903cdf5f2432a6d61db190361",
        strip_prefix = "cloud.google.com/go/longrunning@v0.4.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/longrunning/com_google_cloud_go_longrunning-v0.4.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/longrunning/com_google_cloud_go_longrunning-v0.4.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/longrunning/com_google_cloud_go_longrunning-v0.4.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/longrunning/com_google_cloud_go_longrunning-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_managedidentities",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/managedidentities",
        sha256 = "6ca18f1a180e7ce3159b8c6fdf93ba66122775a112874d9ce9a7f9fca3150a95",
        strip_prefix = "cloud.google.com/go/managedidentities@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/managedidentities/com_google_cloud_go_managedidentities-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/managedidentities/com_google_cloud_go_managedidentities-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/managedidentities/com_google_cloud_go_managedidentities-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/managedidentities/com_google_cloud_go_managedidentities-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_maps",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/maps",
        sha256 = "9988ceccfc296bc154f5cbd0ae455131ddec336e93293b07d1c5f4948653dd93",
        strip_prefix = "cloud.google.com/go/maps@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/maps/com_google_cloud_go_maps-v0.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/maps/com_google_cloud_go_maps-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/maps/com_google_cloud_go_maps-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/maps/com_google_cloud_go_maps-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_mediatranslation",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/mediatranslation",
        sha256 = "e78d770431918e6653b61029adf076402e15875acaa165c0db216567abeb5e63",
        strip_prefix = "cloud.google.com/go/mediatranslation@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/mediatranslation/com_google_cloud_go_mediatranslation-v0.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/mediatranslation/com_google_cloud_go_mediatranslation-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/mediatranslation/com_google_cloud_go_mediatranslation-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/mediatranslation/com_google_cloud_go_mediatranslation-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_memcache",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/memcache",
        sha256 = "e01bca761af97779d7a4b0d632fd0463d324b80fac75662c594dd008270ed389",
        strip_prefix = "cloud.google.com/go/memcache@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/memcache/com_google_cloud_go_memcache-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/memcache/com_google_cloud_go_memcache-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/memcache/com_google_cloud_go_memcache-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/memcache/com_google_cloud_go_memcache-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_metastore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/metastore",
        sha256 = "6ec835f8d18b39056072b7814a51cd6c22179cbf97f2b0204dc73d94082f00a4",
        strip_prefix = "cloud.google.com/go/metastore@v1.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/metastore/com_google_cloud_go_metastore-v1.10.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/metastore/com_google_cloud_go_metastore-v1.10.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/metastore/com_google_cloud_go_metastore-v1.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/metastore/com_google_cloud_go_metastore-v1.10.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_monitoring",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/monitoring",
        sha256 = "3ed009f1b492887939537dc59bea91ad78129eab5cba1fb4f090690a0f2a1f22",
        strip_prefix = "cloud.google.com/go/monitoring@v1.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/monitoring/com_google_cloud_go_monitoring-v1.13.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/monitoring/com_google_cloud_go_monitoring-v1.13.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/monitoring/com_google_cloud_go_monitoring-v1.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/monitoring/com_google_cloud_go_monitoring-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_networkconnectivity",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networkconnectivity",
        sha256 = "c2cd6ef6c8a4141ea70a20669000695559d3f3d41498de98c61878597cca05ea",
        strip_prefix = "cloud.google.com/go/networkconnectivity@v1.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/networkconnectivity/com_google_cloud_go_networkconnectivity-v1.11.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/networkconnectivity/com_google_cloud_go_networkconnectivity-v1.11.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/networkconnectivity/com_google_cloud_go_networkconnectivity-v1.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/networkconnectivity/com_google_cloud_go_networkconnectivity-v1.11.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_networkmanagement",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networkmanagement",
        sha256 = "4c74b55c69b73655d14d2198be6d6e8d4da240e7284c5c99eb2a7591bb95c187",
        strip_prefix = "cloud.google.com/go/networkmanagement@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/networkmanagement/com_google_cloud_go_networkmanagement-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/networkmanagement/com_google_cloud_go_networkmanagement-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/networkmanagement/com_google_cloud_go_networkmanagement-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/networkmanagement/com_google_cloud_go_networkmanagement-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_networksecurity",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networksecurity",
        sha256 = "1a358f55bb3daaba03ad22fe0ecbf67f334e829f3c7412de37f85b607572cb67",
        strip_prefix = "cloud.google.com/go/networksecurity@v0.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/networksecurity/com_google_cloud_go_networksecurity-v0.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/networksecurity/com_google_cloud_go_networksecurity-v0.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/networksecurity/com_google_cloud_go_networksecurity-v0.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/networksecurity/com_google_cloud_go_networksecurity-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_notebooks",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/notebooks",
        sha256 = "24ca6efce18d2cb1001280ad2c3dc2a002279b258ecf5d20bf912b666b19d279",
        strip_prefix = "cloud.google.com/go/notebooks@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/notebooks/com_google_cloud_go_notebooks-v1.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/notebooks/com_google_cloud_go_notebooks-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/notebooks/com_google_cloud_go_notebooks-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/notebooks/com_google_cloud_go_notebooks-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_optimization",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/optimization",
        sha256 = "a86473b6c76f5669e4c98ad4837a2ec77faab9bfabeb52c0f26b10019e039986",
        strip_prefix = "cloud.google.com/go/optimization@v1.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/optimization/com_google_cloud_go_optimization-v1.3.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/optimization/com_google_cloud_go_optimization-v1.3.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/optimization/com_google_cloud_go_optimization-v1.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/optimization/com_google_cloud_go_optimization-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_orchestration",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/orchestration",
        sha256 = "9568ea88c1626f6d69ac48abcbd4dfab26aebe3be89a19f179bf3277bcda26e9",
        strip_prefix = "cloud.google.com/go/orchestration@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/orchestration/com_google_cloud_go_orchestration-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/orchestration/com_google_cloud_go_orchestration-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/orchestration/com_google_cloud_go_orchestration-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/orchestration/com_google_cloud_go_orchestration-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_orgpolicy",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/orgpolicy",
        sha256 = "6fa13831a918ac690ed1073967e210349a13c2cd9bf51f84ba5cd6522a052d32",
        strip_prefix = "cloud.google.com/go/orgpolicy@v1.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/orgpolicy/com_google_cloud_go_orgpolicy-v1.10.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/orgpolicy/com_google_cloud_go_orgpolicy-v1.10.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/orgpolicy/com_google_cloud_go_orgpolicy-v1.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/orgpolicy/com_google_cloud_go_orgpolicy-v1.10.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_osconfig",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/osconfig",
        sha256 = "8f97d324f398aebb4af096041f8547a5b6b09cba754ba082fe3eca7f29a8b885",
        strip_prefix = "cloud.google.com/go/osconfig@v1.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/osconfig/com_google_cloud_go_osconfig-v1.11.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/osconfig/com_google_cloud_go_osconfig-v1.11.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/osconfig/com_google_cloud_go_osconfig-v1.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/osconfig/com_google_cloud_go_osconfig-v1.11.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_oslogin",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/oslogin",
        sha256 = "4e1f1ec2a64a8bb7f878185b3e618bb077df6fa94ed6704ab012e18c4ecd4fce",
        strip_prefix = "cloud.google.com/go/oslogin@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/oslogin/com_google_cloud_go_oslogin-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/oslogin/com_google_cloud_go_oslogin-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/oslogin/com_google_cloud_go_oslogin-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/oslogin/com_google_cloud_go_oslogin-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_phishingprotection",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/phishingprotection",
        sha256 = "7a3ce8e6b2c8f828fcd344b653849cf1e90abeca48a7eef81c75a72cb924d9e2",
        strip_prefix = "cloud.google.com/go/phishingprotection@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/phishingprotection/com_google_cloud_go_phishingprotection-v0.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/phishingprotection/com_google_cloud_go_phishingprotection-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/phishingprotection/com_google_cloud_go_phishingprotection-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/phishingprotection/com_google_cloud_go_phishingprotection-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_policytroubleshooter",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/policytroubleshooter",
        sha256 = "9d5fccfe01a31ec395ba3a26474168e5a8db09275dfbdfcd5dfd44923d9ac4bd",
        strip_prefix = "cloud.google.com/go/policytroubleshooter@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/policytroubleshooter/com_google_cloud_go_policytroubleshooter-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/policytroubleshooter/com_google_cloud_go_policytroubleshooter-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/policytroubleshooter/com_google_cloud_go_policytroubleshooter-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/policytroubleshooter/com_google_cloud_go_policytroubleshooter-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_privatecatalog",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/privatecatalog",
        sha256 = "f475f487df7906e4e35bda4b69ce53f141ade7ea6463674eb9b57f5fa302c367",
        strip_prefix = "cloud.google.com/go/privatecatalog@v0.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/privatecatalog/com_google_cloud_go_privatecatalog-v0.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/privatecatalog/com_google_cloud_go_privatecatalog-v0.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/privatecatalog/com_google_cloud_go_privatecatalog-v0.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/privatecatalog/com_google_cloud_go_privatecatalog-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/pubsub",
        sha256 = "9c15c75b6204fd3d42114006896a72d82827d01a756d2f78423c101102da4977",
        strip_prefix = "cloud.google.com/go/pubsub@v1.30.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/pubsub/com_google_cloud_go_pubsub-v1.30.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/pubsub/com_google_cloud_go_pubsub-v1.30.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/pubsub/com_google_cloud_go_pubsub-v1.30.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/pubsub/com_google_cloud_go_pubsub-v1.30.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_pubsublite",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/pubsublite",
        sha256 = "97b1c3637961faf18229a168a5811425b4e64ee6d81bb76e51ebbf93ff3622ba",
        strip_prefix = "cloud.google.com/go/pubsublite@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/pubsublite/com_google_cloud_go_pubsublite-v1.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/pubsublite/com_google_cloud_go_pubsublite-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/pubsublite/com_google_cloud_go_pubsublite-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/pubsublite/com_google_cloud_go_pubsublite-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_recaptchaenterprise_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recaptchaenterprise/v2",
        sha256 = "dbf218232a443651daa58869fb5e87845927c33d683f4fd4f6f4306e056bb7d0",
        strip_prefix = "cloud.google.com/go/recaptchaenterprise/v2@v2.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/recaptchaenterprise/v2/com_google_cloud_go_recaptchaenterprise_v2-v2.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/recaptchaenterprise/v2/com_google_cloud_go_recaptchaenterprise_v2-v2.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/recaptchaenterprise/v2/com_google_cloud_go_recaptchaenterprise_v2-v2.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/recaptchaenterprise/v2/com_google_cloud_go_recaptchaenterprise_v2-v2.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_recommendationengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recommendationengine",
        sha256 = "33cf95d20d5c036b5595c0f66005d82eb3ddb3ccebdcc69c120a1567b0f12f40",
        strip_prefix = "cloud.google.com/go/recommendationengine@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/recommendationengine/com_google_cloud_go_recommendationengine-v0.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/recommendationengine/com_google_cloud_go_recommendationengine-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/recommendationengine/com_google_cloud_go_recommendationengine-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/recommendationengine/com_google_cloud_go_recommendationengine-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_recommender",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recommender",
        sha256 = "8e9ccaf1167b4a7d3fd682581537f525f712af72c99b586aaea05832b82c86e8",
        strip_prefix = "cloud.google.com/go/recommender@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/recommender/com_google_cloud_go_recommender-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/recommender/com_google_cloud_go_recommender-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/recommender/com_google_cloud_go_recommender-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/recommender/com_google_cloud_go_recommender-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_redis",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/redis",
        sha256 = "51e5063e393d443f9d265b2aad809f45cee8af95a41ab8b532af38711ff451dc",
        strip_prefix = "cloud.google.com/go/redis@v1.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/redis/com_google_cloud_go_redis-v1.11.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/redis/com_google_cloud_go_redis-v1.11.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/redis/com_google_cloud_go_redis-v1.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/redis/com_google_cloud_go_redis-v1.11.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_resourcemanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/resourcemanager",
        sha256 = "92bba6de5d69d3928378722537f0b76ec8f958cece23acb9336512f3407eb8e4",
        strip_prefix = "cloud.google.com/go/resourcemanager@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/resourcemanager/com_google_cloud_go_resourcemanager-v1.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/resourcemanager/com_google_cloud_go_resourcemanager-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/resourcemanager/com_google_cloud_go_resourcemanager-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/resourcemanager/com_google_cloud_go_resourcemanager-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_resourcesettings",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/resourcesettings",
        sha256 = "9ff4470670ebcfa07f7964f85e312e41901afed236c14ecd10952d90e81f99f7",
        strip_prefix = "cloud.google.com/go/resourcesettings@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/resourcesettings/com_google_cloud_go_resourcesettings-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/resourcesettings/com_google_cloud_go_resourcesettings-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/resourcesettings/com_google_cloud_go_resourcesettings-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/resourcesettings/com_google_cloud_go_resourcesettings-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_retail",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/retail",
        sha256 = "5e71739001223ca2cdf7a6fa0ff61673a407ec18503fdd772b96e91ce42b67fc",
        strip_prefix = "cloud.google.com/go/retail@v1.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/retail/com_google_cloud_go_retail-v1.12.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/retail/com_google_cloud_go_retail-v1.12.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/retail/com_google_cloud_go_retail-v1.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/retail/com_google_cloud_go_retail-v1.12.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_run",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/run",
        sha256 = "7828480d028ff1b8496855bbd9dc264e772fae5f7866ceb5e1a7db6f18052edd",
        strip_prefix = "cloud.google.com/go/run@v0.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/run/com_google_cloud_go_run-v0.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/run/com_google_cloud_go_run-v0.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/run/com_google_cloud_go_run-v0.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/run/com_google_cloud_go_run-v0.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_scheduler",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/scheduler",
        sha256 = "3e225392a86a45fa9b5144f18bd3ea418f0cd7fab270ab4524a2e897bae54416",
        strip_prefix = "cloud.google.com/go/scheduler@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/scheduler/com_google_cloud_go_scheduler-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/scheduler/com_google_cloud_go_scheduler-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/scheduler/com_google_cloud_go_scheduler-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/scheduler/com_google_cloud_go_scheduler-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/secretmanager",
        sha256 = "d24cb4f507e9d531f7d75a4b070bff5f9dc548a2be1591337f4865cd8b084929",
        strip_prefix = "cloud.google.com/go/secretmanager@v1.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/secretmanager/com_google_cloud_go_secretmanager-v1.10.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/secretmanager/com_google_cloud_go_secretmanager-v1.10.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/secretmanager/com_google_cloud_go_secretmanager-v1.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/secretmanager/com_google_cloud_go_secretmanager-v1.10.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_security",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/security",
        sha256 = "e74202ce5419ed745d1c8089a2e4ffb790c0bc045d4f4ab788129ea0f0f5576d",
        strip_prefix = "cloud.google.com/go/security@v1.13.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/security/com_google_cloud_go_security-v1.13.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/security/com_google_cloud_go_security-v1.13.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/security/com_google_cloud_go_security-v1.13.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/security/com_google_cloud_go_security-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_securitycenter",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/securitycenter",
        sha256 = "0f451a28499260a21edf268bb8b657fc55fb81a883ab47fb3d2ca472f8707afd",
        strip_prefix = "cloud.google.com/go/securitycenter@v1.19.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/securitycenter/com_google_cloud_go_securitycenter-v1.19.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/securitycenter/com_google_cloud_go_securitycenter-v1.19.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/securitycenter/com_google_cloud_go_securitycenter-v1.19.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/securitycenter/com_google_cloud_go_securitycenter-v1.19.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_servicecontrol",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/servicecontrol",
        sha256 = "499ce8763d315e0ffdf3705549a507051a27eff9b8dec9debe43bca8d130fabb",
        strip_prefix = "cloud.google.com/go/servicecontrol@v1.11.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/servicecontrol/com_google_cloud_go_servicecontrol-v1.11.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/servicecontrol/com_google_cloud_go_servicecontrol-v1.11.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/servicecontrol/com_google_cloud_go_servicecontrol-v1.11.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/servicecontrol/com_google_cloud_go_servicecontrol-v1.11.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_servicedirectory",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/servicedirectory",
        sha256 = "4705df69c7e353bfa6a03dad8a50dde5066151b82528946b818df40547c79088",
        strip_prefix = "cloud.google.com/go/servicedirectory@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/servicedirectory/com_google_cloud_go_servicedirectory-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/servicedirectory/com_google_cloud_go_servicedirectory-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/servicedirectory/com_google_cloud_go_servicedirectory-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/servicedirectory/com_google_cloud_go_servicedirectory-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_servicemanagement",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/servicemanagement",
        sha256 = "2e02a723d1c226c2ecba4e47892b96052efb941be2910fd7afc38197f5bc6083",
        strip_prefix = "cloud.google.com/go/servicemanagement@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/servicemanagement/com_google_cloud_go_servicemanagement-v1.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/servicemanagement/com_google_cloud_go_servicemanagement-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/servicemanagement/com_google_cloud_go_servicemanagement-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/servicemanagement/com_google_cloud_go_servicemanagement-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_serviceusage",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/serviceusage",
        sha256 = "377bad0176bbec558ddb55b1fe10318e2c034c9e87536aba1ba8216b57548f3f",
        strip_prefix = "cloud.google.com/go/serviceusage@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/serviceusage/com_google_cloud_go_serviceusage-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/serviceusage/com_google_cloud_go_serviceusage-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/serviceusage/com_google_cloud_go_serviceusage-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/serviceusage/com_google_cloud_go_serviceusage-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_shell",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/shell",
        sha256 = "f88e9c2ff25a5ea22d71a1125cc6e756845ec8221c821092d05e67859966ca48",
        strip_prefix = "cloud.google.com/go/shell@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/shell/com_google_cloud_go_shell-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/shell/com_google_cloud_go_shell-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/shell/com_google_cloud_go_shell-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/shell/com_google_cloud_go_shell-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_spanner",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/spanner",
        sha256 = "e4f3951ea69d07ed383f41579c3a6af8e639558ecfa796421dc6cf3d268118ec",
        strip_prefix = "cloud.google.com/go/spanner@v1.45.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/spanner/com_google_cloud_go_spanner-v1.45.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/spanner/com_google_cloud_go_spanner-v1.45.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/spanner/com_google_cloud_go_spanner-v1.45.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/spanner/com_google_cloud_go_spanner-v1.45.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_speech",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/speech",
        sha256 = "27c7d30f3573b4d14a6096588fef65635bf7df8b98e921e934a0af1c7fcf7771",
        strip_prefix = "cloud.google.com/go/speech@v1.15.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/speech/com_google_cloud_go_speech-v1.15.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/speech/com_google_cloud_go_speech-v1.15.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/speech/com_google_cloud_go_speech-v1.15.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/speech/com_google_cloud_go_speech-v1.15.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/storage",
        sha256 = "d3702216afe89ee6a768eb302695d8ec64395c30a13fd0dc855acc9e30d4aad8",
        strip_prefix = "cloud.google.com/go/storage@v1.30.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/storage/com_google_cloud_go_storage-v1.30.1.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/storage/com_google_cloud_go_storage-v1.30.1.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/storage/com_google_cloud_go_storage-v1.30.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/storage/com_google_cloud_go_storage-v1.30.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_storagetransfer",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/storagetransfer",
        sha256 = "16e315b990875ac30d149de8b20f75338b178a9a4d34f03a7e181ed5fba7dd33",
        strip_prefix = "cloud.google.com/go/storagetransfer@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/storagetransfer/com_google_cloud_go_storagetransfer-v1.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/storagetransfer/com_google_cloud_go_storagetransfer-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/storagetransfer/com_google_cloud_go_storagetransfer-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/storagetransfer/com_google_cloud_go_storagetransfer-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_talent",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/talent",
        sha256 = "e6de9c5d91eb9c336fe36bc6c40c724f75773afe38f8719ec31add3a144328e6",
        strip_prefix = "cloud.google.com/go/talent@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/talent/com_google_cloud_go_talent-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/talent/com_google_cloud_go_talent-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/talent/com_google_cloud_go_talent-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/talent/com_google_cloud_go_talent-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_texttospeech",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/texttospeech",
        sha256 = "47fd557bca4ad5f4e8dff734c323a24a03253d19d2fcb693c9f3bd6ad3c15cd3",
        strip_prefix = "cloud.google.com/go/texttospeech@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/texttospeech/com_google_cloud_go_texttospeech-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/texttospeech/com_google_cloud_go_texttospeech-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/texttospeech/com_google_cloud_go_texttospeech-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/texttospeech/com_google_cloud_go_texttospeech-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_tpu",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/tpu",
        sha256 = "631fdef221fa6e2374bc43fabd37de734b402e6cc04449d095a6ddc8a1f64303",
        strip_prefix = "cloud.google.com/go/tpu@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/tpu/com_google_cloud_go_tpu-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/tpu/com_google_cloud_go_tpu-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/tpu/com_google_cloud_go_tpu-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/tpu/com_google_cloud_go_tpu-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_trace",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/trace",
        sha256 = "8012eaad65d2aa6dca225c708e6b0b43eb91bfc1c7dc82573fe7d993eb2c4384",
        strip_prefix = "cloud.google.com/go/trace@v1.9.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/trace/com_google_cloud_go_trace-v1.9.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/trace/com_google_cloud_go_trace-v1.9.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/trace/com_google_cloud_go_trace-v1.9.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/trace/com_google_cloud_go_trace-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_translate",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/translate",
        sha256 = "2bbf1bd793abf22ec8b0b200e8b49ea08821b1923ed24ffa668999f7330046fa",
        strip_prefix = "cloud.google.com/go/translate@v1.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/translate/com_google_cloud_go_translate-v1.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/translate/com_google_cloud_go_translate-v1.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/translate/com_google_cloud_go_translate-v1.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/translate/com_google_cloud_go_translate-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_video",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/video",
        sha256 = "fac96bb5bb2dafb9d19c6b3e70455999c65f2be1f4a0ee86c7772796fcbf660c",
        strip_prefix = "cloud.google.com/go/video@v1.15.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/video/com_google_cloud_go_video-v1.15.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/video/com_google_cloud_go_video-v1.15.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/video/com_google_cloud_go_video-v1.15.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/video/com_google_cloud_go_video-v1.15.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_videointelligence",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/videointelligence",
        sha256 = "d7a24a20e8f4c0b7dc088010263be03132f63f62dbfa9eb69447c229ef80626b",
        strip_prefix = "cloud.google.com/go/videointelligence@v1.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/videointelligence/com_google_cloud_go_videointelligence-v1.10.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/videointelligence/com_google_cloud_go_videointelligence-v1.10.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/videointelligence/com_google_cloud_go_videointelligence-v1.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/videointelligence/com_google_cloud_go_videointelligence-v1.10.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_vision_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vision/v2",
        sha256 = "323f1c5e07ea11ee90bec85c0fdccbcf73c26ce28baa832528cf4a9c50d0b4f7",
        strip_prefix = "cloud.google.com/go/vision/v2@v2.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/vision/v2/com_google_cloud_go_vision_v2-v2.7.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/vision/v2/com_google_cloud_go_vision_v2-v2.7.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/vision/v2/com_google_cloud_go_vision_v2-v2.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/vision/v2/com_google_cloud_go_vision_v2-v2.7.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_vmmigration",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vmmigration",
        sha256 = "a289f09b2e6249b493e3ae8bb10225d77590f3823302e46a99ea51b732debb65",
        strip_prefix = "cloud.google.com/go/vmmigration@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/vmmigration/com_google_cloud_go_vmmigration-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/vmmigration/com_google_cloud_go_vmmigration-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/vmmigration/com_google_cloud_go_vmmigration-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/vmmigration/com_google_cloud_go_vmmigration-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_vmwareengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vmwareengine",
        sha256 = "f6f5753bf4ee0c4264f78a78966f019fd200bb5bae79fad321093a439b08a2b6",
        strip_prefix = "cloud.google.com/go/vmwareengine@v0.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/vmwareengine/com_google_cloud_go_vmwareengine-v0.3.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/vmwareengine/com_google_cloud_go_vmwareengine-v0.3.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/vmwareengine/com_google_cloud_go_vmwareengine-v0.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/vmwareengine/com_google_cloud_go_vmwareengine-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_vpcaccess",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vpcaccess",
        sha256 = "8d0662362ec347afedf274930c139afd0c9cdb219646ceb58a07668c5c84278b",
        strip_prefix = "cloud.google.com/go/vpcaccess@v1.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/vpcaccess/com_google_cloud_go_vpcaccess-v1.6.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/vpcaccess/com_google_cloud_go_vpcaccess-v1.6.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/vpcaccess/com_google_cloud_go_vpcaccess-v1.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/vpcaccess/com_google_cloud_go_vpcaccess-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_webrisk",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/webrisk",
        sha256 = "8cc27cca95d2dd5efc58f335b085da8b46d6520a1963f6b2a33676f2837f3553",
        strip_prefix = "cloud.google.com/go/webrisk@v1.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/webrisk/com_google_cloud_go_webrisk-v1.8.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/webrisk/com_google_cloud_go_webrisk-v1.8.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/webrisk/com_google_cloud_go_webrisk-v1.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/webrisk/com_google_cloud_go_webrisk-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_websecurityscanner",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/websecurityscanner",
        sha256 = "7f0774556cb41ac4acd16a386a9f8664c7f0ac11ed126d5d771fe07a217ef131",
        strip_prefix = "cloud.google.com/go/websecurityscanner@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/websecurityscanner/com_google_cloud_go_websecurityscanner-v1.5.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/websecurityscanner/com_google_cloud_go_websecurityscanner-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/websecurityscanner/com_google_cloud_go_websecurityscanner-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/websecurityscanner/com_google_cloud_go_websecurityscanner-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_workflows",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/workflows",
        sha256 = "e6e83869c5fbcccd3ee489128a300b75cb02a99b48b59bbb829b2e7d7ab81f9c",
        strip_prefix = "cloud.google.com/go/workflows@v1.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/cloud.google.com/go/workflows/com_google_cloud_go_workflows-v1.10.0.zip",
            "http://ats.apps.svc/gomod/cloud.google.com/go/workflows/com_google_cloud_go_workflows-v1.10.0.zip",
            "https://cache.hawkingrei.com/gomod/cloud.google.com/go/workflows/com_google_cloud_go_workflows-v1.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/cloud.google.com/go/workflows/com_google_cloud_go_workflows-v1.10.0.zip",
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
        sha256 = "bd2492d9db05362c2fecd0b3d0f6002c89a6d90d678fb93b4158298ab883736f",
        strip_prefix = "sourcegraph.com/sourcegraph/appdash@v0.0.0-20190731080439-ebfcffb1b5c0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/sourcegraph.com/sourcegraph/appdash/com_sourcegraph_sourcegraph_appdash-v0.0.0-20190731080439-ebfcffb1b5c0.zip",
            "http://ats.apps.svc/gomod/sourcegraph.com/sourcegraph/appdash/com_sourcegraph_sourcegraph_appdash-v0.0.0-20190731080439-ebfcffb1b5c0.zip",
            "https://cache.hawkingrei.com/gomod/sourcegraph.com/sourcegraph/appdash/com_sourcegraph_sourcegraph_appdash-v0.0.0-20190731080439-ebfcffb1b5c0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/sourcegraph.com/sourcegraph/appdash/com_sourcegraph_sourcegraph_appdash-v0.0.0-20190731080439-ebfcffb1b5c0.zip",
        ],
    )
    go_repository(
        name = "com_sourcegraph_sourcegraph_appdash_data",
        build_file_proto_mode = "disable_global",
        importpath = "sourcegraph.com/sourcegraph/appdash-data",
        sha256 = "382adefecd62bb79172e2552bcfb7d45f47122f9bd22259b0566b26fb2627b87",
        strip_prefix = "sourcegraph.com/sourcegraph/appdash-data@v0.0.0-20151005221446-73f23eafcf67",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/sourcegraph.com/sourcegraph/appdash-data/com_sourcegraph_sourcegraph_appdash_data-v0.0.0-20151005221446-73f23eafcf67.zip",
            "http://ats.apps.svc/gomod/sourcegraph.com/sourcegraph/appdash-data/com_sourcegraph_sourcegraph_appdash_data-v0.0.0-20151005221446-73f23eafcf67.zip",
            "https://cache.hawkingrei.com/gomod/sourcegraph.com/sourcegraph/appdash-data/com_sourcegraph_sourcegraph_appdash_data-v0.0.0-20151005221446-73f23eafcf67.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/sourcegraph.com/sourcegraph/appdash-data/com_sourcegraph_sourcegraph_appdash_data-v0.0.0-20151005221446-73f23eafcf67.zip",
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
        name = "dev_tmz_go_musttag",
        build_file_proto_mode = "disable_global",
        importpath = "go.tmz.dev/musttag",
        sha256 = "48d2a63695ac1b8e3daf16d7764e8392e53b22e7f261cac888dfad64a7b0b961",
        strip_prefix = "go.tmz.dev/musttag@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.tmz.dev/musttag/dev_tmz_go_musttag-v0.7.0.zip",
            "http://ats.apps.svc/gomod/go.tmz.dev/musttag/dev_tmz_go_musttag-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/go.tmz.dev/musttag/dev_tmz_go_musttag-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.tmz.dev/musttag/dev_tmz_go_musttag-v0.7.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_alecthomas_kingpin_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/alecthomas/kingpin.v2",
        sha256 = "638080591aefe7d2642f2575b627d534c692606f02ea54ba89f42db112ba8839",
        strip_prefix = "gopkg.in/alecthomas/kingpin.v2@v2.2.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/alecthomas/kingpin.v2/in_gopkg_alecthomas_kingpin_v2-v2.2.6.zip",
            "http://ats.apps.svc/gomod/gopkg.in/alecthomas/kingpin.v2/in_gopkg_alecthomas_kingpin_v2-v2.2.6.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/alecthomas/kingpin.v2/in_gopkg_alecthomas_kingpin_v2-v2.2.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/alecthomas/kingpin.v2/in_gopkg_alecthomas_kingpin_v2-v2.2.6.zip",
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
        name = "in_gopkg_fsnotify_fsnotify_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/fsnotify/fsnotify.v1",
        sha256 = "d24b0fa77291be6c99ad3f75dfde626112e018ce8d28cc2e0d68b6c8f2c29521",
        strip_prefix = "gopkg.in/fsnotify/fsnotify.v1@v1.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/fsnotify/fsnotify.v1/in_gopkg_fsnotify_fsnotify_v1-v1.3.1.zip",
            "http://ats.apps.svc/gomod/gopkg.in/fsnotify/fsnotify.v1/in_gopkg_fsnotify_fsnotify_v1-v1.3.1.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/fsnotify/fsnotify.v1/in_gopkg_fsnotify_fsnotify_v1-v1.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/fsnotify/fsnotify.v1/in_gopkg_fsnotify_fsnotify_v1-v1.3.1.zip",
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
        name = "in_gopkg_resty_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/resty.v1",
        sha256 = "43487bb0bb40626d16502b1fe9e719cf751e7a5b4e4233276971873e7863d3cf",
        strip_prefix = "gopkg.in/resty.v1@v1.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gopkg.in/resty.v1/in_gopkg_resty_v1-v1.12.0.zip",
            "http://ats.apps.svc/gomod/gopkg.in/resty.v1/in_gopkg_resty_v1-v1.12.0.zip",
            "https://cache.hawkingrei.com/gomod/gopkg.in/resty.v1/in_gopkg_resty_v1-v1.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gopkg.in/resty.v1/in_gopkg_resty_v1-v1.12.0.zip",
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
        sha256 = "a357fccd93e865dce3d3859ed857ce827f7a2f2dc5b90cfaa95202f5d76e4ac2",
        strip_prefix = "go.etcd.io/bbolt@v1.3.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/bbolt/io_etcd_go_bbolt-v1.3.6.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/bbolt/io_etcd_go_bbolt-v1.3.6.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/bbolt/io_etcd_go_bbolt-v1.3.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/bbolt/io_etcd_go_bbolt-v1.3.6.zip",
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
        sha256 = "bfd9ce626389c8a11c2d33eb3c823cc277898c51254a6e02ed967f948aec79f6",
        strip_prefix = "go.etcd.io/etcd/api/v3@v3.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/api/v3/io_etcd_go_etcd_api_v3-v3.5.2.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/api/v3/io_etcd_go_etcd_api_v3-v3.5.2.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/api/v3/io_etcd_go_etcd_api_v3-v3.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/api/v3/io_etcd_go_etcd_api_v3-v3.5.2.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_client_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/pkg/v3",
        sha256 = "b183c377b46eb622d80d77b14755acbdbba43b9b5882ed2a5e9975985eaacd25",
        strip_prefix = "go.etcd.io/etcd/client/pkg/v3@v3.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/client/pkg/v3/io_etcd_go_etcd_client_pkg_v3-v3.5.2.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/client/pkg/v3/io_etcd_go_etcd_client_pkg_v3-v3.5.2.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/client/pkg/v3/io_etcd_go_etcd_client_pkg_v3-v3.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/client/pkg/v3/io_etcd_go_etcd_client_pkg_v3-v3.5.2.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v2",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/v2",
        sha256 = "25e0a2e179114cdc122e57dcee974cff927cbe2f04304d71575fe0dbf66d506b",
        strip_prefix = "go.etcd.io/etcd/client/v2@v2.305.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/client/v2/io_etcd_go_etcd_client_v2-v2.305.2.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/client/v2/io_etcd_go_etcd_client_v2-v2.305.2.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/client/v2/io_etcd_go_etcd_client_v2-v2.305.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/client/v2/io_etcd_go_etcd_client_v2-v2.305.2.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/v3",
        sha256 = "06aae6f25789a7dea98a2f7df67a4d65b660b81a8accd88ddced9ca8c335d99d",
        strip_prefix = "go.etcd.io/etcd/client/v3@v3.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/client/v3/io_etcd_go_etcd_client_v3-v3.5.2.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/client/v3/io_etcd_go_etcd_client_v3-v3.5.2.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/client/v3/io_etcd_go_etcd_client_v3-v3.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/client/v3/io_etcd_go_etcd_client_v3-v3.5.2.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_etcdutl_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/etcdutl/v3",
        sha256 = "9d694d9b204037b05d13c6897a3b81a8234cc444e9b9892846a79a3ade72aeab",
        strip_prefix = "go.etcd.io/etcd/etcdutl/v3@v3.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/etcdutl/v3/io_etcd_go_etcd_etcdutl_v3-v3.5.2.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/etcdutl/v3/io_etcd_go_etcd_etcdutl_v3-v3.5.2.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/etcdutl/v3/io_etcd_go_etcd_etcdutl_v3-v3.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/etcdutl/v3/io_etcd_go_etcd_etcdutl_v3-v3.5.2.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/pkg/v3",
        sha256 = "a1d96686d541509919732896d79e885e40147b5eeb8315db58dc07ad8c191226",
        strip_prefix = "go.etcd.io/etcd/pkg/v3@v3.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/pkg/v3/io_etcd_go_etcd_pkg_v3-v3.5.2.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/pkg/v3/io_etcd_go_etcd_pkg_v3-v3.5.2.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/pkg/v3/io_etcd_go_etcd_pkg_v3-v3.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/pkg/v3/io_etcd_go_etcd_pkg_v3-v3.5.2.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_raft_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/raft/v3",
        sha256 = "2b1fdd35d496af817cfe06ff74949e3cc77efac3473f817f998569107162d41a",
        strip_prefix = "go.etcd.io/etcd/raft/v3@v3.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/raft/v3/io_etcd_go_etcd_raft_v3-v3.5.2.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/raft/v3/io_etcd_go_etcd_raft_v3-v3.5.2.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/raft/v3/io_etcd_go_etcd_raft_v3-v3.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/raft/v3/io_etcd_go_etcd_raft_v3-v3.5.2.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_server_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/server/v3",
        sha256 = "7eac7dcb18c57f880830d363ab250f9b387c0cbed3e4910427b8e23b7d8e28d3",
        strip_prefix = "go.etcd.io/etcd/server/v3@v3.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/server/v3/io_etcd_go_etcd_server_v3-v3.5.2.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/server/v3/io_etcd_go_etcd_server_v3-v3.5.2.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/server/v3/io_etcd_go_etcd_server_v3-v3.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/server/v3/io_etcd_go_etcd_server_v3-v3.5.2.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_tests_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/tests/v3",
        sha256 = "fc00d13163948f7633e1f53f08d05ee4e75930d02114754384a736f733d35148",
        strip_prefix = "go.etcd.io/etcd/tests/v3@v3.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.etcd.io/etcd/tests/v3/io_etcd_go_etcd_tests_v3-v3.5.2.zip",
            "http://ats.apps.svc/gomod/go.etcd.io/etcd/tests/v3/io_etcd_go_etcd_tests_v3-v3.5.2.zip",
            "https://cache.hawkingrei.com/gomod/go.etcd.io/etcd/tests/v3/io_etcd_go_etcd_tests_v3-v3.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.etcd.io/etcd/tests/v3/io_etcd_go_etcd_tests_v3-v3.5.2.zip",
        ],
    )
    go_repository(
        name = "io_k8s_api",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/api",
        sha256 = "417e394e3510035a617292da245c07d606d9a4c0674361719f6a08dc0bf67b68",
        strip_prefix = "k8s.io/api@v0.27.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/api/io_k8s_api-v0.27.2.zip",
            "http://ats.apps.svc/gomod/k8s.io/api/io_k8s_api-v0.27.2.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/api/io_k8s_api-v0.27.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/api/io_k8s_api-v0.27.2.zip",
        ],
    )
    go_repository(
        name = "io_k8s_apimachinery",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/apimachinery",
        sha256 = "1f2f04041166fcddead7f31f03149f33ee6fdc08db1093ddbc027191204c2f86",
        strip_prefix = "k8s.io/apimachinery@v0.27.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/apimachinery/io_k8s_apimachinery-v0.27.2.zip",
            "http://ats.apps.svc/gomod/k8s.io/apimachinery/io_k8s_apimachinery-v0.27.2.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/apimachinery/io_k8s_apimachinery-v0.27.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/apimachinery/io_k8s_apimachinery-v0.27.2.zip",
        ],
    )
    go_repository(
        name = "io_k8s_client_go",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/client-go",
        sha256 = "27e135d4d9663f42f5e6b75830a9e795db9752d6da9cc5f595a1b75233efd817",
        strip_prefix = "k8s.io/client-go@v11.0.1-0.20190409021438-1a26190bd76a+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/client-go/io_k8s_client_go-v11.0.1-0.20190409021438-1a26190bd76a+incompatible.zip",
            "http://ats.apps.svc/gomod/k8s.io/client-go/io_k8s_client_go-v11.0.1-0.20190409021438-1a26190bd76a+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/client-go/io_k8s_client_go-v11.0.1-0.20190409021438-1a26190bd76a+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/client-go/io_k8s_client_go-v11.0.1-0.20190409021438-1a26190bd76a+incompatible.zip",
        ],
    )
    go_repository(
        name = "io_k8s_klog",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog",
        sha256 = "520558eccd4b172aa20cd0b3ee1f60d15a8d5894ace059304c19f83afd4df36a",
        strip_prefix = "k8s.io/klog@v0.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/klog/io_k8s_klog-v0.3.0.zip",
            "http://ats.apps.svc/gomod/k8s.io/klog/io_k8s_klog-v0.3.0.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/klog/io_k8s_klog-v0.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/klog/io_k8s_klog-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_klog_v2",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog/v2",
        sha256 = "73f9da873c79b331e5b9d70a56ca89bda33f42b04564bf8f848807f60e3232e5",
        strip_prefix = "k8s.io/klog/v2@v2.90.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/klog/v2/io_k8s_klog_v2-v2.90.1.zip",
            "http://ats.apps.svc/gomod/k8s.io/klog/v2/io_k8s_klog_v2-v2.90.1.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/klog/v2/io_k8s_klog_v2-v2.90.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/klog/v2/io_k8s_klog_v2-v2.90.1.zip",
        ],
    )
    go_repository(
        name = "io_k8s_kube_openapi",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/kube-openapi",
        sha256 = "2738e6254f17109e5cf8c0ac5b902bcfe014ad065a9520b8dadad5a7a7d166d5",
        strip_prefix = "k8s.io/kube-openapi@v0.0.0-20230501164219-8b0f38b5fd1f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/kube-openapi/io_k8s_kube_openapi-v0.0.0-20230501164219-8b0f38b5fd1f.zip",
            "http://ats.apps.svc/gomod/k8s.io/kube-openapi/io_k8s_kube_openapi-v0.0.0-20230501164219-8b0f38b5fd1f.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/kube-openapi/io_k8s_kube_openapi-v0.0.0-20230501164219-8b0f38b5fd1f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/kube-openapi/io_k8s_kube_openapi-v0.0.0-20230501164219-8b0f38b5fd1f.zip",
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
        sha256 = "81c147c247afbc71fa135b2f4209c0641e141267be5c6a956a0f8bf851e74e31",
        strip_prefix = "sigs.k8s.io/structured-merge-diff/v4@v4.2.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/sigs.k8s.io/structured-merge-diff/v4/io_k8s_sigs_structured_merge_diff_v4-v4.2.3.zip",
            "http://ats.apps.svc/gomod/sigs.k8s.io/structured-merge-diff/v4/io_k8s_sigs_structured_merge_diff_v4-v4.2.3.zip",
            "https://cache.hawkingrei.com/gomod/sigs.k8s.io/structured-merge-diff/v4/io_k8s_sigs_structured_merge_diff_v4-v4.2.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/sigs.k8s.io/structured-merge-diff/v4/io_k8s_sigs_structured_merge_diff_v4-v4.2.3.zip",
        ],
    )
    go_repository(
        name = "io_k8s_sigs_yaml",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/yaml",
        sha256 = "aac88da551c2a512b642cb35658bf3dc862595a3566bd840ebf18e4802f1fcc9",
        strip_prefix = "sigs.k8s.io/yaml@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/sigs.k8s.io/yaml/io_k8s_sigs_yaml-v1.3.0.zip",
            "http://ats.apps.svc/gomod/sigs.k8s.io/yaml/io_k8s_sigs_yaml-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/sigs.k8s.io/yaml/io_k8s_sigs_yaml-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/sigs.k8s.io/yaml/io_k8s_sigs_yaml-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_utils",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/utils",
        sha256 = "29b4ceb51420d6f5c6482e9cc0884b369dc44647b4b4cc09886acd5091817026",
        strip_prefix = "k8s.io/utils@v0.0.0-20230209194617-a36077c30491",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/k8s.io/utils/io_k8s_utils-v0.0.0-20230209194617-a36077c30491.zip",
            "http://ats.apps.svc/gomod/k8s.io/utils/io_k8s_utils-v0.0.0-20230209194617-a36077c30491.zip",
            "https://cache.hawkingrei.com/gomod/k8s.io/utils/io_k8s_utils-v0.0.0-20230209194617-a36077c30491.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/k8s.io/utils/io_k8s_utils-v0.0.0-20230209194617-a36077c30491.zip",
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
        name = "io_opencensus_go_contrib_exporter_ocagent",
        build_file_proto_mode = "disable_global",
        importpath = "contrib.go.opencensus.io/exporter/ocagent",
        sha256 = "a06ce658c281fe830953bf2548047de8c43776c87d0eac63db37206538928e51",
        strip_prefix = "contrib.go.opencensus.io/exporter/ocagent@v0.4.12",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/contrib.go.opencensus.io/exporter/ocagent/io_opencensus_go_contrib_exporter_ocagent-v0.4.12.zip",
            "http://ats.apps.svc/gomod/contrib.go.opencensus.io/exporter/ocagent/io_opencensus_go_contrib_exporter_ocagent-v0.4.12.zip",
            "https://cache.hawkingrei.com/gomod/contrib.go.opencensus.io/exporter/ocagent/io_opencensus_go_contrib_exporter_ocagent-v0.4.12.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/contrib.go.opencensus.io/exporter/ocagent/io_opencensus_go_contrib_exporter_ocagent-v0.4.12.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_contrib",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/contrib",
        sha256 = "b33252dafaa7884e1925ca052bfc32275bd69f7faa1a294ce2dbf05b7f62fda1",
        strip_prefix = "go.opentelemetry.io/contrib@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/contrib/io_opentelemetry_go_contrib-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/contrib/io_opentelemetry_go_contrib-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/contrib/io_opentelemetry_go_contrib-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/contrib/io_opentelemetry_go_contrib-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
        sha256 = "5d75e50405735d05540a3cc59c3741cc43275ba9203bcc77ac85214ebd5212f8",
        strip_prefix = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel",
        sha256 = "8e55c823cde41ae4920f331e3b3999adca4c8729f0f096950454c996520972a3",
        strip_prefix = "go.opentelemetry.io/otel@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/io_opentelemetry_go_otel-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/io_opentelemetry_go_otel-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/io_opentelemetry_go_otel-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/io_opentelemetry_go_otel-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp",
        sha256 = "abd40ffff96f3caa01ee6854b52e69e6787b10d31a6c2023447d5106496c9b2e",
        strip_prefix = "go.opentelemetry.io/otel/exporters/otlp@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/exporters/otlp/io_opentelemetry_go_otel_exporters_otlp-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/exporters/otlp/io_opentelemetry_go_otel_exporters_otlp-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/exporters/otlp/io_opentelemetry_go_otel_exporters_otlp-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/exporters/otlp/io_opentelemetry_go_otel_exporters_otlp-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_metric",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/metric",
        sha256 = "d7ae3abbdcf9ea48ff23a477f324cb3595c77f3eb83f6acde5c0c9300e23fedb",
        strip_prefix = "go.opentelemetry.io/otel/metric@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/metric/io_opentelemetry_go_otel_metric-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/metric/io_opentelemetry_go_otel_metric-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/metric/io_opentelemetry_go_otel_metric-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/metric/io_opentelemetry_go_otel_metric-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_oteltest",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/oteltest",
        sha256 = "5773e674e2f095c2348d13133d2c5ed3019c3c4dc43c47dcae788a673f197d20",
        strip_prefix = "go.opentelemetry.io/otel/oteltest@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/oteltest/io_opentelemetry_go_otel_oteltest-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/oteltest/io_opentelemetry_go_otel_oteltest-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/oteltest/io_opentelemetry_go_otel_oteltest-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/oteltest/io_opentelemetry_go_otel_oteltest-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/sdk",
        sha256 = "13c01e92ebcbde0b3d2efc4d3a4445c2cce8d505c823aeffff6398a7dabb3806",
        strip_prefix = "go.opentelemetry.io/otel/sdk@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/sdk/io_opentelemetry_go_otel_sdk-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/sdk/io_opentelemetry_go_otel_sdk-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/sdk/io_opentelemetry_go_otel_sdk-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/sdk/io_opentelemetry_go_otel_sdk-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk_export_metric",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/sdk/export/metric",
        sha256 = "e0037e543d27111d06904f8a2060b41fb40e960ddce5cec5e6f190490ae52f57",
        strip_prefix = "go.opentelemetry.io/otel/sdk/export/metric@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/sdk/export/metric/io_opentelemetry_go_otel_sdk_export_metric-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/sdk/export/metric/io_opentelemetry_go_otel_sdk_export_metric-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/sdk/export/metric/io_opentelemetry_go_otel_sdk_export_metric-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/sdk/export/metric/io_opentelemetry_go_otel_sdk_export_metric-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk_metric",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/sdk/metric",
        sha256 = "b0d5ffded967229eeee79bb9fb50320c68af812d5f2e6dcb9e44ddb7bd2afe16",
        strip_prefix = "go.opentelemetry.io/otel/sdk/metric@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/sdk/metric/io_opentelemetry_go_otel_sdk_metric-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/sdk/metric/io_opentelemetry_go_otel_sdk_metric-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/sdk/metric/io_opentelemetry_go_otel_sdk_metric-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/sdk/metric/io_opentelemetry_go_otel_sdk_metric-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/trace",
        sha256 = "fd6a9646a66f0fa98fc2b12eed1abe11220e5e6cc0cb4b8d9c5905631c87608d",
        strip_prefix = "go.opentelemetry.io/otel/trace@v0.20.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/trace/io_opentelemetry_go_otel_trace-v0.20.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/trace/io_opentelemetry_go_otel_trace-v0.20.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/trace/io_opentelemetry_go_otel_trace-v0.20.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/trace/io_opentelemetry_go_otel_trace-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_proto_otlp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/proto/otlp",
        sha256 = "a7db0590bc4c5f0b9b99cc958decf644f1e5cc11e0b995dc20b3583a2215259b",
        strip_prefix = "go.opentelemetry.io/proto/otlp@v0.7.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/proto/otlp/io_opentelemetry_go_proto_otlp-v0.7.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/proto/otlp/io_opentelemetry_go_proto_otlp-v0.7.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/proto/otlp/io_opentelemetry_go_proto_otlp-v0.7.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/proto/otlp/io_opentelemetry_go_proto_otlp-v0.7.0.zip",
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
        name = "org_golang_google_api",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/api",
        sha256 = "42c62aaba1d76efede08c70d8aef7889c5c8ee9c9c4f1e7c455b07838cabb785",
        strip_prefix = "google.golang.org/api@v0.114.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/api/org_golang_google_api-v0.114.0.zip",
            "http://ats.apps.svc/gomod/google.golang.org/api/org_golang_google_api-v0.114.0.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/api/org_golang_google_api-v0.114.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/api/org_golang_google_api-v0.114.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_appengine",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/appengine",
        sha256 = "79f80dfac18681788f1414e21a4a7734eff4cdf992070be9163103eb8d9f92cd",
        strip_prefix = "google.golang.org/appengine@v1.6.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/appengine/org_golang_google_appengine-v1.6.7.zip",
            "http://ats.apps.svc/gomod/google.golang.org/appengine/org_golang_google_appengine-v1.6.7.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/appengine/org_golang_google_appengine-v1.6.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/appengine/org_golang_google_appengine-v1.6.7.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_genproto",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto",
        sha256 = "28f0317e6948788a33c07698109005675062f0203ed06bc866350a575bc974bf",
        strip_prefix = "google.golang.org/genproto@v0.0.0-20230410155749-daa745c078e1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/genproto/org_golang_google_genproto-v0.0.0-20230410155749-daa745c078e1.zip",
            "http://ats.apps.svc/gomod/google.golang.org/genproto/org_golang_google_genproto-v0.0.0-20230410155749-daa745c078e1.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/genproto/org_golang_google_genproto-v0.0.0-20230410155749-daa745c078e1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/genproto/org_golang_google_genproto-v0.0.0-20230410155749-daa745c078e1.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/grpc",
        sha256 = "8e279a7a36347098a00debb5f76ef75b981939c282cd7771cc22b9b576065d84",
        strip_prefix = "google.golang.org/grpc@v1.54.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/grpc/org_golang_google_grpc-v1.54.0.zip",
            "http://ats.apps.svc/gomod/google.golang.org/grpc/org_golang_google_grpc-v1.54.0.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/grpc/org_golang_google_grpc-v1.54.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/grpc/org_golang_google_grpc-v1.54.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/protobuf",
        sha256 = "8fb83b4d6e898c75a8d305dd46d46bb975d0a7ebab1e0042acdf25e3273d50e4",
        strip_prefix = "google.golang.org/protobuf@v1.30.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/google.golang.org/protobuf/org_golang_google_protobuf-v1.30.0.zip",
            "http://ats.apps.svc/gomod/google.golang.org/protobuf/org_golang_google_protobuf-v1.30.0.zip",
            "https://cache.hawkingrei.com/gomod/google.golang.org/protobuf/org_golang_google_protobuf-v1.30.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/google.golang.org/protobuf/org_golang_google_protobuf-v1.30.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_crypto",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/crypto",
        sha256 = "29b788bd8f1229214af831bf99412a09d19096dea3c62bc3281656b64093d12d",
        strip_prefix = "golang.org/x/crypto@v0.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/crypto/org_golang_x_crypto-v0.12.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/crypto/org_golang_x_crypto-v0.12.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/crypto/org_golang_x_crypto-v0.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/crypto/org_golang_x_crypto-v0.12.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_exp",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/exp",
        sha256 = "d4b6a3cc6bf072a05030324328169b5f878f7be012508fff618e251cccb3d0aa",
        strip_prefix = "golang.org/x/exp@v0.0.0-20230711005742-c3f37128e5a4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/exp/org_golang_x_exp-v0.0.0-20230711005742-c3f37128e5a4.zip",
            "http://ats.apps.svc/gomod/golang.org/x/exp/org_golang_x_exp-v0.0.0-20230711005742-c3f37128e5a4.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/exp/org_golang_x_exp-v0.0.0-20230711005742-c3f37128e5a4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/exp/org_golang_x_exp-v0.0.0-20230711005742-c3f37128e5a4.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_exp_typeparams",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/exp/typeparams",
        sha256 = "1113b29eb80ff666c87898cb9ffca08571702f32e59bd05d08e7c2eafdcb4ece",
        strip_prefix = "golang.org/x/exp/typeparams@v0.0.0-20230224173230-c95f2b4c22f2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/exp/typeparams/org_golang_x_exp_typeparams-v0.0.0-20230224173230-c95f2b4c22f2.zip",
            "http://ats.apps.svc/gomod/golang.org/x/exp/typeparams/org_golang_x_exp_typeparams-v0.0.0-20230224173230-c95f2b4c22f2.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/exp/typeparams/org_golang_x_exp_typeparams-v0.0.0-20230224173230-c95f2b4c22f2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/exp/typeparams/org_golang_x_exp_typeparams-v0.0.0-20230224173230-c95f2b4c22f2.zip",
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
        sha256 = "0a4a5ebd2b1d79e7f480cbf5a54b45a257ae1ec9d11f01688efc5c35268d4603",
        strip_prefix = "golang.org/x/lint@v0.0.0-20210508222113-6edffad5e616",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/lint/org_golang_x_lint-v0.0.0-20210508222113-6edffad5e616.zip",
            "http://ats.apps.svc/gomod/golang.org/x/lint/org_golang_x_lint-v0.0.0-20210508222113-6edffad5e616.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/lint/org_golang_x_lint-v0.0.0-20210508222113-6edffad5e616.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/lint/org_golang_x_lint-v0.0.0-20210508222113-6edffad5e616.zip",
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
        sha256 = "364198930cc7f46ba5bb1c0987d089a557aa0b406f8efec0490744a454df00a5",
        strip_prefix = "golang.org/x/mod@v0.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/mod/org_golang_x_mod-v0.11.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/mod/org_golang_x_mod-v0.11.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/mod/org_golang_x_mod-v0.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/mod/org_golang_x_mod-v0.11.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_net",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/net",
        sha256 = "fdd5ca5653644b65d9062705ecae70b156660547f96d4606659960fe0c053871",
        strip_prefix = "golang.org/x/net@v0.14.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/net/org_golang_x_net-v0.14.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/net/org_golang_x_net-v0.14.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/net/org_golang_x_net-v0.14.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/net/org_golang_x_net-v0.14.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_oauth2",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/oauth2",
        sha256 = "774ad761b3732b86eaa3d70c30bcaed6dd09e96eec3cdeb2c0a9c112ce168704",
        strip_prefix = "golang.org/x/oauth2@v0.8.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/oauth2/org_golang_x_oauth2-v0.8.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/oauth2/org_golang_x_oauth2-v0.8.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/oauth2/org_golang_x_oauth2-v0.8.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/oauth2/org_golang_x_oauth2-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_sync",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sync",
        sha256 = "1870e7a196f7119d4c6edba7de9cdfc49ee13c8cb7921f3a947568171c6152e0",
        strip_prefix = "golang.org/x/sync@v0.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/sync/org_golang_x_sync-v0.3.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/sync/org_golang_x_sync-v0.3.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/sync/org_golang_x_sync-v0.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/sync/org_golang_x_sync-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_sys",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sys",
        sha256 = "0d03f4d1aa3b28ec80e2005ec8301004e2153e3154911baebf57b9cfa900f993",
        strip_prefix = "golang.org/x/sys@v0.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/sys/org_golang_x_sys-v0.11.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/sys/org_golang_x_sys-v0.11.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/sys/org_golang_x_sys-v0.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/sys/org_golang_x_sys-v0.11.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_term",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/term",
        sha256 = "d9a79ecfb908333f03d7f3f4b597551a6916462c6c5d040528c9887df956600e",
        strip_prefix = "golang.org/x/term@v0.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/term/org_golang_x_term-v0.11.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/term/org_golang_x_term-v0.11.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/term/org_golang_x_term-v0.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/term/org_golang_x_term-v0.11.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_text",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/text",
        sha256 = "437a787c7f92bcb8b2f2ab97fcd74ce88b5e7a5b21aa299e90f5c5dd28a7b66f",
        strip_prefix = "golang.org/x/text@v0.12.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/text/org_golang_x_text-v0.12.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/text/org_golang_x_text-v0.12.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/text/org_golang_x_text-v0.12.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/text/org_golang_x_text-v0.12.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_time",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/time",
        sha256 = "fa9a1fcd03a4acb817faa5d44e95f0a73182a96cb81012c9b94f832d70f7296b",
        strip_prefix = "golang.org/x/time@v0.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/time/org_golang_x_time-v0.3.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/time/org_golang_x_time-v0.3.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/time/org_golang_x_time-v0.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/time/org_golang_x_time-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_tools",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/tools",
        sha256 = "562f25e674aab49f00a23ec1fcb46a57b0a9d27287ea9a885886c994306d9c14",
        strip_prefix = "golang.org/x/tools@v0.10.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/tools/org_golang_x_tools-v0.10.0.zip",
            "http://ats.apps.svc/gomod/golang.org/x/tools/org_golang_x_tools-v0.10.0.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/tools/org_golang_x_tools-v0.10.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/tools/org_golang_x_tools-v0.10.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_xerrors",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/xerrors",
        sha256 = "b9c481db33c4b682ba8ba348018ddbd2155bd227cc38ff9f6b4cb2b74bbc3c14",
        strip_prefix = "golang.org/x/xerrors@v0.0.0-20220907171357-04be3eba64a2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/xerrors/org_golang_x_xerrors-v0.0.0-20220907171357-04be3eba64a2.zip",
            "http://ats.apps.svc/gomod/golang.org/x/xerrors/org_golang_x_xerrors-v0.0.0-20220907171357-04be3eba64a2.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/xerrors/org_golang_x_xerrors-v0.0.0-20220907171357-04be3eba64a2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/xerrors/org_golang_x_xerrors-v0.0.0-20220907171357-04be3eba64a2.zip",
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
        sha256 = "81ddbe54f9eca46effe625329bfb961ea5e63a8acc1c793e2ee1b8a61a770bf9",
        strip_prefix = "modernc.org/golex@v1.0.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/golex/org_modernc_golex-v1.0.5.zip",
            "http://ats.apps.svc/gomod/modernc.org/golex/org_modernc_golex-v1.0.5.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/golex/org_modernc_golex-v1.0.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/golex/org_modernc_golex-v1.0.5.zip",
        ],
    )
    go_repository(
        name = "org_modernc_mathutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/mathutil",
        sha256 = "c17a767eaa5eb62d9bb105b8ece7f249186dd52b9b533301bec140b3d5fd260f",
        strip_prefix = "modernc.org/mathutil@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/mathutil/org_modernc_mathutil-v1.5.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/mathutil/org_modernc_mathutil-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/mathutil/org_modernc_mathutil-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/mathutil/org_modernc_mathutil-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_parser",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/parser",
        sha256 = "fd46145315aac782cfe7199f58b01a88da814200857b136029aa635c55eff705",
        strip_prefix = "modernc.org/parser@v1.0.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/parser/org_modernc_parser-v1.0.7.zip",
            "http://ats.apps.svc/gomod/modernc.org/parser/org_modernc_parser-v1.0.7.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/parser/org_modernc_parser-v1.0.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/parser/org_modernc_parser-v1.0.7.zip",
        ],
    )
    go_repository(
        name = "org_modernc_sortutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/sortutil",
        sha256 = "0450e080b84a4964908698a9615c0629859ddc527a70f1e5c0f911836dbabc17",
        strip_prefix = "modernc.org/sortutil@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/sortutil/org_modernc_sortutil-v1.1.1.zip",
            "http://ats.apps.svc/gomod/modernc.org/sortutil/org_modernc_sortutil-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/sortutil/org_modernc_sortutil-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/sortutil/org_modernc_sortutil-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "org_modernc_strutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/strutil",
        sha256 = "2e59915393fa6a75021a97a41c60fac71c662bb9d1dc2d06e2c4ed77ea5da8cc",
        strip_prefix = "modernc.org/strutil@v1.1.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/strutil/org_modernc_strutil-v1.1.3.zip",
            "http://ats.apps.svc/gomod/modernc.org/strutil/org_modernc_strutil-v1.1.3.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/strutil/org_modernc_strutil-v1.1.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/strutil/org_modernc_strutil-v1.1.3.zip",
        ],
    )
    go_repository(
        name = "org_modernc_y",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/y",
        sha256 = "eeb1fb4b8988f45c9f6124b6b172ddfab89dbfe1dc48093c295c67401f37d564",
        strip_prefix = "modernc.org/y@v1.0.9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/y/org_modernc_y-v1.0.9.zip",
            "http://ats.apps.svc/gomod/modernc.org/y/org_modernc_y-v1.0.9.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/y/org_modernc_y-v1.0.9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/y/org_modernc_y-v1.0.9.zip",
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
        sha256 = "3c8abd84c8f255eabd30f12acfdb882701e3d804b7a0db66b7bffbb4f9b72b8d",
        strip_prefix = "go.uber.org/goleak@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/goleak/org_uber_go_goleak-v1.2.1.zip",
            "http://ats.apps.svc/gomod/go.uber.org/goleak/org_uber_go_goleak-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/goleak/org_uber_go_goleak-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/goleak/org_uber_go_goleak-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_mock",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/mock",
        sha256 = "df840a589119d0c1966e3f8888fb6b6a05b4aa793b1074c3fd4c4a508e0b0e3a",
        strip_prefix = "go.uber.org/mock@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/mock/org_uber_go_mock-v0.2.0.zip",
            "http://ats.apps.svc/gomod/go.uber.org/mock/org_uber_go_mock-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/mock/org_uber_go_mock-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/mock/org_uber_go_mock-v0.2.0.zip",
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
        sha256 = "0793f45a0c874bb560fafe7eb46c59038cd10fec58dbd4addfa0a4c35a21a80c",
        strip_prefix = "go.uber.org/zap@v1.25.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.uber.org/zap/org_uber_go_zap-v1.25.0.zip",
            "http://ats.apps.svc/gomod/go.uber.org/zap/org_uber_go_zap-v1.25.0.zip",
            "https://cache.hawkingrei.com/gomod/go.uber.org/zap/org_uber_go_zap-v1.25.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.uber.org/zap/org_uber_go_zap-v1.25.0.zip",
        ],
    )
