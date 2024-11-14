load("@bazel_gazelle//:deps.bzl", "go_repository")

def go_deps():
    # NOTE: We ensure that we pin to these specific dependencies by calling
    # this function FIRST, before calls to pull in dependencies for
    # third-party libraries (e.g. rules_go, gazelle, etc.)
    go_repository(
        name = "cc_mvdan_gofumpt",
        build_file_proto_mode = "disable_global",
        importpath = "mvdan.cc/gofumpt",
        sum = "h1:JVf4NN1mIpHogBj7ABpgOyZc65/UUOkKQFkoURsz4MM=",
        version = "v0.4.0",
    )
    go_repository(
        name = "cc_mvdan_interfacer",
        build_file_proto_mode = "disable",
        importpath = "mvdan.cc/interfacer",
        sum = "h1:WX1yoOaKQfddO/mLzdV4wptyWgoH/6hwLs7QHTixo0I=",
        version = "v0.0.0-20180901003855-c20040233aed",
    )
    go_repository(
        name = "cc_mvdan_lint",
        build_file_proto_mode = "disable",
        importpath = "mvdan.cc/lint",
        sum = "h1:DxJ5nJdkhDlLok9K6qO+5290kphDJbHOQO1DFFFTeBo=",
        version = "v0.0.0-20170908181259-adc824a0674b",
    )

    go_repository(
        name = "cc_mvdan_unparam",
        build_file_proto_mode = "disable_global",
        importpath = "mvdan.cc/unparam",
        sum = "h1:seuXWbRB1qPrS3NQnHmFKLJLtskWyueeIzmLXghMGgk=",
        version = "v0.0.0-20220706161116-678bad134442",
    )
    go_repository(
        name = "co_honnef_go_tools",
        build_file_proto_mode = "disable_global",
        importpath = "honnef.co/go/tools",
        sum = "h1:4bH5o3b5ZULQ4UrBmP+63W9r7qIkqJClEA9ko5YKx+I=",
        version = "v0.5.1",
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
        sum = "h1:zeZSRqj5yCg28tCkIV/z/lWbwvNm5qnKVS15PI8nhD0=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_4meepo_tagalign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/4meepo/tagalign",
        sha256 = "1d1264ebe9e8034ccf3dd0f0a237ca9c14e16265592115ac684ac2f2216f9dff",
        strip_prefix = "github.com/4meepo/tagalign@v1.3.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.3.4.zip",
            "http://ats.apps.svc/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.3.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.3.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/4meepo/tagalign/com_github_4meepo_tagalign-v1.3.4.zip",
        ],
    )
    go_repository(
        name = "com_github_abirdcfly_dupword",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Abirdcfly/dupword",
        sum = "h1:z14n0yytA3wNO2gpCD/jVtp/acEXPGmYu0esewpBt6Q=",
        version = "v0.0.7",
    )
    go_repository(
        name = "com_github_acarl005_stripansi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/acarl005/stripansi",
        sum = "h1:licZJFw2RwpHMqeKTCYkitsPqHNxTmd4SNR5r94FGM8=",
        version = "v0.0.0-20180116102854-5a71ef0e047d",
    )
    go_repository(
        name = "com_github_aclements_go_moremath",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aclements/go-moremath",
        sha256 = "d83b2a13bee30e772c4f414ccb02c8fec9a4d614e814e1a2c740a6567974861d",
        strip_prefix = "github.com/aclements/go-moremath@v0.0.0-20210112150236-f10218a38794",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/aclements/go-moremath/com_github_aclements_go_moremath-v0.0.0-20210112150236-f10218a38794.zip",
            "http://ats.apps.svc/gomod/github.com/aclements/go-moremath/com_github_aclements_go_moremath-v0.0.0-20210112150236-f10218a38794.zip",
            "https://cache.hawkingrei.com/gomod/github.com/aclements/go-moremath/com_github_aclements_go_moremath-v0.0.0-20210112150236-f10218a38794.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/aclements/go-moremath/com_github_aclements_go_moremath-v0.0.0-20210112150236-f10218a38794.zip",
        ],
    )
    go_repository(
        name = "com_github_ajg_form",
        build_file_proto_mode = "disable",
        importpath = "github.com/ajg/form",
        sum = "h1:t9c7v8JUKu/XxOGBU0yjNpaMloxGEJhUkqFRq0ibGeU=",
        version = "v1.5.1",
    )

    go_repository(
        name = "com_github_ajstarks_deck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajstarks/deck",
        sha256 = "68bad2e38bf5b01e6bbd7b9bbdba35da94dac72bc4ba41f8ea5fe92aa836a3c3",
        strip_prefix = "github.com/ajstarks/deck@v0.0.0-20200831202436-30c9fc6549a9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ajstarks/deck/com_github_ajstarks_deck-v0.0.0-20200831202436-30c9fc6549a9.zip",
            "http://ats.apps.svc/gomod/github.com/ajstarks/deck/com_github_ajstarks_deck-v0.0.0-20200831202436-30c9fc6549a9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ajstarks/deck/com_github_ajstarks_deck-v0.0.0-20200831202436-30c9fc6549a9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ajstarks/deck/com_github_ajstarks_deck-v0.0.0-20200831202436-30c9fc6549a9.zip",
        ],
    )
    go_repository(
        name = "com_github_ajstarks_deck_generate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajstarks/deck/generate",
        sha256 = "dce1cbc4cb42ac26512dd0bccf997baeea99fb4595cd419a28e8566d2d7c7ba8",
        strip_prefix = "github.com/ajstarks/deck/generate@v0.0.0-20210309230005-c3f852c02e19",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ajstarks/deck/generate/com_github_ajstarks_deck_generate-v0.0.0-20210309230005-c3f852c02e19.zip",
            "http://ats.apps.svc/gomod/github.com/ajstarks/deck/generate/com_github_ajstarks_deck_generate-v0.0.0-20210309230005-c3f852c02e19.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ajstarks/deck/generate/com_github_ajstarks_deck_generate-v0.0.0-20210309230005-c3f852c02e19.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ajstarks/deck/generate/com_github_ajstarks_deck_generate-v0.0.0-20210309230005-c3f852c02e19.zip",
        ],
    )
    go_repository(
        name = "com_github_ajstarks_svgo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajstarks/svgo",
        sum = "h1:wVe6/Ea46ZMeNkQjjBW6xcqyQA/j5e0D6GytH95g0gQ=",
        version = "v0.0.0-20180226025133-644b8db467af",
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
        sum = "h1:f48lwail6p8zpO1bC4TxtqACaGqHYA22qkHjHpqDjYY=",
        version = "v2.4.0",
    )
    go_repository(
        name = "com_github_alecthomas_template",
        build_file_proto_mode = "disable",
        importpath = "github.com/alecthomas/template",
        sum = "h1:JYp7IbQjafoB+tBA3gMyHYHrpOtNuDiK/uB5uXxq5wM=",
        version = "v0.0.0-20190718012654-fb15b899a751",
    )

    go_repository(
        name = "com_github_alecthomas_units",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/units",
        sum = "h1:s6gZFSlWYmbqAuRjVTiNNhvNRfY2Wxp9nhfyel4rklc=",
        version = "v0.0.0-20211218093645-b94a6e3cc137",
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
        sha256 = "52de5925201f2c9a01fe921c15d809372002d57062ebbeb7141e7d7d182ca01b",
        strip_prefix = "github.com/alexkohler/nakedret/v2@v2.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/alexkohler/nakedret/v2/com_github_alexkohler_nakedret_v2-v2.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/alexkohler/nakedret/v2/com_github_alexkohler_nakedret_v2-v2.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/alexkohler/nakedret/v2/com_github_alexkohler_nakedret_v2-v2.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/alexkohler/nakedret/v2/com_github_alexkohler_nakedret_v2-v2.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_alexkohler_prealloc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alexkohler/prealloc",
        sum = "h1:Hbq0/3fJPQhNkN0dR95AVrr6R7tou91y0uHG5pOcUuw=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_alingse_asasalint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alingse/asasalint",
        sum = "h1:SFwnQXJ49Kx/1GghOFz1XGqHYKp21Kq1nHad/0WQRnw=",
        version = "v0.0.11",
    )
    go_repository(
        name = "com_github_aliyun_alibaba_cloud_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aliyun/alibaba-cloud-sdk-go",
        sum = "h1:Q/yk4z/cHUVZfgTqtD09qeYBxHwshQAjVRX73qs8UH0=",
        version = "v1.61.1581",
    )
    go_repository(
        name = "com_github_andreasbriese_bbloom",
        build_file_proto_mode = "disable",
        importpath = "github.com/AndreasBriese/bbloom",
        sum = "h1:HD8gA2tkByhMAwYaFAX9w2l7vxvBQ5NMoxDrkhqhtn4=",
        version = "v0.0.0-20190306092124-e2d15f34fcf9",
    )

    go_repository(
        name = "com_github_andybalholm_brotli",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/andybalholm/brotli",
        sha256 = "f5ae9b2f3260a22ff3f3445fff081d3ef12ee1aa3c0b87eadc59b5a8fb2cdef0",
        strip_prefix = "github.com/andybalholm/brotli@v1.0.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/andybalholm/brotli/com_github_andybalholm_brotli-v1.0.5.zip",
            "http://ats.apps.svc/gomod/github.com/andybalholm/brotli/com_github_andybalholm_brotli-v1.0.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/andybalholm/brotli/com_github_andybalholm_brotli-v1.0.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/andybalholm/brotli/com_github_andybalholm_brotli-v1.0.5.zip",
        ],
    )
    go_repository(
        name = "com_github_antihax_optional",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/antihax/optional",
        sum = "h1:xK2lYat7ZLaVVcIuj82J8kIro4V6kDe0AUDFboUCwcg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_antonboom_errname",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Antonboom/errname",
        sum = "h1:mBBDKvEYwPl4WFFNwec1CZO096G6vzK9vvDQzAwkako=",
        version = "v0.1.7",
    )
    go_repository(
        name = "com_github_antonboom_nilnil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Antonboom/nilnil",
        sum = "h1:PHhrh5ANKFWRBh7TdYmyyq2gyT2lotnvFvvFbylF81Q=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_antonboom_testifylint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Antonboom/testifylint",
        sha256 = "a047122f5f48e41e25e25ee91f31a0d4b0b6ad608d8d2e99e4e74f7b6ab7f114",
        strip_prefix = "github.com/Antonboom/testifylint@v1.4.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Antonboom/testifylint/com_github_antonboom_testifylint-v1.4.3.zip",
            "http://ats.apps.svc/gomod/github.com/Antonboom/testifylint/com_github_antonboom_testifylint-v1.4.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Antonboom/testifylint/com_github_antonboom_testifylint-v1.4.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Antonboom/testifylint/com_github_antonboom_testifylint-v1.4.3.zip",
        ],
    )
    go_repository(
        name = "com_github_apache_arrow_go_v12",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/apache/arrow/go/v12",
        sha256 = "5eb05ed9c2c5e164503b00912b7b2456400578de29e7e8a8956a41acd861ab5b",
        strip_prefix = "github.com/apache/arrow/go/v12@v12.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/apache/arrow/go/v12/com_github_apache_arrow_go_v12-v12.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/apache/arrow/go/v12/com_github_apache_arrow_go_v12-v12.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/apache/arrow/go/v12/com_github_apache_arrow_go_v12-v12.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/apache/arrow/go/v12/com_github_apache_arrow_go_v12-v12.0.1.zip",
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
        sum = "h1:Jz3KVLYY5+JO7rDiX0sAuRGtuv2vG01r17Y9nLMWNUw=",
        version = "v0.13.1-0.20201008052519-daf620915714",
    )
    go_repository(
        name = "com_github_armon_circbuf",
        build_file_proto_mode = "disable",
        importpath = "github.com/armon/circbuf",
        sum = "h1:QEF07wC0T1rKkctt1RINW/+RMTVmiwxETico2l3gxJA=",
        version = "v0.0.0-20150827004946-bbbad097214e",
    )

    go_repository(
        name = "com_github_armon_consul_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/consul-api",
        sum = "h1:G1bPvciwNyF7IUmKXNt9Ak3m6u9DE1rF+RmtIkBpVdA=",
        version = "v0.0.0-20180202201655-eb2c6b5be1b6",
    )
    go_repository(
        name = "com_github_armon_go_metrics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/go-metrics",
        sum = "h1:8GUt8eRujhVEGZFFEjBj46YV4rDjvGrNxb0KMWYkL2I=",
        version = "v0.0.0-20180917152333-f0300d1749da",
    )
    go_repository(
        name = "com_github_armon_go_radix",
        build_file_proto_mode = "disable",
        importpath = "github.com/armon/go-radix",
        sum = "h1:BUAU3CGlLvorLI26FmByPp2eC2qla6E1Tw+scpcg/to=",
        version = "v0.0.0-20180808171621-7fddfc383310",
    )

    go_repository(
        name = "com_github_armon_go_socks5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/go-socks5",
        sum = "h1:0CwZNZbxp69SHPdPJAN/hZIm0C4OItdklCFmMRWYpio=",
        version = "v0.0.0-20160902184237-e75332964ef5",
    )
    go_repository(
        name = "com_github_asaskevich_govalidator",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/asaskevich/govalidator",
        sum = "h1:idn718Q4B6AGu/h5Sxe66HYVdqdGu2l9Iebqhi/AEoA=",
        version = "v0.0.0-20190424111038-f61b66f89f4a",
    )
    go_repository(
        name = "com_github_ashanbrown_forbidigo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ashanbrown/forbidigo",
        sum = "h1:VkYIwb/xxdireGAdJNZoo24O4lmnEWkactplBlWTShc=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_ashanbrown_makezero",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ashanbrown/makezero",
        sum = "h1:iCQ87C0V0vSyO+M9E/FZYbu65auqH0lnsOkf5FcB28s=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go",
        sum = "h1:7yDn1dcv4DZFMKpu+2exIH5O6ipNj9qXrKfdMUaIJwY=",
        version = "v1.44.259",
    )
    go_repository(
        name = "com_github_aymerick_douceur",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aymerick/douceur",
        sha256 = "dcbf69760cc1a8b32384495438e1086e4c3d669b2ebc0debd92e1865ffd6be60",
        strip_prefix = "github.com/aymerick/douceur@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/aymerick/douceur/com_github_aymerick_douceur-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/aymerick/douceur/com_github_aymerick_douceur-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/aymerick/douceur/com_github_aymerick_douceur-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/aymerick/douceur/com_github_aymerick_douceur-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_aymerick_raymond",
        build_file_proto_mode = "disable",
        importpath = "github.com/aymerick/raymond",
        sum = "h1:Ppm0npCCsmuR9oQaBtRuZcmILVE74aXE+AmrJj8L2ns=",
        version = "v2.0.3-0.20180322193309-b565731e1464+incompatible",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go",
        build_file_proto_mode = "disable",
        importpath = "github.com/Azure/azure-sdk-for-go",
        sum = "h1:bch1RS060vGpHpY3zvQDV4rOiRw25J1zmR/B9a76aSA=",
        version = "v23.2.0+incompatible",
    )

    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azcore",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azcore",
        sum = "h1:JZg6HRh6W6U4OLl6lk7BZ7BLisIzM9dG1R50zUk9C/M=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azidentity",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azidentity",
        sum = "h1:B/dfvscEQtew9dVuoxqxrUKKv8Ih2f55PydknDamU+g=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azidentity_cache",
        build_file_proto_mode = "disable",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azidentity/cache",
        sum = "h1:+m0M/LFxN43KvULkDNfdXOgrjtg6UYJPFBJyuEcRCAw=",
        version = "v0.3.0",
    )

    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_internal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/internal",
        sum = "h1:ywEEhmNahHBihViHepv3xPBn1663uRv2t2q/ESv9seY=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5",
        sha256 = "f47869d09394fbe965b013f539bf7c1c65af9833dbfea0c12f7b6a081870b6f6",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5@v5.4.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v5-v5.4.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v5-v5.4.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v5-v5.4.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v5-v5.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4",
        sha256 = "e002f35fb0d7200d8cd31bc6b1e91e56400ddc3f50887e8795285268ac5bdaff",
        strip_prefix = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4@v4.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v4-v4.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v4-v4.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v4-v4.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4/com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v4-v4.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_resourcemanager_storage_armstorage",
        build_file_proto_mode = "disable",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage",
        sum = "h1:PiSrjRPpkQNjrM8H0WwKMnZUdu1RGMtd/LdGKUrOo+c=",
        version = "v1.6.0",
    )

    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_storage_azblob",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob",
        sum = "h1:mlmW46Q0B79I+Aj4azKC6xDMFN9a9SyZWESlGWYXbFs=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_azure_go_autorest",
        build_file_proto_mode = "disable",
        importpath = "github.com/Azure/go-autorest",
        sum = "h1:Q2feRPMlcfVcqz3pF87PJzkm5lZrL+x6BDtzhODzNJM=",
        version = "v11.2.8+incompatible",
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
        name = "com_github_azuread_microsoft_authentication_extensions_for_go_cache",
        build_file_proto_mode = "disable",
        importpath = "github.com/AzureAD/microsoft-authentication-extensions-for-go/cache",
        sum = "h1:WJTmL004Abzc5wDB5VtZG2PJk5ndYDgVacGqfirKxjM=",
        version = "v0.1.1",
    )

    go_repository(
        name = "com_github_azuread_microsoft_authentication_library_for_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/AzureAD/microsoft-authentication-library-for-go",
        sum = "h1:gUDtaZk8heteyfdmv+pcfHvhR9llnh7c7GMwZ8RVG04=",
        version = "v1.3.1",
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
        sum = "h1:/BUvuaB8MEiUA2oLPPCGtuw5V+doAYyiGTFyoSWlkrw=",
        version = "v0.50.1",
    )
    go_repository(
        name = "com_github_bboreham_go_loser",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bboreham/go-loser",
        sha256 = "d39c329a916c6e3af23a77ed3615f49726258cf3fa7b126c5ad06e7a5c3cbb4f",
        strip_prefix = "github.com/bboreham/go-loser@v0.0.0-20230920113527-fcc2c21820a3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bboreham/go-loser/com_github_bboreham_go_loser-v0.0.0-20230920113527-fcc2c21820a3.zip",
            "http://ats.apps.svc/gomod/github.com/bboreham/go-loser/com_github_bboreham_go_loser-v0.0.0-20230920113527-fcc2c21820a3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bboreham/go-loser/com_github_bboreham_go_loser-v0.0.0-20230920113527-fcc2c21820a3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bboreham/go-loser/com_github_bboreham_go_loser-v0.0.0-20230920113527-fcc2c21820a3.zip",
        ],
    )
    go_repository(
        name = "com_github_benbjohnson_clock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/benbjohnson/clock",
        sum = "h1:ip6w0uFQkncKQ979AypyG0ER7mqUSBdKLOgAle/AT8A=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_beorn7_perks",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/beorn7/perks",
        sum = "h1:VlbKKnNfV8bJzeqoa4cOKqO6bYr3WgKZxO8Z16+hsOM=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_bgentry_speakeasy",
        build_file_proto_mode = "disable",
        importpath = "github.com/bgentry/speakeasy",
        sum = "h1:ByYyxL9InA1OWqxJqqp2A5pYHUrCiAL6K3J+LKSsQkY=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_biogo_store",
        build_file_proto_mode = "disable",
        importpath = "github.com/biogo/store",
        sum = "h1:tYoz1OeRpx3dJZlh9T4dQt4kAndcmpl+VNdzbSgFC/0=",
        version = "v0.0.0-20160505134755-913427a1d5e8",
    )

    go_repository(
        name = "com_github_bits_and_blooms_bitset",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bits-and-blooms/bitset",
        sha256 = "29542828d3fa62199ac8fe6b69ed5284502b52549e1c64dcdbeeed4eab981a37",
        strip_prefix = "github.com/bits-and-blooms/bitset@v1.14.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bits-and-blooms/bitset/com_github_bits_and_blooms_bitset-v1.14.3.zip",
            "http://ats.apps.svc/gomod/github.com/bits-and-blooms/bitset/com_github_bits_and_blooms_bitset-v1.14.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bits-and-blooms/bitset/com_github_bits_and_blooms_bitset-v1.14.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bits-and-blooms/bitset/com_github_bits_and_blooms_bitset-v1.14.3.zip",
        ],
    )
    go_repository(
        name = "com_github_bketelsen_crypt",
        build_file_proto_mode = "disable",
        importpath = "github.com/bketelsen/crypt",
        sum = "h1:+0HFd5KSZ/mm3JmhmrDukiId5iR6w4+BdFtfSy4yWIc=",
        version = "v0.0.3-0.20200106085610-5cbc8cc4026c",
    )

    go_repository(
        name = "com_github_bkielbasa_cyclop",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bkielbasa/cyclop",
        sum = "h1:7Jmnh0yL2DjKfw28p86YTd/B4lRGcNuu12sKE35sM7A=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_blacktear23_go_proxyprotocol",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/blacktear23/go-proxyprotocol",
        sum = "h1:eTt6UMpEnq59NjON49b3Cay8Dm0sCs1nDliwgkyEsRM=",
        version = "v1.0.6",
    )
    go_repository(
        name = "com_github_blizzy78_varnamelen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/blizzy78/varnamelen",
        sum = "h1:oqSblyuQvFsW1hbBHh1zfwrKe3kcSj0rnXkKzsQ089M=",
        version = "v0.8.0",
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
        build_file_proto_mode = "disable",
        importpath = "github.com/bombsimon/wsl/v3",
        sum = "h1:Mka/+kRLoQJq7g2rggtgQsjuI/K5Efd87WX96EWFxjM=",
        version = "v3.3.0",
    )

    go_repository(
        name = "com_github_bombsimon_wsl_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bombsimon/wsl/v4",
        sha256 = "7071bec41122acb8bcd49d778c4c9606d9e43faa5120eb51bfb5e347bafc3023",
        strip_prefix = "github.com/bombsimon/wsl/v4@v4.4.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/bombsimon/wsl/v4/com_github_bombsimon_wsl_v4-v4.4.1.zip",
            "http://ats.apps.svc/gomod/github.com/bombsimon/wsl/v4/com_github_bombsimon_wsl_v4-v4.4.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/bombsimon/wsl/v4/com_github_bombsimon_wsl_v4-v4.4.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/bombsimon/wsl/v4/com_github_bombsimon_wsl_v4-v4.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_boombuler_barcode",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/boombuler/barcode",
        sha256 = "812c5beeaa87864227f9d92a9ae71792dc0e6302a33737a91aabe1e511cde42b",
        strip_prefix = "github.com/boombuler/barcode@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/boombuler/barcode/com_github_boombuler_barcode-v1.0.1.zip",
            "http://ats.apps.svc/gomod/github.com/boombuler/barcode/com_github_boombuler_barcode-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/boombuler/barcode/com_github_boombuler_barcode-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/boombuler/barcode/com_github_boombuler_barcode-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_breml_bidichk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/breml/bidichk",
        sum = "h1:qe6ggxpTfA8E75hdjWPZ581sY3a2lnl0IRxLQFelECI=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_breml_errchkjson",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/breml/errchkjson",
        sum = "h1:YdDqhfqMT+I1vIxPSas44P+9Z9HzJwCeAzjB8PxP1xw=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_burntsushi_toml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/BurntSushi/toml",
        sum = "h1:pxW6RcqyfI9/kWtOwnv/G+AzdKuy2ZrqINhenH4HyNs=",
        version = "v1.4.1-0.20240526193622-a339e1f7089c",
    )
    go_repository(
        name = "com_github_burntsushi_xgb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/BurntSushi/xgb",
        sum = "h1:1BDTz0u9nC3//pOCMdNH+CiXJVYJh5UQNCOBG7jbELc=",
        version = "v0.0.0-20160522181843-27f122750802",
    )
    go_repository(
        name = "com_github_butuzov_ireturn",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/butuzov/ireturn",
        sum = "h1:QvrO2QF2+/Cx1WA/vETCIYBKtRjc30vesdoPUNo1EbY=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_butuzov_mirror",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/butuzov/mirror",
        sha256 = "81803d1cfc8f32392dcbd24649f43813a9ff560f50c178aa370e1edb2c8fcf41",
        strip_prefix = "github.com/butuzov/mirror@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/butuzov/mirror/com_github_butuzov_mirror-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/butuzov/mirror/com_github_butuzov_mirror-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/butuzov/mirror/com_github_butuzov_mirror-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/butuzov/mirror/com_github_butuzov_mirror-v1.2.0.zip",
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
        sum = "h1:/c4uK3ie786Z7caXLcIMvePNSSiH3bQVGDvmGLMme60=",
        version = "v0.21.0",
    )
    go_repository(
        name = "com_github_catenacyber_perfsprint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/catenacyber/perfsprint",
        sha256 = "a4f47bb9e144f8237200bf597242c8ecf5b4cc99e7b5ee94658d3a7f052dd8af",
        strip_prefix = "github.com/catenacyber/perfsprint@v0.7.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/catenacyber/perfsprint/com_github_catenacyber_perfsprint-v0.7.1.zip",
            "http://ats.apps.svc/gomod/github.com/catenacyber/perfsprint/com_github_catenacyber_perfsprint-v0.7.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/catenacyber/perfsprint/com_github_catenacyber_perfsprint-v0.7.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/catenacyber/perfsprint/com_github_catenacyber_perfsprint-v0.7.1.zip",
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
        name = "com_github_cenk_backoff",
        build_file_proto_mode = "disable",
        importpath = "github.com/cenk/backoff",
        sum = "h1:7vXVw3g7XE+Vnj0A9TmFGtMeP4oZQ5ZzpPvKhLFa80E=",
        version = "v2.0.0+incompatible",
    )

    go_repository(
        name = "com_github_cenkalti_backoff_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cenkalti/backoff/v4",
        sum = "h1:G2HAfAmvm/GcKan2oOQpBXOd2tT2G57ZnZGWa1PxPBQ=",
        version = "v4.1.1",
    )
    go_repository(
        name = "com_github_census_instrumentation_opencensus_proto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/census-instrumentation/opencensus-proto",
        sum = "h1:iKLQ0xPNFxR/2hzXZMrBo8f1j86j5WHzznCCQxV/b8g=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_certifi_gocertifi",
        build_file_proto_mode = "disable",
        importpath = "github.com/certifi/gocertifi",
        sum = "h1:uH66TXeswKn5PW5zdZ39xEwfS9an067BirqA+P4QaLI=",
        version = "v0.0.0-20200922220541-2c3bb06c6054",
    )
    go_repository(
        name = "com_github_cespare_xxhash",
        build_file_proto_mode = "disable",
        importpath = "github.com/cespare/xxhash",
        sum = "h1:a6HrQnmkObjyL+Gs60czilIUGqrzKutQD6XZog3p+ko=",
        version = "v1.1.0",
    )

    go_repository(
        name = "com_github_cespare_xxhash_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cespare/xxhash/v2",
        sum = "h1:UL815xU9SqsFlibzuggzjXhog7bL6oX9BbNZnL2UFvs=",
        version = "v2.3.0",
    )
    go_repository(
        name = "com_github_charithe_durationcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/charithe/durationcheck",
        sum = "h1:mPP4ucLrf/rKZiIG/a9IPXHGlh8p4CzgpyTy6EEutYk=",
        version = "v0.0.9",
    )
    go_repository(
        name = "com_github_chavacava_garif",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chavacava/garif",
        sum = "h1:E7LT642ysztPWE0dfz43cWOvMiF42DyTRC+eZIaO4yI=",
        version = "v0.0.0-20220630083739-93517212f375",
    )
    go_repository(
        name = "com_github_cheggaaa_pb_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cheggaaa/pb/v3",
        sum = "h1:bC8oemdChbke2FHIIGy9mn4DPJ2caZYQnfbRqwmdCoA=",
        version = "v3.0.8",
    )
    go_repository(
        name = "com_github_cheynewallace_tabby",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cheynewallace/tabby",
        sum = "h1:JvUR8waht4Y0S3JF17G6Vhyt+FRhnqVCkk8l4YrOU54=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_chzyer_logex",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chzyer/logex",
        sum = "h1:Swpa1K6QvQznwJRcfTfQJmTE72DqScAa40E+fbHEXEE=",
        version = "v1.1.10",
    )
    go_repository(
        name = "com_github_chzyer_readline",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chzyer/readline",
        sum = "h1:fY5BOSpyZCqRo5OhCuC+XN+r/bBCmeuuJtjz+bCNIf8=",
        version = "v0.0.0-20180603132655-2972be24d48e",
    )
    go_repository(
        name = "com_github_chzyer_test",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chzyer/test",
        sum = "h1:q763qf9huN11kDQavWsoZXJNW3xEE4JJyHa5Q25/sd8=",
        version = "v0.0.0-20180213035817-a1ea475d72b1",
    )
    go_repository(
        name = "com_github_cilium_ebpf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cilium/ebpf",
        sha256 = "5130d073e34b07b80ecf6cccb1ff4b20ecee59bf1b5b71272ef9e90949a85952",
        strip_prefix = "github.com/cilium/ebpf@v0.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cilium/ebpf/com_github_cilium_ebpf-v0.11.0.zip",
            "http://ats.apps.svc/gomod/github.com/cilium/ebpf/com_github_cilium_ebpf-v0.11.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cilium/ebpf/com_github_cilium_ebpf-v0.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cilium/ebpf/com_github_cilium_ebpf-v0.11.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ckaznocha_intrange",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ckaznocha/intrange",
        sha256 = "2cdefa74cabc9bfe27168929f68fe2927143899e8a340e932d030c3206101d82",
        strip_prefix = "github.com/ckaznocha/intrange@v0.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ckaznocha/intrange/com_github_ckaznocha_intrange-v0.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/ckaznocha/intrange/com_github_ckaznocha_intrange-v0.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ckaznocha/intrange/com_github_ckaznocha_intrange-v0.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ckaznocha/intrange/com_github_ckaznocha_intrange-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_client9_misspell",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/client9/misspell",
        sum = "h1:ta993UF76GwbvJcIo3Y68y/M3WxlpEHPWIGDkJYwzJI=",
        version = "v0.3.4",
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
        sum = "h1:3SgJcK9l5uPdBC/X17wanyJAMxM33+4ZhEIV96MIH8U=",
        version = "v0.0.0-20170127035650-74b38d55f37a",
    )
    go_repository(
        name = "com_github_cloudykit_jet",
        build_file_proto_mode = "disable",
        importpath = "github.com/CloudyKit/jet",
        sum = "h1:rZgFj+Gtf3NMi/U5FvCvhzaxzW/TaPYgUYx3bAPz9DE=",
        version = "v2.1.3-0.20180809161101-62edd43e4f88+incompatible",
    )

    go_repository(
        name = "com_github_cloudykit_jet_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/CloudyKit/jet/v6",
        sha256 = "24c18e2a19eb56a01fce96e2504196f85d1c2291ff448f20dd32f6247a979264",
        strip_prefix = "github.com/CloudyKit/jet/v6@v6.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/CloudyKit/jet/v6/com_github_cloudykit_jet_v6-v6.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/CloudyKit/jet/v6/com_github_cloudykit_jet_v6-v6.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/CloudyKit/jet/v6/com_github_cloudykit_jet_v6-v6.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/CloudyKit/jet/v6/com_github_cloudykit_jet_v6-v6.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cncf_udpa_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cncf/udpa/go",
        sum = "h1:QQ3GSy+MqSHxm/d8nCtnAiZdYFd45cYZPs8vOOIYKfk=",
        version = "v0.0.0-20220112060539-c52dc94e7fbe",
    )
    go_repository(
        name = "com_github_cncf_xds_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cncf/xds/go",
        sum = "h1:/inchEIKaYC1Akx+H+gqO04wryn5h75LSazbRlnya1k=",
        version = "v0.0.0-20230607035331-e9ce68804cb4",
    )
    go_repository(
        name = "com_github_cockroachdb_apd",
        build_file_proto_mode = "disable",
        importpath = "github.com/cockroachdb/apd",
        sum = "h1:3LFP3629v+1aKXU5Q37mxmRxX/pIu1nijXydLShEq5I=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_cockroachdb_cmux",
        build_file_proto_mode = "disable",
        importpath = "github.com/cockroachdb/cmux",
        sum = "h1:dzj1/xcivGjNPwwifh/dWTczkwcuqsXXFHY1X/TZMtw=",
        version = "v0.0.0-20170110192607-30d10be49292",
    )
    go_repository(
        name = "com_github_cockroachdb_cockroach",
        build_file_proto_mode = "disable",
        importpath = "github.com/cockroachdb/cockroach",
        sum = "h1:0FHGBrsIyDci8tF7zujQkHdMTJdCTSIV9esrni2fKQI=",
        version = "v0.0.0-20170608034007-84bc9597164f",
    )
    go_repository(
        name = "com_github_cockroachdb_cockroach_go",
        build_file_proto_mode = "disable",
        importpath = "github.com/cockroachdb/cockroach-go",
        sum = "h1:2zRrJWIt/f9c9HhNHAgrRgq0San5gRRUJTBXLkchal0=",
        version = "v0.0.0-20181001143604-e0a95dfd547c",
    )

    go_repository(
        name = "com_github_cockroachdb_datadriven",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/datadriven",
        sum = "h1:H9MtNqVoVhvd9nCBwOyDjUEdZCREqbIdCJD93PBm/jA=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_cockroachdb_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/errors",
        sum = "h1:A5+txlVZfOqFBDa4mGz2bUWSp0aHElvHX2bKkdbQu+Y=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_github_cockroachdb_logtags",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/logtags",
        sum = "h1:o/kfcElHqOiXqcou5a3rIlMc7oJbMQkeLk0VQJ7zgqY=",
        version = "v0.0.0-20190617123548-eb05cc24525f",
    )
    go_repository(
        name = "com_github_cockroachdb_pebble",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/pebble",
        sum = "h1:Igd6YmtOZ77EgLAIaE9+mHl7+sAKaZ5m4iMI0Dz/J2A=",
        version = "v0.0.0-20210719141320-8c3bd06debb5",
    )
    go_repository(
        name = "com_github_cockroachdb_redact",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/redact",
        sum = "h1:8QG/764wK+vmEYoOlfobpe12EQcS81ukx/a4hdVMxNw=",
        version = "v1.0.8",
    )
    go_repository(
        name = "com_github_cockroachdb_sentry_go",
        build_file_proto_mode = "disable",
        importpath = "github.com/cockroachdb/sentry-go",
        sum = "h1:IKgmqgMQlVJIZj19CdocBeSfSaiCbEBZGKODaixqtHM=",
        version = "v0.6.1-cockroachdb.2",
    )

    go_repository(
        name = "com_github_cockroachdb_tokenbucket",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/tokenbucket",
        sha256 = "150f3e8e5b515c0886cda0809f09b5d5173d7f2c30eb2f2c6045c2aeb2183aa3",
        strip_prefix = "github.com/cockroachdb/tokenbucket@v0.0.0-20230807174530-cc333fc44b06",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/cockroachdb/tokenbucket/com_github_cockroachdb_tokenbucket-v0.0.0-20230807174530-cc333fc44b06.zip",
            "http://ats.apps.svc/gomod/github.com/cockroachdb/tokenbucket/com_github_cockroachdb_tokenbucket-v0.0.0-20230807174530-cc333fc44b06.zip",
            "https://cache.hawkingrei.com/gomod/github.com/cockroachdb/tokenbucket/com_github_cockroachdb_tokenbucket-v0.0.0-20230807174530-cc333fc44b06.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/cockroachdb/tokenbucket/com_github_cockroachdb_tokenbucket-v0.0.0-20230807174530-cc333fc44b06.zip",
        ],
    )
    go_repository(
        name = "com_github_codahale_hdrhistogram",
        build_file_proto_mode = "disable",
        importpath = "github.com/codahale/hdrhistogram",
        sum = "h1:qMd81Ts1T2OTKmB4acZcyKaMtRnY5Y44NuXGX2GFJ1w=",
        version = "v0.0.0-20161010025455-3a0bb77429bd",
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
        sum = "h1:sDMmm+q/3+BukdIpxwO365v/Rbspp2Nt5XntgQRXq8Q=",
        version = "v0.0.0-20150114235600-33e0aa1cb7c0",
    )
    go_repository(
        name = "com_github_colinmarc_hdfs_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/colinmarc/hdfs/v2",
        sum = "h1:x0hw/m+o3UE20Scso/KCkvYNc9Di39TBlCfGMkJ1/a0=",
        version = "v2.1.1",
    )
    go_repository(
        name = "com_github_containerd_cgroups_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/cgroups/v3",
        sha256 = "bc22dd9c675f1abd77a9de83506abb15656529848b2274323a88ea9bfce980be",
        strip_prefix = "github.com/containerd/cgroups/v3@v3.0.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/containerd/cgroups/v3/com_github_containerd_cgroups_v3-v3.0.3.zip",
            "http://ats.apps.svc/gomod/github.com/containerd/cgroups/v3/com_github_containerd_cgroups_v3-v3.0.3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/containerd/cgroups/v3/com_github_containerd_cgroups_v3-v3.0.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/containerd/cgroups/v3/com_github_containerd_cgroups_v3-v3.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_log",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/log",
        sha256 = "2008faf206ec820e7fc3d40baba924936c21347dafad4a7ff122fa90e26e57d7",
        strip_prefix = "github.com/containerd/log@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/containerd/log/com_github_containerd_log-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/containerd/log/com_github_containerd_log-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/containerd/log/com_github_containerd_log-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/containerd/log/com_github_containerd_log-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_coocood_bbloom",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coocood/bbloom",
        sum = "h1:W1SHiII3e0jVwvaQFglwu3kS9NLxOeTpvik7MbKCyuQ=",
        version = "v0.0.0-20190830030839-58deb6228d64",
    )
    go_repository(
        name = "com_github_coocood_freecache",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coocood/freecache",
        sum = "h1:/v1CqMq45NFH9mp/Pt142reundeBM0dVUD3osQBeu/U=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_coocood_rtutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coocood/rtutil",
        sum = "h1:NnLfQ77q0G4k2Of2c1ceQ0ec6MkLQyDp+IGdVM0D8XM=",
        version = "v0.0.0-20190304133409-c84515f646f2",
    )
    go_repository(
        name = "com_github_coreos_bbolt",
        build_file_proto_mode = "disable",
        importpath = "github.com/coreos/bbolt",
        sum = "h1:wZwiHHUieZCquLkDL0B8UhzreNWsPHooDAG3q34zk0s=",
        version = "v1.3.2",
    )

    go_repository(
        name = "com_github_coreos_etcd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/etcd",
        sum = "h1:8F3hqu9fGYLBifCmRCJsicFqDx/D68Rt3q1JMazcgBQ=",
        version = "v3.3.13+incompatible",
    )
    go_repository(
        name = "com_github_coreos_go_etcd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-etcd",
        sum = "h1:bXhRBIXoTm9BYHS3gE0TtQuyNZyeEMux2sDi4oo5YOo=",
        version = "v2.0.0+incompatible",
    )
    go_repository(
        name = "com_github_coreos_go_semver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-semver",
        sum = "h1:yi21YpKnrx1gt5R+la8n5WgS0kCrsPp33dmEyHReZr4=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_coreos_go_systemd",
        build_file_proto_mode = "disable",
        importpath = "github.com/coreos/go-systemd",
        sum = "h1:Wf6HqHfScWJN9/ZjdUKyjop4mf3Qdd+1TvvltAvM3m8=",
        version = "v0.0.0-20190321100706-95778dfbb74e",
    )

    go_repository(
        name = "com_github_coreos_go_systemd_v22",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-systemd/v22",
        sum = "h1:D9/bQk5vlXQFZ6Kwuu6zaiXJ9oTPe68++AzAJc1DzSI=",
        version = "v22.3.2",
    )
    go_repository(
        name = "com_github_coreos_pkg",
        build_file_proto_mode = "disable",
        importpath = "github.com/coreos/pkg",
        sum = "h1:lBNOc5arjvs8E5mO2tbpBpLoyyu8B6e44T7hJy6potg=",
        version = "v0.0.0-20180928190104-399ea9e2e55f",
    )

    go_repository(
        name = "com_github_cpuguy83_go_md2man",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cpuguy83/go-md2man",
        sum = "h1:BSKMNlYxDvnunlTymqtgONjNnaRV1sTpcovwwjF22jk=",
        version = "v1.0.10",
    )
    go_repository(
        name = "com_github_cpuguy83_go_md2man_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cpuguy83/go-md2man/v2",
        sum = "h1:p1EgwI/C7NhT0JmVkwCD2ZBK8j4aeHQX2pMHHBfMQ6w=",
        version = "v2.0.2",
    )
    go_repository(
        name = "com_github_creack_pty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/creack/pty",
        sum = "h1:07n33Z8lZxZ2qwegKbObQohDhXDQxiMMz1NOUGYlesw=",
        version = "v1.1.11",
    )
    go_repository(
        name = "com_github_crocmagnon_fatcontext",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Crocmagnon/fatcontext",
        sha256 = "1ff4d8805b98e74ffe50e6b6d1c3759b69b09c8babe800b88aa25e2fc10b6805",
        strip_prefix = "github.com/Crocmagnon/fatcontext@v0.5.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Crocmagnon/fatcontext/com_github_crocmagnon_fatcontext-v0.5.2.zip",
            "http://ats.apps.svc/gomod/github.com/Crocmagnon/fatcontext/com_github_crocmagnon_fatcontext-v0.5.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Crocmagnon/fatcontext/com_github_crocmagnon_fatcontext-v0.5.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Crocmagnon/fatcontext/com_github_crocmagnon_fatcontext-v0.5.2.zip",
        ],
    )
    go_repository(
        name = "com_github_curioswitch_go_reassign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/curioswitch/go-reassign",
        sum = "h1:G9UZyOcpk/d7Gd6mqYgd8XYWFMw/znxwGDUstnC9DIo=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_cznic_mathutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cznic/mathutil",
        sum = "h1:iwZdTE0PVqJCos1vaoKsclOGD3ADKpshg3SRtYBbwso=",
        version = "v0.0.0-20181122101859-297441e03548",
    )
    go_repository(
        name = "com_github_cznic_sortutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cznic/sortutil",
        sum = "h1:LpMLYGyy67BoAFGda1NeOBQwqlv7nUXpm+rIVHGxZZ4=",
        version = "v0.0.0-20181122101858-f5f958428db8",
    )
    go_repository(
        name = "com_github_cznic_strutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cznic/strutil",
        sum = "h1:0rkFMAbn5KBKNpJyHQ6Prb95vIKanmAe62KxsrN+sqA=",
        version = "v0.0.0-20171016134553-529a34b1c186",
    )
    go_repository(
        name = "com_github_daixiang0_gci",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/daixiang0/gci",
        sum = "h1:yBdsd376w+RIBvFXjj0MAcGWS8cSCfAlRNPfn5xvjl0=",
        version = "v0.8.5",
    )
    go_repository(
        name = "com_github_danjacques_gofslock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/danjacques/gofslock",
        sum = "h1:X6mKGhCFOxrKeeHAjv/3UvT6e5RRxW6wRdlqlV6/H4w=",
        version = "v0.0.0-20191023191349-0a45f885bc37",
    )
    go_repository(
        name = "com_github_data_dog_go_sqlmock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/DATA-DOG/go-sqlmock",
        sum = "h1:Shsta01QNfFxHCfpW6YH2STWB0MudeXXEWMr20OEh60=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_datadog_zstd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/DataDog/zstd",
        sum = "h1:EndNeuB0l9syBZhut0wns3gV1hL8zX8LIu6ZiVHWLIQ=",
        version = "v1.4.5",
    )
    go_repository(
        name = "com_github_davecgh_go_spew",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/davecgh/go-spew",
        sum = "h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_decred_dcrd_crypto_blake256",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/decred/dcrd/crypto/blake256",
        sum = "h1:7PltbUIQB7u/FfZ39+DGa/ShuMyJ5ilcvdfma9wOH6Y=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_decred_dcrd_dcrec_secp256k1_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/decred/dcrd/dcrec/secp256k1/v4",
        sum = "h1:8UrgZ3GkP4i/CLijOJx79Yu+etlyjdBU4sfcs2WYQMs=",
        version = "v4.2.0",
    )
    go_repository(
        name = "com_github_denis_tingaikin_go_header",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/denis-tingaikin/go-header",
        sum = "h1:tEaZKAlqql6SKCY++utLmkPLd6K8IBM20Ha7UVm+mtU=",
        version = "v0.4.3",
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
        build_file_proto_mode = "disable",
        importpath = "github.com/dgraph-io/badger",
        sum = "h1:DshxFxZWXUcO0xX476VJC07Xsr6ZCBVRHKZ93Oh7Evo=",
        version = "v1.6.0",
    )

    go_repository(
        name = "com_github_dgraph_io_ristretto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgraph-io/ristretto",
        sum = "h1:6CWw5tJNgpegArSHpNHJKldNeq03FQCwYvfMVWajOK8=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_dgrijalva_jwt_go",
        build_file_proto_mode = "disable",
        importpath = "github.com/dgrijalva/jwt-go",
        replace = "github.com/form3tech-oss/jwt-go",
        sum = "h1:0sWoh2EtO7UrQdNTAN+hnU3QXa4AoivplyPLLHkcrLk=",
        version = "v3.2.6-0.20210809144907-32ab6a8243d7+incompatible",
    )

    go_repository(
        name = "com_github_dgryski_go_farm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgryski/go-farm",
        sum = "h1:fAjc9m62+UWV/WAFKLNi6ZS0675eEUC9y3AlwSbQu1Y=",
        version = "v0.0.0-20200201041132-a6ae2369ad13",
    )
    go_repository(
        name = "com_github_dgryski_go_rendezvous",
        build_file_proto_mode = "disable",
        importpath = "github.com/dgryski/go-rendezvous",
        sum = "h1:lO4WD4F/rVNCu3HqELle0jiPLLBs70cWOduZpkS1E78=",
        version = "v0.0.0-20200823014737-9f7001d12a5f",
    )

    go_repository(
        name = "com_github_dgryski_go_sip13",
        build_file_proto_mode = "disable",
        importpath = "github.com/dgryski/go-sip13",
        sum = "h1:RMLoZVzv4GliuWafOuPuQDKSm1SJph7uCRnnS61JAn4=",
        version = "v0.0.0-20181026042036-e10d5fee7954",
    )

    go_repository(
        name = "com_github_digitalocean_godo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/digitalocean/godo",
        sha256 = "05d6f193e8313915fbea712226612a8d0bdc8e61975564262982b6ca106d38ee",
        strip_prefix = "github.com/digitalocean/godo@v1.108.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/digitalocean/godo/com_github_digitalocean_godo-v1.108.0.zip",
            "http://ats.apps.svc/gomod/github.com/digitalocean/godo/com_github_digitalocean_godo-v1.108.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/digitalocean/godo/com_github_digitalocean_godo-v1.108.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/digitalocean/godo/com_github_digitalocean_godo-v1.108.0.zip",
        ],
    )
    go_repository(
        name = "com_github_distribution_reference",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/distribution/reference",
        sha256 = "d812d0281581beb04facbd0ca03bc529ae7de484f959ade09765c1af532e1b7c",
        strip_prefix = "github.com/distribution/reference@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/distribution/reference/com_github_distribution_reference-v0.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/distribution/reference/com_github_distribution_reference-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/distribution/reference/com_github_distribution_reference-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/distribution/reference/com_github_distribution_reference-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_djarvur_go_err113",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Djarvur/go-err113",
        sum = "h1:sHglBQTwgx+rWPdisA5ynNEsoARbiCBOyGcJM4/OzsM=",
        version = "v0.0.0-20210108212216-aea10b59be24",
    )
    go_repository(
        name = "com_github_dnaeon_go_vcr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dnaeon/go-vcr",
        sum = "h1:zHCHvJYTMh1N7xnV7zf1m1GPBF9Ad0Jk/whtQ1663qI=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_docker_docker",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/docker",
        sha256 = "943382432be5c45b64f779becb4d2c76a67727452b38a17a197a3c5d939f9cdc",
        strip_prefix = "github.com/docker/docker@v25.0.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/docker/docker/com_github_docker_docker-v25.0.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/docker/docker/com_github_docker_docker-v25.0.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/docker/docker/com_github_docker_docker-v25.0.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/docker/docker/com_github_docker_docker-v25.0.0+incompatible.zip",
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
        sum = "h1:3uh0PgVws3nIA0Q+MwDC8yjEPf9zjRfZZWXZYDct3Tw=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_docopt_docopt_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docopt/docopt-go",
        sum = "h1:bWDMxwH3px2JBh6AyO7hdCn/PkvCZXii8TGj7sbtEbQ=",
        version = "v0.0.0-20180111231733-ee0de3bc6815",
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
        sum = "h1:VSnTsYCnlFHaM2/igO1h6X3HA71jcobQuxemgkq4zYo=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_eapache_go_resiliency",
        build_file_proto_mode = "disable",
        importpath = "github.com/eapache/go-resiliency",
        sum = "h1:n3NRTnBn5N0Cbi/IeOHuQn9s2UwVUH7Ga0ZWcP+9JTA=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_eapache_go_xerial_snappy",
        build_file_proto_mode = "disable",
        importpath = "github.com/eapache/go-xerial-snappy",
        sum = "h1:Oy0F4ALJ04o5Qqpdz8XLIpNA3WM/iSIXqxtqo7UGVws=",
        version = "v0.0.0-20230731223053-c322873962e3",
    )
    go_repository(
        name = "com_github_eapache_queue",
        build_file_proto_mode = "disable",
        importpath = "github.com/eapache/queue",
        sum = "h1:YOEu7KNc61ntiQlcEeUIoDTJ2o8mQznoNvUhiigpIqc=",
        version = "v1.1.0",
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
        sum = "h1:clC1lXBpe2kTj2VHdaIu9ajZQe4kcEY9j0NsnDDBZ3o=",
        version = "v0.0.0-20171010120322-cdade1c07385",
    )
    go_repository(
        name = "com_github_elastic_gosigar",
        build_file_proto_mode = "disable",
        importpath = "github.com/elastic/gosigar",
        sum = "h1:ehdJWCzrtTHhYDmUAO6Zpu+uez4UB/dhH0oJSQ/o1Pk=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_github_elazarl_go_bindata_assetfs",
        build_file_proto_mode = "disable",
        importpath = "github.com/elazarl/go-bindata-assetfs",
        sum = "h1:G/bYguwHIzWq9ZoyUQqrjTmJbbYn3j3CKKpKinvZLFk=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_elazarl_goproxy",
        build_file_proto_mode = "disable",
        importpath = "github.com/elazarl/goproxy",
        sum = "h1:yUdfgN0XgIJw7foRItutHYUIhlcKzcSf5vDpdhQAKTc=",
        version = "v0.0.0-20180725130230-947c36da3153",
    )
    go_repository(
        name = "com_github_emicklei_go_restful",
        build_file_proto_mode = "disable",
        importpath = "github.com/emicklei/go-restful",
        sum = "h1:H2pdYOb3KQ1/YsqVWoWNLQO+fusocsw354rqGTZtAgw=",
        version = "v0.0.0-20170410110728-ff4f55a20633",
    )

    go_repository(
        name = "com_github_emicklei_go_restful_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/emicklei/go-restful/v3",
        sha256 = "fc71c649398fa5d28ac7948d143bc8ac4803c01d24f852b9d50e87724ac8efc8",
        strip_prefix = "github.com/emicklei/go-restful/v3@v3.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/emicklei/go-restful/v3/com_github_emicklei_go_restful_v3-v3.11.0.zip",
            "http://ats.apps.svc/gomod/github.com/emicklei/go-restful/v3/com_github_emicklei_go_restful_v3-v3.11.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/emicklei/go-restful/v3/com_github_emicklei_go_restful_v3-v3.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/emicklei/go-restful/v3/com_github_emicklei_go_restful_v3-v3.11.0.zip",
        ],
    )
    go_repository(
        name = "com_github_emirpasic_gods",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/emirpasic/gods",
        sum = "h1:FXtiHYKDGKCW2KzwZKx0iC0PQmdlorYgdFG9jPXJ1Bc=",
        version = "v1.18.1",
    )
    go_repository(
        name = "com_github_envoyproxy_go_control_plane",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/envoyproxy/go-control-plane",
        sum = "h1:wSUXTlLfiAQRWs2F+p+EKOY9rUyis1MyGqJ2DIk5HpM=",
        version = "v0.11.1",
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sum = "h1:QkIBuU5k+x7/QXPvPPnWXWlCdaBFApVqftFV6k087DA=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_esimonov_ifshort",
        build_file_proto_mode = "disable",
        importpath = "github.com/esimonov/ifshort",
        sum = "h1:6SID4yGWfRae/M7hkVDVVyppy8q/v9OuxNdmjLQStBA=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_etcd_io_bbolt",
        build_file_proto_mode = "disable",
        importpath = "github.com/etcd-io/bbolt",
        sum = "h1:gSJmxrs37LgTqR/oyJBWok6k6SvXEUerFTbltIhXkBM=",
        version = "v1.3.3",
    )

    go_repository(
        name = "com_github_etcd_io_gofail",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/etcd-io/gofail",
        sum = "h1:Y2I0lxOttdUKz+hNaIdG3FtjuQrTmwXun1opRV65IZc=",
        version = "v0.0.0-20190801230047-ad7f989257ca",
    )
    go_repository(
        name = "com_github_ettle_strcase",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ettle/strcase",
        sum = "h1:htFueZyVeE1XNnMEfbqp5r67qAN/4r6ya1ysq8Q+Zcw=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_evanphx_json_patch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/evanphx/json-patch",
        sum = "h1:4onqiflcdA9EOZ4RxV643DvftH5pOlLGNtQ5lPWQu84=",
        version = "v4.12.0+incompatible",
    )
    go_repository(
        name = "com_github_facebookgo_clock",
        build_file_proto_mode = "disable",
        importpath = "github.com/facebookgo/clock",
        sum = "h1:yDWHCSQ40h88yih2JAcL6Ls/kVkSE8GFACTGVnMPruw=",
        version = "v0.0.0-20150410010913-600d898af40a",
    )

    go_repository(
        name = "com_github_facette_natsort",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/facette/natsort",
        sha256 = "08cd11112374bf6bf945345bfdede1141a0bb973706291016facc0508eca3ae7",
        strip_prefix = "github.com/facette/natsort@v0.0.0-20181210072756-2cd4dd1e2dcb",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/facette/natsort/com_github_facette_natsort-v0.0.0-20181210072756-2cd4dd1e2dcb.zip",
            "http://ats.apps.svc/gomod/github.com/facette/natsort/com_github_facette_natsort-v0.0.0-20181210072756-2cd4dd1e2dcb.zip",
            "https://cache.hawkingrei.com/gomod/github.com/facette/natsort/com_github_facette_natsort-v0.0.0-20181210072756-2cd4dd1e2dcb.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/facette/natsort/com_github_facette_natsort-v0.0.0-20181210072756-2cd4dd1e2dcb.zip",
        ],
    )
    go_repository(
        name = "com_github_fasthttp_contrib_websocket",
        build_file_proto_mode = "disable",
        importpath = "github.com/fasthttp-contrib/websocket",
        sum = "h1:DddqAaWDpywytcG8w/qoQ5sAN8X12d3Z3koB0C3Rxsc=",
        version = "v0.0.0-20160511215533-1f3b11f56072",
    )
    go_repository(
        name = "com_github_fatanugraha_noloopclosure",
        build_file_proto_mode = "disable",
        importpath = "github.com/fatanugraha/noloopclosure",
        sum = "h1:AhepjAikNpk50qTZoipHZqeZtnyKT/C2Tk5dGn7nC+A=",
        version = "v0.1.1",
    )

    go_repository(
        name = "com_github_fatih_color",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatih/color",
        sum = "h1:8LOYc1KYPPmyKMuN8QV2DNRWNbLo6LZ0iLs8+mlH53w=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_github_fatih_structs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatih/structs",
        sum = "h1:Q7juDM0QtcnhCpeyLGQKyg4TOIghuNXrkL32pHAUMxo=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_fatih_structtag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatih/structtag",
        sum = "h1:/OdNE99OxoI/PqaW/SuSK9uxxT3f/tcSZgon/ssNSx4=",
        version = "v1.2.0",
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
        sum = "h1:lvB5Jl89CsZtGIWuTcDM1E/vkVs49/Ml7JJe07l8SPQ=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_firefart_nonamedreturns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/firefart/nonamedreturns",
        sum = "h1:abzI1p7mAEPYuR4A+VLKn4eNDOycjYo2phmY9sfv40Y=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_flosch_pongo2",
        build_file_proto_mode = "disable",
        importpath = "github.com/flosch/pongo2",
        sum = "h1:GY1+t5Dr9OKADM64SYnQjw/w99HMYvQ0A8/JoUkxVmc=",
        version = "v0.0.0-20190707114632-bbf5a6c351f4",
    )

    go_repository(
        name = "com_github_flosch_pongo2_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/flosch/pongo2/v4",
        sha256 = "88e92416c43e05ab51f36bef211fcd03bb25428e2d2bebeed8a1877b8ad43281",
        strip_prefix = "github.com/flosch/pongo2/v4@v4.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/flosch/pongo2/v4/com_github_flosch_pongo2_v4-v4.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/flosch/pongo2/v4/com_github_flosch_pongo2_v4-v4.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/flosch/pongo2/v4/com_github_flosch_pongo2_v4-v4.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/flosch/pongo2/v4/com_github_flosch_pongo2_v4-v4.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_fogleman_gg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fogleman/gg",
        sum = "h1:WXb3TSNmHp2vHoCroCIB1foO/yQ36swABL8aOVeDpgg=",
        version = "v1.2.1-0.20190220221249-0403632d5b90",
    )
    go_repository(
        name = "com_github_form3tech_oss_jwt_go",
        build_file_proto_mode = "disable",
        importpath = "github.com/form3tech-oss/jwt-go",
        sum = "h1:7ZaBxOI7TMoYBfyA3cQHErNNyAWIKUMIwqxEtgHOs5c=",
        version = "v3.2.3+incompatible",
    )
    go_repository(
        name = "com_github_fortytw2_leaktest",
        build_file_proto_mode = "disable",
        importpath = "github.com/fortytw2/leaktest",
        sum = "h1:u8491cBMTQ8ft8aeV+adlcytMZylmA5nnwwkRZjI8vw=",
        version = "v1.3.0",
    )

    go_repository(
        name = "com_github_frankban_quicktest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/frankban/quicktest",
        sum = "h1:8sXhOn0uLys67V8EsXLc6eszDs8VXWxL3iRvebPhedY=",
        version = "v1.11.3",
    )
    go_repository(
        name = "com_github_fsnotify_fsnotify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fsnotify/fsnotify",
        sum = "h1:jRbGcIw6P2Meqdwuo0H1p6JVLbL5DHKAKlYndzMwVZI=",
        version = "v1.5.4",
    )
    go_repository(
        name = "com_github_fsouza_fake_gcs_server",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fsouza/fake-gcs-server",
        sum = "h1:XyaGOlqo+R5sjT03x2ymk0xepaQlgwhRLTT2IopW0zA=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_github_fzipp_gocyclo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fzipp/gocyclo",
        sum = "h1:lsblElZG7d3ALtGMx9fmxeTKZaLLpU8mET09yN4BBLo=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_gaijinentertainment_go_exhaustruct_v2",
        build_file_proto_mode = "disable",
        importpath = "github.com/GaijinEntertainment/go-exhaustruct/v2",
        sum = "h1:+r1rSv4gvYn0wmRjC8X7IAzX8QezqtFV9m0MUHFJgts=",
        version = "v2.3.0",
    )

    go_repository(
        name = "com_github_gaijinentertainment_go_exhaustruct_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/GaijinEntertainment/go-exhaustruct/v3",
        sha256 = "59e6345f8758331dd5b059fa1a9f7f54a03206cd096f5cd239e7a1eb54b99c07",
        strip_prefix = "github.com/GaijinEntertainment/go-exhaustruct/v3@v3.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/GaijinEntertainment/go-exhaustruct/v3/com_github_gaijinentertainment_go_exhaustruct_v3-v3.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/GaijinEntertainment/go-exhaustruct/v3/com_github_gaijinentertainment_go_exhaustruct_v3-v3.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/GaijinEntertainment/go-exhaustruct/v3/com_github_gaijinentertainment_go_exhaustruct_v3-v3.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/GaijinEntertainment/go-exhaustruct/v3/com_github_gaijinentertainment_go_exhaustruct_v3-v3.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gavv_httpexpect",
        build_file_proto_mode = "disable",
        importpath = "github.com/gavv/httpexpect",
        sum = "h1:1X9kcRshkSKEjNJJxX9Y9mQ5BRfbxU5kORdjhlA1yX8=",
        version = "v2.0.0+incompatible",
    )
    go_repository(
        name = "com_github_getkin_kin_openapi",
        build_file_proto_mode = "disable",
        importpath = "github.com/getkin/kin-openapi",
        sum = "h1:j77zg3Ec+k+r+GA3d8hBoXpAc6KX9TbBPrwQGBIy2sY=",
        version = "v0.76.0",
    )
    go_repository(
        name = "com_github_getsentry_raven_go",
        build_file_proto_mode = "disable",
        importpath = "github.com/getsentry/raven-go",
        sum = "h1:no+xWJRb5ZI7eE8TWgIq1jLulQiIoLG0IfYxv5JYMGs=",
        version = "v0.2.0",
    )

    go_repository(
        name = "com_github_getsentry_sentry_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/getsentry/sentry-go",
        sha256 = "679a02061b0d653713146278ee120a5fa1fefcf59a03419990673c17cbfd6e6e",
        strip_prefix = "github.com/getsentry/sentry-go@v0.27.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/getsentry/sentry-go/com_github_getsentry_sentry_go-v0.27.0.zip",
            "http://ats.apps.svc/gomod/github.com/getsentry/sentry-go/com_github_getsentry_sentry_go-v0.27.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/getsentry/sentry-go/com_github_getsentry_sentry_go-v0.27.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/getsentry/sentry-go/com_github_getsentry_sentry_go-v0.27.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ghemawat_stream",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ghemawat/stream",
        sum = "h1:r5GgOLGbza2wVHRzK7aAj6lWZjfbAwiu/RDCVOKjRyM=",
        version = "v0.0.0-20171120220530-696b145b53b9",
    )
    go_repository(
        name = "com_github_ghodss_yaml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ghodss/yaml",
        sum = "h1:wQHKEahhL6wmXdzwWG11gIVCkOv05bNOh+Rxn0yngAk=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_ghostiam_protogetter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ghostiam/protogetter",
        sha256 = "55355f5017049085fcef1b7d9fbf9f6b774485dc2e06d5554cf6e2a564a04ada",
        strip_prefix = "github.com/ghostiam/protogetter@v0.3.8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ghostiam/protogetter/com_github_ghostiam_protogetter-v0.3.8.zip",
            "http://ats.apps.svc/gomod/github.com/ghostiam/protogetter/com_github_ghostiam_protogetter-v0.3.8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ghostiam/protogetter/com_github_ghostiam_protogetter-v0.3.8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ghostiam/protogetter/com_github_ghostiam_protogetter-v0.3.8.zip",
        ],
    )
    go_repository(
        name = "com_github_gin_contrib_sse",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gin-contrib/sse",
        sum = "h1:t8FVkw33L+wilf2QiWkw0UV77qRpcH/JHPKGpKa2E8g=",
        version = "v0.0.0-20190301062529-5545eab6dad3",
    )
    go_repository(
        name = "com_github_gin_gonic_gin",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gin-gonic/gin",
        sum = "h1:3tMoCCfM7ppqsR0ptz/wi1impNpT7/9wQtMZ8lr1mCQ=",
        version = "v1.4.0",
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
        build_file_proto_mode = "disable",
        importpath = "github.com/go-check/check",
        sum = "h1:0gkP6mzaMqkmpcJYCFOLkIBwI7xFExG03bbkOkCvUPI=",
        version = "v0.0.0-20180628173108-788fd7840127",
    )

    go_repository(
        name = "com_github_go_critic_go_critic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-critic/go-critic",
        sum = "h1:fDaR/5GWURljXwF8Eh31T2GZNz9X4jeboS912mWF8Uo=",
        version = "v0.6.5",
    )
    go_repository(
        name = "com_github_go_errors_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-errors/errors",
        sum = "h1:LUHzmkK3GUKUrL/1gfBUxAHzcev3apQlezX/+O7ma6w=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_go_fonts_dejavu",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-fonts/dejavu",
        sha256 = "c2094ce49cfc24b7b7a041e54d924e311322b73a8e56db28ff179fcd403b4111",
        strip_prefix = "github.com/go-fonts/dejavu@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-fonts/dejavu/com_github_go_fonts_dejavu-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-fonts/dejavu/com_github_go_fonts_dejavu-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-fonts/dejavu/com_github_go_fonts_dejavu-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-fonts/dejavu/com_github_go_fonts_dejavu-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_fonts_latin_modern",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-fonts/latin-modern",
        sha256 = "037085a80ad108287e772d064d64bb72deb62514de84ef610506bc079f330ec0",
        strip_prefix = "github.com/go-fonts/latin-modern@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-fonts/latin-modern/com_github_go_fonts_latin_modern-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-fonts/latin-modern/com_github_go_fonts_latin_modern-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-fonts/latin-modern/com_github_go_fonts_latin_modern-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-fonts/latin-modern/com_github_go_fonts_latin_modern-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_fonts_liberation",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-fonts/liberation",
        sha256 = "bd7561251c221fe0fd8cd4c361b062a5796f6f3a1096968b8fecdd61eb82d8fe",
        strip_prefix = "github.com/go-fonts/liberation@v0.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-fonts/liberation/com_github_go_fonts_liberation-v0.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-fonts/liberation/com_github_go_fonts_liberation-v0.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-fonts/liberation/com_github_go_fonts_liberation-v0.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-fonts/liberation/com_github_go_fonts_liberation-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_fonts_stix",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-fonts/stix",
        sha256 = "51ea5a38b9fda7854af60f280dbd8b40a3e5b5a48eb00d3f8d4e43de3f514ecf",
        strip_prefix = "github.com/go-fonts/stix@v0.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-fonts/stix/com_github_go_fonts_stix-v0.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-fonts/stix/com_github_go_fonts_stix-v0.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-fonts/stix/com_github_go_fonts_stix-v0.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-fonts/stix/com_github_go_fonts_stix-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_gl_glfw",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-gl/glfw",
        sum = "h1:QbL/5oDUmRBzO9/Z7Seo6zf912W/a6Sr4Eu0G/3Jho0=",
        version = "v0.0.0-20190409004039-e6da0acd62b1",
    )
    go_repository(
        name = "com_github_go_gl_glfw_v3_3_glfw",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-gl/glfw/v3.3/glfw",
        sum = "h1:WtGNWLvXpe6ZudgnXrq0barxBImvnnJoMEhXAzcbM0I=",
        version = "v0.0.0-20200222043503-6f7a984d4dc4",
    )
    go_repository(
        name = "com_github_go_ini_ini",
        build_file_proto_mode = "disable",
        importpath = "github.com/go-ini/ini",
        sum = "h1:Mujh4R/dH6YL8bxuISne3xX2+qcQ9p0IxKAP6ExWoUo=",
        version = "v1.25.4",
    )

    go_repository(
        name = "com_github_go_kit_kit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-kit/kit",
        sum = "h1:wDJmvq38kDhkVxi50ni9ykkdUr1PKgqKOoi01fa0Mdk=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_github_go_kit_log",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-kit/log",
        sum = "h1:MRVx0/zhvdseW+Gza6N9rVzU/IVzaeE1SFI4raAhmBU=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_go_latex_latex",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-latex/latex",
        sha256 = "c58be686b31679ad0a51a5d70e60df92fb4bb50a16727caa58b4a67b33f16509",
        strip_prefix = "github.com/go-latex/latex@v0.0.0-20210823091927-c0d11ff05a81",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-latex/latex/com_github_go_latex_latex-v0.0.0-20210823091927-c0d11ff05a81.zip",
            "http://ats.apps.svc/gomod/github.com/go-latex/latex/com_github_go_latex_latex-v0.0.0-20210823091927-c0d11ff05a81.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-latex/latex/com_github_go_latex_latex-v0.0.0-20210823091927-c0d11ff05a81.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-latex/latex/com_github_go_latex_latex-v0.0.0-20210823091927-c0d11ff05a81.zip",
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
        sum = "h1:otpy5pqBCBZ1ng9RQ0dPu4PN7ba75Y/aA+UpowDyNVA=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_github_go_logr_logr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logr/logr",
        sum = "h1:QK40JKJyMdUDz+h+xvCsru/bJhvG0UxvePV0ufL/AcE=",
        version = "v1.2.0",
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
        sum = "h1:xveKWz2iaueeTaUgdetzel+U7exyigDYBryyVfV/rZk=",
        version = "v0.0.0-20170121215854-22fa46961aab",
    )
    go_repository(
        name = "com_github_go_ole_go_ole",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-ole/go-ole",
        sum = "h1:Dt6ye7+vXGIKZ7Xtk4s6/xVdGDQynvom7xCFEdWr6uE=",
        version = "v1.3.0",
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
        sha256 = "fd36596bb434cedffc79748a261193cf1938c19b05afa9e56e65f8b643561fee",
        strip_prefix = "github.com/go-openapi/errors@v0.21.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/errors/com_github_go_openapi_errors-v0.21.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/errors/com_github_go_openapi_errors-v0.21.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/errors/com_github_go_openapi_errors-v0.21.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/errors/com_github_go_openapi_errors-v0.21.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_jsonpointer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonpointer",
        sum = "h1:gZr+CIYByUqjcgeLXnQu2gHYQC9o73G2XUeOFYEICuY=",
        version = "v0.19.5",
    )
    go_repository(
        name = "com_github_go_openapi_jsonreference",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonreference",
        sum = "h1:5cxNfTy0UVC3X8JL5ymxzyoUZmo8iZb+jeTWn7tUa8o=",
        version = "v0.19.3",
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
        sha256 = "37f512d6ac447bc026276a87eeb89d3c0ec243740c69e79743f8d9761d29aafe",
        strip_prefix = "github.com/go-openapi/strfmt@v0.22.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-openapi/strfmt/com_github_go_openapi_strfmt-v0.22.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-openapi/strfmt/com_github_go_openapi_strfmt-v0.22.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-openapi/strfmt/com_github_go_openapi_strfmt-v0.22.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-openapi/strfmt/com_github_go_openapi_strfmt-v0.22.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_swag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/swag",
        sum = "h1:lTz6Ys4CmqqCQmZPBlbQENR1/GucA2bzYTE12Pw4tFY=",
        version = "v0.19.5",
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
        name = "com_github_go_pdf_fpdf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-pdf/fpdf",
        sha256 = "03a6909fc346ac972b008b77585ac3954d76b416c33b4b64dc22c5f35f0e1edb",
        strip_prefix = "github.com/go-pdf/fpdf@v0.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-pdf/fpdf/com_github_go_pdf_fpdf-v0.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-pdf/fpdf/com_github_go_pdf_fpdf-v0.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-pdf/fpdf/com_github_go_pdf_fpdf-v0.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-pdf/fpdf/com_github_go_pdf_fpdf-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_playground_locales",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/locales",
        sha256 = "e103ae2c635cde62d2b75ff021be20443ab8d227aebfed5f043846575ea1fa43",
        strip_prefix = "github.com/go-playground/locales@v0.14.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-playground/locales/com_github_go_playground_locales-v0.14.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-playground/locales/com_github_go_playground_locales-v0.14.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-playground/locales/com_github_go_playground_locales-v0.14.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-playground/locales/com_github_go_playground_locales-v0.14.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_playground_universal_translator",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/universal-translator",
        sha256 = "15f3241347dfcfe7d668595727629bcf54ff028ebc4b7c955b9c2bdeb253a110",
        strip_prefix = "github.com/go-playground/universal-translator@v0.18.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-playground/universal-translator/com_github_go_playground_universal_translator-v0.18.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-playground/universal-translator/com_github_go_playground_universal_translator-v0.18.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-playground/universal-translator/com_github_go_playground_universal_translator-v0.18.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-playground/universal-translator/com_github_go_playground_universal_translator-v0.18.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_playground_validator_v10",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/validator/v10",
        sha256 = "d10a0eb03b84570af1f1278f8df82cee6c5dcddfe2e23d6f2c5bc018a2d3929e",
        strip_prefix = "github.com/go-playground/validator/v10@v10.11.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-playground/validator/v10/com_github_go_playground_validator_v10-v10.11.1.zip",
            "http://ats.apps.svc/gomod/github.com/go-playground/validator/v10/com_github_go_playground_validator_v10-v10.11.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-playground/validator/v10/com_github_go_playground_validator_v10-v10.11.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-playground/validator/v10/com_github_go_playground_validator_v10-v10.11.1.zip",
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
        sum = "h1:BCTh4TKNUYmOmMUcQ3IipzF5prigylS7XXjEkfCHuOE=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_go_stack_stack",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-stack/stack",
        sum = "h1:5SgMzNM5HxrEjV0ww2lTmX6E2Izsfxas4+YHWRs3Lsk=",
        version = "v1.8.0",
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
        sum = "h1:JojxlmI6STnFVG9yOImLeGREv8W2ocNUM+iOhR6jE7g=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astcopy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astcopy",
        sum = "h1:YnWf5Rnh1hUudj11kei53kI57quN/VH6Hp1n+erozn0=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_go_toolsmith_astequal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astequal",
        sum = "h1:+LVdyRatFS+XO78SGV4I3TCEA0AC7fKEGma+fH+674o=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_go_toolsmith_astfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astfmt",
        sum = "h1:A0vDDXt+vsvLEdbMFJAUBI/uTbRw1ffOPnxsILnFL6k=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astp",
        sum = "h1:alXE75TXgcmupDsMK1fRAy0YUzLzqPVvBKoyWV+KPXg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_strparse",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/strparse",
        sum = "h1:Vcw78DnpCAKlM20kSbAyO4mPfJn/lyYA4BJUDxe2Jb4=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_typep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/typep",
        sum = "h1:8xdsa1+FSIH/RhEkgnD1j2CJOy5mNllW1Q9tRiYwvlk=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_go_viper_mapstructure_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-viper/mapstructure/v2",
        sha256 = "e1ea7643504db811d7af92e61b919b9a56c3a15a6ad56ffd8327625bf6e5f3f8",
        strip_prefix = "github.com/go-viper/mapstructure/v2@v2.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/go-viper/mapstructure/v2/com_github_go_viper_mapstructure_v2-v2.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/go-viper/mapstructure/v2/com_github_go_viper_mapstructure_v2-v2.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/go-viper/mapstructure/v2/com_github_go_viper_mapstructure_v2-v2.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/go-viper/mapstructure/v2/com_github_go_viper_mapstructure_v2-v2.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_xmlfmt_xmlfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-xmlfmt/xmlfmt",
        sum = "h1:khEcpUM4yFcxg4/FHQWkvVRmgijNXRfzkIDHh23ggEo=",
        version = "v0.0.0-20191208150333-d5b6f63a941b",
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
        sum = "h1:A4xDbljILXROh+kObIiy5kIaPYD8e96x1tgBhUI5J+Y=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_gobwas_httphead",
        build_file_proto_mode = "disable",
        importpath = "github.com/gobwas/httphead",
        sum = "h1:s+21KNqlpePfkah2I+gwHF8xmJWRjooY+5248k6m4A0=",
        version = "v0.0.0-20180130184737-2c6c146eadee",
    )
    go_repository(
        name = "com_github_gobwas_pool",
        build_file_proto_mode = "disable",
        importpath = "github.com/gobwas/pool",
        sum = "h1:QEmUOlnSjWtnpRGHF3SauEiOsy82Cup83Vf2LcMlnc8=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_gobwas_ws",
        build_file_proto_mode = "disable",
        importpath = "github.com/gobwas/ws",
        sum = "h1:CoAavW/wd/kulfZmSIBt6p24n4j7tHgNVCjsfHVNUbo=",
        version = "v1.0.2",
    )

    go_repository(
        name = "com_github_goccy_go_json",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/goccy/go-json",
        sum = "h1:CrxCmQqYDkv1z7lO7Wbh2HN93uovUHgrECaO5ZrCXAU=",
        version = "v0.10.2",
    )
    go_repository(
        name = "com_github_goccy_go_reflect",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/goccy/go-reflect",
        sha256 = "d5d5b55be60c40d1ecfbd13a7e89c3fb5363e8b7cd07e2827f7e987944c41458",
        strip_prefix = "github.com/goccy/go-reflect@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/goccy/go-reflect/com_github_goccy_go_reflect-v1.2.0.zip",
            "http://ats.apps.svc/gomod/github.com/goccy/go-reflect/com_github_goccy_go_reflect-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/goccy/go-reflect/com_github_goccy_go_reflect-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/goccy/go-reflect/com_github_goccy_go_reflect-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_godbus_dbus_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/godbus/dbus/v5",
        sum = "h1:9349emZab16e7zQvpmsbtjc18ykshndd8y2PG3sgJbA=",
        version = "v5.0.4",
    )
    go_repository(
        name = "com_github_gofrs_flock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gofrs/flock",
        sum = "h1:+gYjHKf32LDeiEEFhQaotPbLuUXjY5ZqxKgXy7n59aw=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_github_gogo_googleapis",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/googleapis",
        sum = "h1:dR8+Q0uO5S2ZBcs2IH6VBKYwSxPo2vYCYq0ot0mu7xA=",
        version = "v0.0.0-20180223154316-0cd9801be74a",
    )
    go_repository(
        name = "com_github_gogo_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/protobuf",
        sum = "h1:Ov1cvc58UF3b5XjBnZv7+opcTcQFZebYjWzi34vdm4Q=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_github_gogo_status",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/status",
        sum = "h1:+eIkrewn5q6b30y+g/BJINVVdi2xH7je5MPJ3ZPK3JA=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_goji_httpauth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/goji/httpauth",
        sum = "h1:lBXNCxVENCipq4D1Is42JVOP4eQjlB8TQ6H69Yx5J9Q=",
        version = "v0.0.0-20160601135302-2da839ab0f4d",
    )
    go_repository(
        name = "com_github_golang_freetype",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/freetype",
        sum = "h1:DACJavvAHhabrF08vX0COfcOBJRhZ8lUbR+ZWIs0Y5g=",
        version = "v0.0.0-20170609003504-e2365dfdc4a0",
    )
    go_repository(
        name = "com_github_golang_glog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/glog",
        sum = "h1:DVjP2PbBOzHyzA+dn3WhHIq4NdVu3Q+pvivFICf/7fo=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_golang_groupcache",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/groupcache",
        sum = "h1:oI5xCqsCo564l8iNU+DwB5epxmsaqB+rhGL0m5jtYqE=",
        version = "v0.0.0-20210331224755-41bb18bfe9da",
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
        sum = "h1:rcc4lwaZgFMCZ5jxF9ABolDcIHdBytAFgqFPbSJQAYs=",
        version = "v4.4.2",
    )
    go_repository(
        name = "com_github_golang_jwt_jwt_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-jwt/jwt/v5",
        sum = "h1:OuVbFODueb089Lh128TAcimifWaLhJwVflnrgM17wHk=",
        version = "v5.2.1",
    )
    go_repository(
        name = "com_github_golang_mock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/mock",
        sum = "h1:YojYx61/OLFsiv6Rw1Z96LpldJIy31o+UHmwAUMJ6/U=",
        version = "v1.7.0-rc.1",
    )
    go_repository(
        name = "com_github_golang_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/protobuf",
        sum = "h1:i7eJL8qZTpSEXOPTxNKhASYpMn+8e5Q6AdndVa1dWek=",
        version = "v1.5.4",
    )
    go_repository(
        name = "com_github_golang_snappy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/snappy",
        sum = "h1:yAGX7huGHXlcLOEtBnF4w7FQwA26wojNCwOYAEhLjQM=",
        version = "v0.0.4",
    )
    go_repository(
        name = "com_github_golangci_check",
        build_file_proto_mode = "disable",
        importpath = "github.com/golangci/check",
        sum = "h1:23T5iq8rbUYlhpt5DB4XJkc6BU31uODLD1o1gKvZmD0=",
        version = "v0.0.0-20180506172741-cfe4005ccda2",
    )

    go_repository(
        name = "com_github_golangci_dupl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/dupl",
        sum = "h1:w8hkcTqaFpzKqonE9uMCefW1WDie15eSP/4MssdenaM=",
        version = "v0.0.0-20180902072040-3e9179ac440a",
    )
    go_repository(
        name = "com_github_golangci_go_misc",
        build_file_proto_mode = "disable",
        importpath = "github.com/golangci/go-misc",
        sum = "h1:6RGUuS7EGotKx6J5HIP8ZtyMdiDscjMLfRBSPuzVVeo=",
        version = "v0.0.0-20220329215616-d24fe342adfe",
    )

    go_repository(
        name = "com_github_golangci_gofmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/gofmt",
        sum = "h1:amWTbTGqOZ71ruzrdA+Nx5WA3tV1N0goTspwmKCQvBY=",
        version = "v0.0.0-20220901101216-f2edd75033f2",
    )
    go_repository(
        name = "com_github_golangci_golangci_lint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/golangci-lint",
        sum = "h1:C829clMcZXEORakZlwpk7M4iDw2XiwxxKaG504SZ9zY=",
        version = "v1.50.1",
    )
    go_repository(
        name = "com_github_golangci_gosec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/gosec",
        sum = "h1:Bi7BYmZVg4C+mKGi8LeohcP2GGUl2XJD4xCkJoZSaYc=",
        version = "v0.0.0-20180901114220-8afd9cbb6cfb",
    )
    go_repository(
        name = "com_github_golangci_lint_1",
        build_file_proto_mode = "disable",
        importpath = "github.com/golangci/lint-1",
        sum = "h1:MfyDlzVjl1hoaPzPD4Gpb/QgoRfSBR0jdhwGyAWwMSA=",
        version = "v0.0.0-20191013205115-297bf364a8e0",
    )
    go_repository(
        name = "com_github_golangci_maligned",
        build_file_proto_mode = "disable",
        importpath = "github.com/golangci/maligned",
        sum = "h1:kNY3/svz5T29MYHubXix4aDDuE3RWHkPvopM/EDv/MA=",
        version = "v0.0.0-20180506175553-b1d89398deca",
    )

    go_repository(
        name = "com_github_golangci_misspell",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/misspell",
        sum = "h1:pLzmVdl3VxTOncgzHcvLOKirdvcx/TydsClUQXTehjo=",
        version = "v0.3.5",
    )
    go_repository(
        name = "com_github_golangci_modinfo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/modinfo",
        sha256 = "52b2fa1dfa55b4a08335599825ede662b1f4c8c78a0df0c9500fb204f0502c3c",
        strip_prefix = "github.com/golangci/modinfo@v0.3.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/modinfo/com_github_golangci_modinfo-v0.3.4.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/modinfo/com_github_golangci_modinfo-v0.3.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/modinfo/com_github_golangci_modinfo-v0.3.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/modinfo/com_github_golangci_modinfo-v0.3.4.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_plugin_module_register",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/plugin-module-register",
        sha256 = "83c1ed7753d7fde729ece4305a8b69baec4c882dea64594572dc73e423cf5802",
        strip_prefix = "github.com/golangci/plugin-module-register@v0.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/golangci/plugin-module-register/com_github_golangci_plugin_module_register-v0.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/golangci/plugin-module-register/com_github_golangci_plugin_module_register-v0.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/golangci/plugin-module-register/com_github_golangci_plugin_module_register-v0.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/golangci/plugin-module-register/com_github_golangci_plugin_module_register-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_prealloc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/prealloc",
        sum = "h1:leSNB7iYzLYSSx3J/s5sVf4Drkc68W2wm4Ixh/mr0us=",
        version = "v0.0.0-20180630174525-215b22d4de21",
    )
    go_repository(
        name = "com_github_golangci_revgrep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/revgrep",
        sum = "h1:DIPQnGy2Gv2FSA4B/hh8Q7xx3B7AIDk3DAMeHclH1vQ=",
        version = "v0.0.0-20220804021717-745bb2f7c2e6",
    )
    go_repository(
        name = "com_github_golangci_unconvert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/unconvert",
        sum = "h1:zwtduBRr5SSWhqsYNgcuWO2kFlpdOZbP0+yRjmvPGys=",
        version = "v0.0.0-20180507085042-28b1c447d1f4",
    )
    go_repository(
        name = "com_github_gomodule_redigo",
        build_file_proto_mode = "disable",
        importpath = "github.com/gomodule/redigo",
        sum = "h1:y0Wmhvml7cGnzPa9nocn/fMraMH/lMDdeG+rkx4VgYY=",
        version = "v1.7.1-0.20190724094224-574c33c3df38",
    )

    go_repository(
        name = "com_github_google_btree",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/btree",
        sum = "h1:xf4v41cLI2Z6FxbKm+8Bu+m8ifhj15JuZ9sa0jZCMUU=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_google_flatbuffers",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/flatbuffers",
        sha256 = "0c0a4aab1c6029141d655bc7fdc07e22dd06f3f64ebbf7a2250b870ef7aac7ee",
        strip_prefix = "github.com/google/flatbuffers@v2.0.8+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/google/flatbuffers/com_github_google_flatbuffers-v2.0.8+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/google/flatbuffers/com_github_google_flatbuffers-v2.0.8+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/google/flatbuffers/com_github_google_flatbuffers-v2.0.8+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/google/flatbuffers/com_github_google_flatbuffers-v2.0.8+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_google_gnostic",
        build_file_proto_mode = "disable",
        importpath = "github.com/google/gnostic",
        sum = "h1:FhTMOKj2VhjpouxvWJAV1TL304uMlb9zcDqkl6cEI54=",
        version = "v0.5.7-v3refs",
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
        sum = "h1:ofyhxvXcZhMsU5ulbFiLKl/XBFqE1GSq7atu8tAmTRI=",
        version = "v0.6.0",
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
        sum = "h1:Xkwi/a1rcvNg1PPYe5vI8GbeBY/jrVuDX5ASuANWTrk=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_google_gofuzz",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/gofuzz",
        sum = "h1:Hsa8mG0dQ46ij8Sl2AYJDUv1oA9/d6Vk+3LG99Oe02g=",
        version = "v1.1.0",
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
        sum = "h1:/CP5g8u/VJHijgedC/Legn3BAbAaWPgecwXBIDzw5no=",
        version = "v2.1.0+incompatible",
    )
    go_repository(
        name = "com_github_google_martian_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/martian/v3",
        sum = "h1:IqNFLAmvJOgVlpdEBiQbDc2EwKW77amAycfTuWKdfvw=",
        version = "v3.3.2",
    )
    go_repository(
        name = "com_github_google_pprof",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/pprof",
        sum = "h1:c8EUapQFi+kjzedr4c6WqbwMdmB95+oDBWZ5XFHFYxY=",
        version = "v0.0.0-20211122183932-1daafda22083",
    )
    go_repository(
        name = "com_github_google_renameio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/renameio",
        sum = "h1:GOZbcHa3HfsPKPlmyPyN2KEohoMXOhdMbHrvbpl2QaA=",
        version = "v0.1.0",
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
        sum = "h1:1kZ/sQM3srePvKs3tXAvQzo66XfcReoqFpIpIccE7Oc=",
        version = "v0.1.4",
    )
    go_repository(
        name = "com_github_google_skylark",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/skylark",
        sum = "h1:rZdD1GeRTHD1aG+VIvhQEYXurx6Wfg4QIT5YVl2tSC8=",
        version = "v0.0.0-20181101142754-a5f7082aabed",
    )
    go_repository(
        name = "com_github_google_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/uuid",
        sum = "h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_googleapis_enterprise_certificate_proxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/enterprise-certificate-proxy",
        sum = "h1:uGy6JWR/uMIILU8wbf+OkstIrNiMjGpEIyhx8f6W7s4=",
        version = "v0.2.4",
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/gax-go/v2",
        sum = "h1:9V9PWXEsWnPpQhu/PeQIkS4eGzMlTLGgt80cUUI8Ki4=",
        version = "v2.11.0",
    )
    go_repository(
        name = "com_github_googleapis_gnostic",
        build_file_proto_mode = "disable",
        importpath = "github.com/googleapis/gnostic",
        sum = "h1:l6N3VoaVzTncYYW+9yOz2LJJammFZGBO13sqgEhpy9g=",
        version = "v0.2.0",
    )

    go_repository(
        name = "com_github_gophercloud_gophercloud",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gophercloud/gophercloud",
        sum = "h1:hQpY0g0UGsLKLDs8UJ6xpA2gNCkEdEbvxSPqLItXCpI=",
        version = "v0.0.0-20190301152420-fca40860790e",
    )
    go_repository(
        name = "com_github_gopherjs_gopherjs",
        build_file_proto_mode = "disable",
        importpath = "github.com/gopherjs/gopherjs",
        sum = "h1:EGx4pi6eqNxGaHF6qqu48+N2wcFQ5qg5FXgOdqsJ5d8=",
        version = "v0.0.0-20181017120253-0766667cb4d1",
    )

    go_repository(
        name = "com_github_gordonklaus_ineffassign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gordonklaus/ineffassign",
        sum = "h1:PVRE9d4AQKmbelZ7emNig1+NT27DUmKZn5qXxfio54U=",
        version = "v0.0.0-20210914165742-4cc7213b9bc8",
    )
    go_repository(
        name = "com_github_gorilla_css",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/css",
        sha256 = "d854362b9d723daf613b26aae0254723a4ed1bff680683c3e2a01aeb398168e5",
        strip_prefix = "github.com/gorilla/css@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/gorilla/css/com_github_gorilla_css-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/gorilla/css/com_github_gorilla_css-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/gorilla/css/com_github_gorilla_css-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/gorilla/css/com_github_gorilla_css-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_handlers",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/handlers",
        sum = "h1:9lRY6j8DEeeBT10CvO9hGW0gmky0BprnvDI5vfhUHH4=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_gorilla_mux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/mux",
        sum = "h1:i40aqfkR1h2SlN9hojwV5ZA91wcXFOvkdNIeFDP5koI=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_github_gorilla_securecookie",
        build_file_proto_mode = "disable",
        importpath = "github.com/gorilla/securecookie",
        sum = "h1:miw7JPhV+b/lAHSXz4qd/nN9jRiAFV5FwjeKyCS8BvQ=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_gorilla_sessions",
        build_file_proto_mode = "disable",
        importpath = "github.com/gorilla/sessions",
        sum = "h1:DHd3rPN5lE3Ts3D8rKkQ8x/0kqfeNmBAaiSi+o7FsgI=",
        version = "v1.2.1",
    )

    go_repository(
        name = "com_github_gorilla_websocket",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/websocket",
        sum = "h1:+/TMaTYc4QFitKJxsQ7Yye35DkWvkdLcvGKqM+x0Ufc=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_gostaticanalysis_analysisutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/analysisutil",
        sum = "h1:ZMCjoue3DtDWQ5WyU16YbjbQEQ3VuzwxALrpYd+HeKk=",
        version = "v0.7.1",
    )
    go_repository(
        name = "com_github_gostaticanalysis_comment",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/comment",
        sum = "h1:hlnx5+S2fY9Zo9ePo4AhgYsYHbM2+eAv8m/s1JiCd6Q=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_gostaticanalysis_forcetypeassert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/forcetypeassert",
        sum = "h1:6eUflI3DiGusXGK6X7cCcIgVCpZ2CiZ1Q7jl6ZxNV70=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_gostaticanalysis_nilerr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/nilerr",
        sum = "h1:ThE+hJP0fEp4zWLkWHWcRyI2Od0p7DlgYG3Uqrmrcpk=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_gostaticanalysis_testutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gostaticanalysis/testutil",
        sum = "h1:nhdCmubdmDF6VEatUNjgUZBJKWRqugoISdUv3PPQgHY=",
        version = "v0.4.0",
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
        sum = "h1:+9834+KizmvFV7pXQGSXQTsaWhq2GjuNUt0aUU0YBYw=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_prometheus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
        sum = "h1:Ovs26xHkKqVztRpIrF/92BcuyuQ/YW4NSIpoGtfXNho=",
        version = "v1.2.0",
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
        sum = "h1:gmcG1KaJ57LophUzW0Hy8NmPhnMZb4M0+kPpLofRdBo=",
        version = "v1.16.0",
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
        name = "com_github_grpc_ecosystem_grpc_opentracing",
        build_file_proto_mode = "disable",
        importpath = "github.com/grpc-ecosystem/grpc-opentracing",
        sum = "h1:MJG/KsmcqMwFAkh8mTnAwhyKoB+sTAnY4CACC110tbU=",
        version = "v0.0.0-20180507213350-8e809c8a8645",
    )

    go_repository(
        name = "com_github_guptarohit_asciigraph",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/guptarohit/asciigraph",
        sha256 = "c2b81da57a50425d313a684efd13d9741c4e9df4c3cca92dea34d562d34271a1",
        strip_prefix = "github.com/guptarohit/asciigraph@v0.5.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/guptarohit/asciigraph/com_github_guptarohit_asciigraph-v0.5.5.zip",
            "http://ats.apps.svc/gomod/github.com/guptarohit/asciigraph/com_github_guptarohit_asciigraph-v0.5.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/guptarohit/asciigraph/com_github_guptarohit_asciigraph-v0.5.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/guptarohit/asciigraph/com_github_guptarohit_asciigraph-v0.5.5.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_consul_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/consul/api",
        sum = "h1:BNQPM9ytxj6jbjjdRPioQ94T6YXriSopn0i8COv6SRA=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_hashicorp_consul_sdk",
        build_file_proto_mode = "disable",
        importpath = "github.com/hashicorp/consul/sdk",
        sum = "h1:LnuDWGNsoajlhGyHJvuWW6FVqRl8JOTPqS6CPTsYjhY=",
        version = "v0.1.1",
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
        sum = "h1:hLrqtEDnRye3+sgx6z4qVLNuviH3MR5aQ0ykNJa/UYA=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_cleanhttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-cleanhttp",
        sum = "h1:dH3aiDG9Jvb5r5+bYHsikaOUIpcM0xvgMXVoDkXMzJM=",
        version = "v0.5.1",
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
        sum = "h1:AKDB1HM5PWEA7i4nhcpwOrO2byshxBjXVn/J/3+z5/0=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_msgpack",
        build_file_proto_mode = "disable",
        importpath = "github.com/hashicorp/go-msgpack",
        sum = "h1:SFT72YqIkOcLdWJUYcriVX7hbrZpwc/f7h8aW2NUqrA=",
        version = "v0.5.4",
    )

    go_repository(
        name = "com_github_hashicorp_go_multierror",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-multierror",
        sum = "h1:H5DkEtf6CXdFp0N0Em5UCwQpXMWke8IA0+lD48awMYo=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_hashicorp_go_net",
        build_file_proto_mode = "disable",
        importpath = "github.com/hashicorp/go.net",
        sum = "h1:sNCoNyDEvN1xa+X0baata4RdcpKwcMS6DH+xwfqPgjw=",
        version = "v0.0.1",
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
        sum = "h1:Rqb66Oo1X/eSV1x66xbDccZjhJigjg0+e82kpwzSwCI=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_sockaddr",
        build_file_proto_mode = "disable",
        importpath = "github.com/hashicorp/go-sockaddr",
        sum = "h1:ztczhD1jLxIRjVejw8gFomI1BQZOe2WoVOu0SyteCQc=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_hashicorp_go_syslog",
        build_file_proto_mode = "disable",
        importpath = "github.com/hashicorp/go-syslog",
        sum = "h1:KaodqZuhUoZereWVIYmpUgZysurB1kBLX2j0MwMrUAE=",
        version = "v1.0.0",
    )

    go_repository(
        name = "com_github_hashicorp_go_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-uuid",
        sum = "h1:2gKiV6YVmrJ1i2CKKa9obLvRieoRGviZFL26PcT/Co8=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_hashicorp_go_version",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-version",
        sum = "h1:feTTfFNnjP967rlCxM/I9g701jU+RN74YKx2mOkIeek=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_hashicorp_golang_lru",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/golang-lru",
        sum = "h1:0hERBMJE1eitiLkihrMvRVBYAkpHzc/J3QdDN+dAcgU=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_github_hashicorp_hcl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/hcl",
        sum = "h1:0Anlzjpi4vEasTeNFn2mLJgTSwt0+6sfsiTG8qcWGx4=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_logutils",
        build_file_proto_mode = "disable",
        importpath = "github.com/hashicorp/logutils",
        sum = "h1:dLEQVugN8vlakKOUE3ihGLTZJRB4j+M2cdTm/ORI65Y=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_mdns",
        build_file_proto_mode = "disable",
        importpath = "github.com/hashicorp/mdns",
        sum = "h1:WhIgCr5a7AaVH6jPUwjtRuuE7/RDufnUvzIr48smyxs=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_memberlist",
        build_file_proto_mode = "disable",
        importpath = "github.com/hashicorp/memberlist",
        sum = "h1:EmmoJme1matNzb+hMpDuR/0sbJSUisxyqBGG676r31M=",
        version = "v0.1.3",
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
        sum = "h1:YZ7UKsJv+hKjqGVUUbtE3HNj79Eln2oQ75tniF6iPt0=",
        version = "v0.8.2",
    )
    go_repository(
        name = "com_github_hdrhistogram_hdrhistogram_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/HdrHistogram/hdrhistogram-go",
        sum = "h1:5IcZpTvzydCQeHzK4Ef/D5rrSqwxob0t8PQPMybUNFM=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_hetznercloud_hcloud_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hetznercloud/hcloud-go/v2",
        sha256 = "751828960948066c67e6682647ac68ed3141865d48a9e0e80c6444e0e00f55a1",
        strip_prefix = "github.com/hetznercloud/hcloud-go/v2@v2.6.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/hetznercloud/hcloud-go/v2/com_github_hetznercloud_hcloud_go_v2-v2.6.0.zip",
            "http://ats.apps.svc/gomod/github.com/hetznercloud/hcloud-go/v2/com_github_hetznercloud_hcloud_go_v2-v2.6.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/hetznercloud/hcloud-go/v2/com_github_hetznercloud_hcloud_go_v2-v2.6.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/hetznercloud/hcloud-go/v2/com_github_hetznercloud_hcloud_go_v2-v2.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hexops_gotextdiff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hexops/gotextdiff",
        sum = "h1:gitA9+qJrrTCsiCl7+kh75nPqQt1cx4ZkudSTLoUqJM=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_hpcloud_tail",
        build_file_proto_mode = "disable",
        importpath = "github.com/hpcloud/tail",
        sum = "h1:nfCOvKYfkgYP8hkirhJocXT2+zOD8yUNjXaWfTlyFKI=",
        version = "v1.0.0",
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
        sum = "h1:EPRgaDqXpLFUJLXZdGLnBTy1l6CLiNAPnvn2l+kHit0=",
        version = "v0.0.0-20141126152155-54553eb933fb",
    )
    go_repository(
        name = "com_github_iancoleman_strcase",
        build_file_proto_mode = "disable",
        importpath = "github.com/iancoleman/strcase",
        sum = "h1:05I4QRnGpI0m37iZQRuskXh+w77mr6Z41lwQzuHLwW0=",
        version = "v0.2.0",
    )

    go_repository(
        name = "com_github_ianlancetaylor_demangle",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ianlancetaylor/demangle",
        sum = "h1:uGg2frlt3IcT7kbV6LEp5ONv4vmoO2FW4qSO+my/aoM=",
        version = "v0.0.0-20210905161508-09a460cdf81d",
    )
    go_repository(
        name = "com_github_ibm_sarama",
        build_file_proto_mode = "disable",
        importpath = "github.com/IBM/sarama",
        sum = "h1:Yj6L2IaNvb2mRBop39N7mmJAHBVY3dTPncr3qGVkxPA=",
        version = "v1.43.3",
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
        build_file_proto_mode = "disable",
        importpath = "github.com/imkira/go-interpol",
        sum = "h1:KIiKr0VSG2CUW1hl1jpiyuzuJeKUUpC8iM1AIE7N1Vk=",
        version = "v1.1.0",
    )

    go_repository(
        name = "com_github_inconshreveable_mousetrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/inconshreveable/mousetrap",
        sum = "h1:U3uMjPSQEBMNp1lFxmllqCPM6P5u/Xq7Pgzkat/bFNc=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_influxdata_influxdb",
        build_file_proto_mode = "disable",
        importpath = "github.com/influxdata/influxdb",
        sum = "h1:O08dwjOwv9CYlJJEUZKAazSoQDKlsN34Bq3dnhqhyVI=",
        version = "v0.0.0-20170331210902-15e594fc09f1",
    )

    go_repository(
        name = "com_github_influxdata_tdigest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/tdigest",
        sum = "h1:XpFptwYmnEKUqmkcDjrzffswZ3nvNeevbUSLPP/ZzIY=",
        version = "v0.0.1",
    )
    go_repository(
        name = "com_github_ionos_cloud_sdk_go_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ionos-cloud/sdk-go/v6",
        sha256 = "f9538685d901f7c96779dc5b1341a3a5401ae235dd82ee700ded27c838911e4d",
        strip_prefix = "github.com/ionos-cloud/sdk-go/v6@v6.1.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ionos-cloud/sdk-go/v6/com_github_ionos_cloud_sdk_go_v6-v6.1.11.zip",
            "http://ats.apps.svc/gomod/github.com/ionos-cloud/sdk-go/v6/com_github_ionos_cloud_sdk_go_v6-v6.1.11.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ionos-cloud/sdk-go/v6/com_github_ionos_cloud_sdk_go_v6-v6.1.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ionos-cloud/sdk-go/v6/com_github_ionos_cloud_sdk_go_v6-v6.1.11.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_blackfriday",
        build_file_proto_mode = "disable",
        importpath = "github.com/iris-contrib/blackfriday",
        sum = "h1:o5sHQHHm0ToHUlAJSTjW9UWicjJSDDauOOQ2AHuIVp4=",
        version = "v2.0.0+incompatible",
    )
    go_repository(
        name = "com_github_iris_contrib_go_uuid",
        build_file_proto_mode = "disable",
        importpath = "github.com/iris-contrib/go.uuid",
        sum = "h1:XZubAYg61/JwnJNbZilGjf3b3pB80+OQg2qf6c8BfWE=",
        version = "v2.0.0+incompatible",
    )
    go_repository(
        name = "com_github_iris_contrib_i18n",
        build_file_proto_mode = "disable",
        importpath = "github.com/iris-contrib/i18n",
        sum = "h1:Kyp9KiXwsyZRTeoNjgVCrWks7D8ht9+kg6yCjh8K97o=",
        version = "v0.0.0-20171121225848-987a633949d0",
    )

    go_repository(
        name = "com_github_iris_contrib_schema",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/schema",
        sum = "h1:10g/WnoRR+U+XXHWKBHeNy/+tZmM2kcAVGLOsz+yaDA=",
        version = "v0.0.1",
    )
    go_repository(
        name = "com_github_jackc_fake",
        build_file_proto_mode = "disable",
        importpath = "github.com/jackc/fake",
        sum = "h1:vr3AYkKovP8uR8AvSGGUK1IDqRa5lAAvEkZG1LKaCRc=",
        version = "v0.0.0-20150926172116-812a484cc733",
    )
    go_repository(
        name = "com_github_jackc_pgx",
        build_file_proto_mode = "disable",
        importpath = "github.com/jackc/pgx",
        sum = "h1:0Vihzu20St42/UDsvZGdNE6jak7oi/UOeMzwMPHkgFY=",
        version = "v3.2.0+incompatible",
    )
    go_repository(
        name = "com_github_jarcoal_httpmock",
        build_file_proto_mode = "disable",
        importpath = "github.com/jarcoal/httpmock",
        sum = "h1:gSvTxxFR/MEMfsGrvRbdfpRUMBStovlSRLw0Ep1bwwc=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_jcmturner_aescts_v2",
        build_file_proto_mode = "disable",
        importpath = "github.com/jcmturner/aescts/v2",
        sum = "h1:9YKLH6ey7H4eDBXW8khjYslgyqG2xZikXP0EQFKrle8=",
        version = "v2.0.0",
    )
    go_repository(
        name = "com_github_jcmturner_dnsutils_v2",
        build_file_proto_mode = "disable",
        importpath = "github.com/jcmturner/dnsutils/v2",
        sum = "h1:lltnkeZGL0wILNvrNiVCR6Ro5PGU/SeBvVO/8c/iPbo=",
        version = "v2.0.0",
    )

    go_repository(
        name = "com_github_jcmturner_gofork",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/gofork",
        sum = "h1:QH0l3hzAU1tfT3rZCnW5zXl+orbkNMMRGJfdJjHVETg=",
        version = "v1.7.6",
    )
    go_repository(
        name = "com_github_jcmturner_goidentity_v6",
        build_file_proto_mode = "disable",
        importpath = "github.com/jcmturner/goidentity/v6",
        sum = "h1:VKnZd2oEIMorCTsFBnJWbExfNN7yZr3EhJAxwOkZg6o=",
        version = "v6.0.1",
    )
    go_repository(
        name = "com_github_jcmturner_gokrb5_v8",
        build_file_proto_mode = "disable",
        importpath = "github.com/jcmturner/gokrb5/v8",
        sum = "h1:x1Sv4HaTpepFkXbt2IkL29DXRf8sOfZXo8eRKh687T8=",
        version = "v8.4.4",
    )
    go_repository(
        name = "com_github_jcmturner_rpc_v2",
        build_file_proto_mode = "disable",
        importpath = "github.com/jcmturner/rpc/v2",
        sum = "h1:7FXXj8Ti1IaVFpSAziCZWNzbNuZmnvw/i6CqLNdWfZY=",
        version = "v2.0.3",
    )

    go_repository(
        name = "com_github_jedib0t_go_pretty_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jedib0t/go-pretty/v6",
        sum = "h1:o3McN0rQ4X+IU+HduppSp9TwRdGLRW2rhJXy9CJaCRw=",
        version = "v6.2.2",
    )
    go_repository(
        name = "com_github_jeffail_gabs_v2",
        build_file_proto_mode = "disable",
        importpath = "github.com/Jeffail/gabs/v2",
        sum = "h1:ANfZYjpMlfTTKebycu4X1AgkVWumFVDYQl7JwOr4mDk=",
        version = "v2.5.1",
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
        sum = "h1:HxVbL1MhydKs8R8n/HE5NPvzfaYmQJA3o879lE4+WcM=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_jingyugao_rowserrcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jingyugao/rowserrcheck",
        sum = "h1:zibz55j/MJtLsjP1OF4bSdgXxwL1b+Vn7Tjzq7gFzUs=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_jinzhu_inflection",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jinzhu/inflection",
        sha256 = "cf1087a6f6653ed5f366f85cf0110bbbf581d4e9bc8a4d1a9b56765d94b546c3",
        strip_prefix = "github.com/jinzhu/inflection@v1.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jinzhu/inflection/com_github_jinzhu_inflection-v1.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/jinzhu/inflection/com_github_jinzhu_inflection-v1.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jinzhu/inflection/com_github_jinzhu_inflection-v1.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jinzhu/inflection/com_github_jinzhu_inflection-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jinzhu_now",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jinzhu/now",
        sha256 = "1223dc55db616f156c1f1467adc2c88f786905df3cc3cb4fd5161badd654c62b",
        strip_prefix = "github.com/jinzhu/now@v1.1.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jinzhu/now/com_github_jinzhu_now-v1.1.5.zip",
            "http://ats.apps.svc/gomod/github.com/jinzhu/now/com_github_jinzhu_now-v1.1.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jinzhu/now/com_github_jinzhu_now-v1.1.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jinzhu/now/com_github_jinzhu_now-v1.1.5.zip",
        ],
    )
    go_repository(
        name = "com_github_jirfag_go_printf_func_name",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jirfag/go-printf-func-name",
        sum = "h1:KA9BjwUk7KlCh6S9EAGWBt1oExIUv9WyNCiRz5amv48=",
        version = "v0.0.0-20200119135958-7558a9eaa5af",
    )
    go_repository(
        name = "com_github_jjti_go_spancheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jjti/go-spancheck",
        sha256 = "187ebe9dd66e096715bdb989bf60ddc87df773b4fa4f9c65df88177f14ba50c9",
        strip_prefix = "github.com/jjti/go-spancheck@v0.6.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/jjti/go-spancheck/com_github_jjti_go_spancheck-v0.6.2.zip",
            "http://ats.apps.svc/gomod/github.com/jjti/go-spancheck/com_github_jjti_go_spancheck-v0.6.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/jjti/go-spancheck/com_github_jjti_go_spancheck-v0.6.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/jjti/go-spancheck/com_github_jjti_go_spancheck-v0.6.2.zip",
        ],
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jmespath/go-jmespath",
        sum = "h1:BEgLn5cpjn8UN1mAw4NjwDrS35OdebyEtFe+9YPoQUg=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath_internal_testify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jmespath/go-jmespath/internal/testify",
        sum = "h1:shLQSRRSCCPj3f2gpwzGwWFoC7ycTf1rcQZHOlsJ6N8=",
        version = "v1.5.1",
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
        name = "com_github_johncgriffin_overflow",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/JohnCGriffin/overflow",
        sha256 = "8ad4da840214861386d243127290666cc54eb914d1f4a8856523481876af2a09",
        strip_prefix = "github.com/JohnCGriffin/overflow@v0.0.0-20211019200055-46fa312c352c",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/JohnCGriffin/overflow/com_github_johncgriffin_overflow-v0.0.0-20211019200055-46fa312c352c.zip",
            "http://ats.apps.svc/gomod/github.com/JohnCGriffin/overflow/com_github_johncgriffin_overflow-v0.0.0-20211019200055-46fa312c352c.zip",
            "https://cache.hawkingrei.com/gomod/github.com/JohnCGriffin/overflow/com_github_johncgriffin_overflow-v0.0.0-20211019200055-46fa312c352c.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/JohnCGriffin/overflow/com_github_johncgriffin_overflow-v0.0.0-20211019200055-46fa312c352c.zip",
        ],
    )
    go_repository(
        name = "com_github_joho_sqltocsv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/joho/sqltocsv",
        sum = "h1:Zrb0IbuLOGHL7nrO2WrcuNWgDTlzFv3zY69QMx4ggQE=",
        version = "v0.0.0-20210428211105-a6d6801d59df",
    )
    go_repository(
        name = "com_github_joker_hpp",
        build_file_proto_mode = "disable",
        importpath = "github.com/Joker/hpp",
        sum = "h1:65+iuJYdRXv/XyN62C1uEmmOx3432rNG/rKlX6V7Kkc=",
        version = "v1.0.0",
    )

    go_repository(
        name = "com_github_joker_jade",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Joker/jade",
        sum = "h1:mreN1m/5VJ/Zc3b4pzj9qU6D9SRQ6Vm+3KfI328t3S8=",
        version = "v1.0.1-0.20190614124447-d475f43051e7",
    )
    go_repository(
        name = "com_github_jonboulle_clockwork",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jonboulle/clockwork",
        sum = "h1:UOGuzwb1PwsrDAObMuhUnj0p5ULPj8V/xJ7Kx9qUBdQ=",
        version = "v0.2.2",
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
        name = "com_github_josharian_txtarfs",
        build_file_proto_mode = "disable",
        importpath = "github.com/josharian/txtarfs",
        sum = "h1:8NZHLa6Gp0hW6xJ0c3F1Kse7dJw30fOcDzHuF9sLbnE=",
        version = "v0.0.0-20210218200122-0702f000015a",
    )

    go_repository(
        name = "com_github_jpillora_backoff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jpillora/backoff",
        sum = "h1:uvFg412JmmHBHw7iwprIxkPMI+sGQ4kzOWsMeHnm2EA=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_json_iterator_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/json-iterator/go",
        sum = "h1:PV8peI4a0ysnczrg+LtxykD8LfKY9ML6u2jnxaEnrnM=",
        version = "v1.1.12",
    )
    go_repository(
        name = "com_github_jstemmer_go_junit_report",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jstemmer/go-junit-report",
        sum = "h1:6QPYqodiu3GuPL+7mfx+NwDdp2eTkp9IfEUpgAwUN0o=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_github_jtolds_gls",
        build_file_proto_mode = "disable",
        importpath = "github.com/jtolds/gls",
        sum = "h1:xdiiI2gbIgH/gLH7ADydsJ1uDOEzR8yvV7C0MuV77Wo=",
        version = "v4.20.0+incompatible",
    )
    go_repository(
        name = "com_github_juju_errors",
        build_file_proto_mode = "disable",
        importpath = "github.com/juju/errors",
        sum = "h1:rhqTjzJlm7EbkELJDKMTU7udov+Se0xZkWmugr6zGok=",
        version = "v0.0.0-20181118221551-089d3ea4e4d5",
    )
    go_repository(
        name = "com_github_juju_loggo",
        build_file_proto_mode = "disable",
        importpath = "github.com/juju/loggo",
        sum = "h1:MK144iBQF9hTSwBW/9eJm034bVoG30IshVm688T2hi8=",
        version = "v0.0.0-20180524022052-584905176618",
    )
    go_repository(
        name = "com_github_juju_testing",
        build_file_proto_mode = "disable",
        importpath = "github.com/juju/testing",
        sum = "h1:WQM1NildKThwdP7qWrNAFGzp4ijNLw8RlgENkaI4MJs=",
        version = "v0.0.0-20180920084828-472a3e8b2073",
    )

    go_repository(
        name = "com_github_julienschmidt_httprouter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/julienschmidt/httprouter",
        sum = "h1:U0609e9tgbseu3rBINet9P48AI/D3oJs4dN7jwJOQ1U=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_julz_importas",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/julz/importas",
        sum = "h1:F78HnrsjY3cR7j0etXy5+TU1Zuy7Xt08X/1aJnH5xXY=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_jung_kurt_gofpdf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jung-kurt/gofpdf",
        sum = "h1:PJr+ZMXIecYc1Ey2zucXdR73SMBtgjPgwa31099IMv0=",
        version = "v1.0.3-0.20190309125859-24315acbbda5",
    )
    go_repository(
        name = "com_github_k0kubun_colorstring",
        build_file_proto_mode = "disable",
        importpath = "github.com/k0kubun/colorstring",
        sum = "h1:uC1QfSlInpQF+M0ao65imhwqKnz3Q2z/d8PWZRMQvDM=",
        version = "v0.0.0-20150214042306-9440f1994b88",
    )

    go_repository(
        name = "com_github_karamaru_alpha_copyloopvar",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/karamaru-alpha/copyloopvar",
        sha256 = "60c387926b32870d5f5acc818ec648a3bde2f95acfd37151e073466cf7cafd86",
        strip_prefix = "github.com/karamaru-alpha/copyloopvar@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/karamaru-alpha/copyloopvar/com_github_karamaru_alpha_copyloopvar-v1.1.0.zip",
            "http://ats.apps.svc/gomod/github.com/karamaru-alpha/copyloopvar/com_github_karamaru_alpha_copyloopvar-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/karamaru-alpha/copyloopvar/com_github_karamaru_alpha_copyloopvar-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/karamaru-alpha/copyloopvar/com_github_karamaru_alpha_copyloopvar-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_blocks",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/blocks",
        sha256 = "bdc3d49ea54a2a2ef733ae701986be69fba7d735ae876ea736806e4f3ef00a8b",
        strip_prefix = "github.com/kataras/blocks@v0.0.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kataras/blocks/com_github_kataras_blocks-v0.0.7.zip",
            "http://ats.apps.svc/gomod/github.com/kataras/blocks/com_github_kataras_blocks-v0.0.7.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kataras/blocks/com_github_kataras_blocks-v0.0.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kataras/blocks/com_github_kataras_blocks-v0.0.7.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_golog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/golog",
        sum = "h1:J7Dl82843nbKQDrQM/abbNJZvQjS6PfmkkffhOTXEpM=",
        version = "v0.0.9",
    )
    go_repository(
        name = "com_github_kataras_iris_v12",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/iris/v12",
        sum = "h1:Wo5S7GMWv5OAzJmvFTvss/C4TS1W0uo6LkDlSymT4rM=",
        version = "v12.0.1",
    )
    go_repository(
        name = "com_github_kataras_neffos",
        build_file_proto_mode = "disable",
        importpath = "github.com/kataras/neffos",
        sum = "h1:O06dvQlxjdWvzWbm2Bq+Si6psUhvSmEctAMk9Xujqms=",
        version = "v0.0.10",
    )

    go_repository(
        name = "com_github_kataras_pio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/pio",
        sum = "h1:V5Rs9ztEWdp58oayPq/ulmlqJJZeJP6pP79uP3qjcao=",
        version = "v0.0.0-20190103105442-ea782b38602d",
    )
    go_repository(
        name = "com_github_kataras_sitemap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/sitemap",
        sha256 = "800ba5c5a28e512c18e3aaa7be50125db98c5be70b84107f3f90713ac2269ea0",
        strip_prefix = "github.com/kataras/sitemap@v0.0.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kataras/sitemap/com_github_kataras_sitemap-v0.0.6.zip",
            "http://ats.apps.svc/gomod/github.com/kataras/sitemap/com_github_kataras_sitemap-v0.0.6.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kataras/sitemap/com_github_kataras_sitemap-v0.0.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kataras/sitemap/com_github_kataras_sitemap-v0.0.6.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_tunnel",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/tunnel",
        sha256 = "1ae8dcc9a6ca3f47c5f8b57767a08b0acd916eceef49c48aa9859547316db8e2",
        strip_prefix = "github.com/kataras/tunnel@v0.0.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kataras/tunnel/com_github_kataras_tunnel-v0.0.4.zip",
            "http://ats.apps.svc/gomod/github.com/kataras/tunnel/com_github_kataras_tunnel-v0.0.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kataras/tunnel/com_github_kataras_tunnel-v0.0.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kataras/tunnel/com_github_kataras_tunnel-v0.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_kballard_go_shellquote",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kballard/go-shellquote",
        sha256 = "ae4cb7b097dc4eb0c248dff00ed3bbf0f36984c4162ad1d615266084e58bd6cc",
        strip_prefix = "github.com/kballard/go-shellquote@v0.0.0-20180428030007-95032a82bc51",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/kballard/go-shellquote/com_github_kballard_go_shellquote-v0.0.0-20180428030007-95032a82bc51.zip",
            "http://ats.apps.svc/gomod/github.com/kballard/go-shellquote/com_github_kballard_go_shellquote-v0.0.0-20180428030007-95032a82bc51.zip",
            "https://cache.hawkingrei.com/gomod/github.com/kballard/go-shellquote/com_github_kballard_go_shellquote-v0.0.0-20180428030007-95032a82bc51.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/kballard/go-shellquote/com_github_kballard_go_shellquote-v0.0.0-20180428030007-95032a82bc51.zip",
        ],
    )
    go_repository(
        name = "com_github_keybase_go_keychain",
        build_file_proto_mode = "disable",
        importpath = "github.com/keybase/go-keychain",
        sum = "h1:IsMZxCuZqKuao2vNdfD82fjjgPLfyHLpR41Z88viRWs=",
        version = "v0.0.0-20231219164618-57a3676c3af6",
    )

    go_repository(
        name = "com_github_kimmachinegun_automemlimit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/KimMachineGun/automemlimit",
        sha256 = "fb03ca71f809b89d513c0cbb5ad7b05c1268e220401298958cb970b76027e4ca",
        strip_prefix = "github.com/KimMachineGun/automemlimit@v0.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/KimMachineGun/automemlimit/com_github_kimmachinegun_automemlimit-v0.5.0.zip",
            "http://ats.apps.svc/gomod/github.com/KimMachineGun/automemlimit/com_github_kimmachinegun_automemlimit-v0.5.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/KimMachineGun/automemlimit/com_github_kimmachinegun_automemlimit-v0.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/KimMachineGun/automemlimit/com_github_kimmachinegun_automemlimit-v0.5.0.zip",
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
        sum = "h1:uGQ9xI8/pgc9iOoCe7kWQgRE6SBTrCGmTSf0LrEtY7c=",
        version = "v1.6.2",
    )
    go_repository(
        name = "com_github_kisielk_gotool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kisielk/gotool",
        sum = "h1:AV2c/EiW3KqPNT9ZKl07ehoAGi4C5/01Cfbblndcapg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_kkhaike_contextcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kkHAIKE/contextcheck",
        sum = "h1:l4pNvrb8JSwRd51ojtcOxOeHJzHek+MtOyXbaR0uvmw=",
        version = "v1.1.3",
    )
    go_repository(
        name = "com_github_klauspost_asmfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/asmfmt",
        sha256 = "fa6a350a8677a77e0dbf3664c6baf23aab5c0b60a64b8f3c00299da5d279021f",
        strip_prefix = "github.com/klauspost/asmfmt@v1.3.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/klauspost/asmfmt/com_github_klauspost_asmfmt-v1.3.2.zip",
            "http://ats.apps.svc/gomod/github.com/klauspost/asmfmt/com_github_klauspost_asmfmt-v1.3.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/klauspost/asmfmt/com_github_klauspost_asmfmt-v1.3.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/klauspost/asmfmt/com_github_klauspost_asmfmt-v1.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_klauspost_compress",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/compress",
        sum = "h1:6KIumPrER1LHsvBVuDa0r5xaG0Es51mhhB9BQB2qeMA=",
        version = "v1.17.9",
    )
    go_repository(
        name = "com_github_klauspost_cpuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/cpuid",
        sum = "h1:5JNjFYYQrZeKRJ0734q51WCEEn2huer72Dc7K+R/b6s=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_klauspost_cpuid_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/cpuid/v2",
        sha256 = "52c716413296dce2b1698c6cdbc4c53927ce4aee2a60980daf9672e6b6a3b4cb",
        strip_prefix = "github.com/klauspost/cpuid/v2@v2.0.9",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/klauspost/cpuid/v2/com_github_klauspost_cpuid_v2-v2.0.9.zip",
            "http://ats.apps.svc/gomod/github.com/klauspost/cpuid/v2/com_github_klauspost_cpuid_v2-v2.0.9.zip",
            "https://cache.hawkingrei.com/gomod/github.com/klauspost/cpuid/v2/com_github_klauspost_cpuid_v2-v2.0.9.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/klauspost/cpuid/v2/com_github_klauspost_cpuid_v2-v2.0.9.zip",
        ],
    )
    go_repository(
        name = "com_github_knz_strtime",
        build_file_proto_mode = "disable",
        importpath = "github.com/knz/strtime",
        sum = "h1:45aLE1GlZRKxNfTMkok85BUKAJNLdHr5GAm3h8Fqoww=",
        version = "v0.0.0-20181018220328-af2256ee352c",
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
        sum = "h1:CE8S1cTafDpPvMhIxNJKvHsGVBgn1xWYf1NbHQhywc8=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_kr_logfmt",
        build_file_proto_mode = "disable",
        importpath = "github.com/kr/logfmt",
        sum = "h1:T+h1c/A9Gawja4Y9mFVWj2vyii2bbUNDw3kt9VxK2EY=",
        version = "v0.0.0-20140226030751-b84e30acd515",
    )

    go_repository(
        name = "com_github_kr_pretty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/pretty",
        sum = "h1:flRD4NNwYAUpkphVc1HcthR4KEIFJ65n8Mw5qdRn3LE=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_kr_pty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/pty",
        sum = "h1:VkoXIwSboBpnk99O/KFauAEILuNHv5DVFKZMBN/gUgw=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_kr_text",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/text",
        sum = "h1:5Nx0Ya0ZqY2ygV366QzturHI13Jq95ApcVaJBhpS+AY=",
        version = "v0.2.0",
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
        sum = "h1:ElhKf+AlItIu+xGnI990no4cE2+XaSu1ULymV2Yulxs=",
        version = "v0.6.3",
    )
    go_repository(
        name = "com_github_kunwardeep_paralleltest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kunwardeep/paralleltest",
        sum = "h1:FCKYMF1OF2+RveWlABsdnmsvJrei5aoyZoaGS+Ugg8g=",
        version = "v1.0.6",
    )
    go_repository(
        name = "com_github_kylelemons_godebug",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kylelemons/godebug",
        sum = "h1:RPNrshWIDI6G2gRW9EHilWtl7Z6Sb1BR0xunSBf0SNc=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_kyoh86_exportloopref",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kyoh86/exportloopref",
        sum = "h1:5Ry/at+eFdkX9Vsdw3qU4YkvGtzuVfzT4X7S77LoN/M=",
        version = "v0.1.8",
    )
    go_repository(
        name = "com_github_labstack_echo_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/labstack/echo/v4",
        sum = "h1:z0BZoArY4FqdpUEl+wlHp4hnr/oSR6MTmQmv8OHSoww=",
        version = "v4.1.11",
    )
    go_repository(
        name = "com_github_labstack_gommon",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/labstack/gommon",
        sum = "h1:JEeO0bvc78PKdyHxloTKiF8BD5iGrH8T6MSeGvSgob0=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_lance6716_pebble",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lance6716/pebble",
        sha256 = "8fe7ce7009c4d2b0ae28f4c3d62f38b6256ee20e47944490b58cb1d33a155725",
        strip_prefix = "github.com/lance6716/pebble@v0.0.0-20241108073934-da961314c63f",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lance6716/pebble/com_github_lance6716_pebble-v0.0.0-20241108073934-da961314c63f.zip",
            "http://ats.apps.svc/gomod/github.com/lance6716/pebble/com_github_lance6716_pebble-v0.0.0-20241108073934-da961314c63f.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lance6716/pebble/com_github_lance6716_pebble-v0.0.0-20241108073934-da961314c63f.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lance6716/pebble/com_github_lance6716_pebble-v0.0.0-20241108073934-da961314c63f.zip",
        ],
    )
    go_repository(
        name = "com_github_lasiar_canonicalheader",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lasiar/canonicalheader",
        sha256 = "af7eca4ce304701f7878ec4adbf2165ac1ffe58f8a737a96f887f071b28b20f0",
        strip_prefix = "github.com/lasiar/canonicalheader@v1.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/lasiar/canonicalheader/com_github_lasiar_canonicalheader-v1.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/lasiar/canonicalheader/com_github_lasiar_canonicalheader-v1.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/lasiar/canonicalheader/com_github_lasiar_canonicalheader-v1.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/lasiar/canonicalheader/com_github_lasiar_canonicalheader-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_ldez_gomoddirectives",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ldez/gomoddirectives",
        sum = "h1:y7MBaisZVDYmKvt9/l1mjNCiSA1BVn34U0ObUcJwlhA=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_ldez_tagliatelle",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ldez/tagliatelle",
        sum = "h1:3BqVVlReVUZwafJUwQ+oxbx2BEX2vUG4Yu/NOfMiKiM=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_leodido_go_urn",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/leodido/go-urn",
        sha256 = "8ae6e756f0e919a551e447f286491c08ca36ceaf415c2dde395fd79c1a408d1a",
        strip_prefix = "github.com/leodido/go-urn@v1.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/leodido/go-urn/com_github_leodido_go_urn-v1.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/leodido/go-urn/com_github_leodido_go_urn-v1.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/leodido/go-urn/com_github_leodido_go_urn-v1.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/leodido/go-urn/com_github_leodido_go_urn-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_leonklingele_grouper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/leonklingele/grouper",
        sum = "h1:tC2y/ygPbMFSBOs3DcyaEMKnnwH7eYKzohOtRrf0SAg=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_lestrrat_go_blackmagic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/blackmagic",
        sum = "h1:Cg2gVSc9h7sz9NOByczrbUvLopQmXrfFx//N+AkAr5k=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_lestrrat_go_httpcc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/httpcc",
        sum = "h1:ydWCStUeJLkpYyjLDHihupbn2tYmZ7m22BGkcvZZrIE=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_lestrrat_go_httprc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/httprc",
        sum = "h1:bsTfiH8xaKOJPrg1R+E3iE/AWZr/x0Phj9PBTG/OLUk=",
        version = "v1.0.5",
    )
    go_repository(
        name = "com_github_lestrrat_go_iter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/iter",
        sum = "h1:gMXo1q4c2pHmC3dn8LzRhJfP1ceCbgSiT9lUydIzltI=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_lestrrat_go_jwx_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/jwx/v2",
        sum = "h1:jAPKupy4uHgrHFEdjVjNkUgoBKtVDgrQPB/h55FHrR0=",
        version = "v2.0.21",
    )
    go_repository(
        name = "com_github_lestrrat_go_option",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lestrrat-go/option",
        sum = "h1:oAzP2fvZGQKWkvHa1/SAcFolBEca1oN+mQ7eooNBEYU=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_lib_pq",
        build_file_proto_mode = "disable",
        importpath = "github.com/lib/pq",
        sum = "h1:X5PMW56eZitiTeO7tKzZxFCSpbFZJtkMMooicw2us9A=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_lightstep_lightstep_tracer_go",
        build_file_proto_mode = "disable",
        importpath = "github.com/lightstep/lightstep-tracer-go",
        sum = "h1:D0GGa7afJ7GcQvu5as6ssLEEKYXvRgKI5d5cevtz8r4=",
        version = "v0.15.6",
    )

    go_repository(
        name = "com_github_linode_linodego",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/linode/linodego",
        sha256 = "af21149264f65f5a0383ee0a109e8ef1e4f5db95f951d657cb923b0b4f771d4a",
        strip_prefix = "github.com/linode/linodego@v1.27.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/linode/linodego/com_github_linode_linodego-v1.27.1.zip",
            "http://ats.apps.svc/gomod/github.com/linode/linodego/com_github_linode_linodego-v1.27.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/linode/linodego/com_github_linode_linodego-v1.27.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/linode/linodego/com_github_linode_linodego-v1.27.1.zip",
        ],
    )
    go_repository(
        name = "com_github_lufeee_execinquery",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lufeee/execinquery",
        sum = "h1:hf0Ems4SHcUGBxpGN7Jz78z1ppVkP/837ZlETPCEtOM=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_lufia_plan9stats",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lufia/plan9stats",
        sum = "h1:6E+4a0GO5zZEnZ81pIr0yLvtUWk2if982qA3F3QD6H4=",
        version = "v0.0.0-20211012122336-39d0f177ccd0",
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
        sum = "h1:5ibWZ6iY0NctNGWo87LalDlEZ6R41TqbbDamhfG/Qzo=",
        version = "v1.8.6",
    )
    go_repository(
        name = "com_github_mailgun_raymond_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mailgun/raymond/v2",
        sha256 = "9ff5de08464b1bc2d0a0dd6f4e7cadd20888e5ad39bf2acea09652750b1e92e0",
        strip_prefix = "github.com/mailgun/raymond/v2@v2.0.48",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mailgun/raymond/v2/com_github_mailgun_raymond_v2-v2.0.48.zip",
            "http://ats.apps.svc/gomod/github.com/mailgun/raymond/v2/com_github_mailgun_raymond_v2-v2.0.48.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mailgun/raymond/v2/com_github_mailgun_raymond_v2-v2.0.48.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mailgun/raymond/v2/com_github_mailgun_raymond_v2-v2.0.48.zip",
        ],
    )
    go_repository(
        name = "com_github_mailru_easyjson",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mailru/easyjson",
        sum = "h1:hB2xlXdHp/pmPZq0y3QnmWAArdw9PqbmotexnWx/FU8=",
        version = "v0.0.0-20190626092158-b2ccc519800e",
    )
    go_repository(
        name = "com_github_maratori_testableexamples",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/maratori/testableexamples",
        sum = "h1:dU5alXRrD8WKSjOUnmJZuzdxWOEQ57+7s93SLMxb2vI=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_maratori_testpackage",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/maratori/testpackage",
        sum = "h1:GJY4wlzQhuBusMF1oahQCBtUV/AQ/k69IZ68vxaac2Q=",
        version = "v1.1.0",
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
        sum = "h1:H65muMkzWKEuNDnfl9d70GUjFniHKHRbFPGBuZ3QEww=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_masterminds_semver_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/semver/v3",
        sha256 = "b492e8f6fa4c8240234a6d5095521f75d9e2e7c52672b90b5afde533281b540f",
        strip_prefix = "github.com/Masterminds/semver/v3@v3.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/Masterminds/semver/v3/com_github_masterminds_semver_v3-v3.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/Masterminds/semver/v3/com_github_masterminds_semver_v3-v3.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/Masterminds/semver/v3/com_github_masterminds_semver_v3-v3.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/Masterminds/semver/v3/com_github_masterminds_semver_v3-v3.3.0.zip",
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
        sum = "h1:pWxk9e//NbPwfxat7RXkts09K+dEBJWakUWwICVqYbA=",
        version = "v0.0.0-20210227103229-6504466cf951",
    )
    go_repository(
        name = "com_github_mattn_go_colorable",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-colorable",
        sum = "h1:fFA4WZxdEF4tXPZVKMLwD8oUnCTTo08duU7wxecdEvA=",
        version = "v0.1.13",
    )
    go_repository(
        name = "com_github_mattn_go_isatty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-isatty",
        sum = "h1:bq3VjFmv/sOjHtdEhmkEV4x1AJtvUvOJ2PFAZ5+peKQ=",
        version = "v0.0.16",
    )
    go_repository(
        name = "com_github_mattn_go_runewidth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-runewidth",
        sum = "h1:+xnbZSEeDbOIg5/mE6JF0w6n9duR1l3/WmbinWVwUuU=",
        version = "v0.0.14",
    )
    go_repository(
        name = "com_github_mattn_go_sqlite3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-sqlite3",
        sha256 = "0114d2df439ddeb03eef49a4bf2cc8fb69665c0d76494463cafa7d189a16e0f9",
        strip_prefix = "github.com/mattn/go-sqlite3@v1.14.15",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/mattn/go-sqlite3/com_github_mattn_go_sqlite3-v1.14.15.zip",
            "http://ats.apps.svc/gomod/github.com/mattn/go-sqlite3/com_github_mattn_go_sqlite3-v1.14.15.zip",
            "https://cache.hawkingrei.com/gomod/github.com/mattn/go-sqlite3/com_github_mattn_go_sqlite3-v1.14.15.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/mattn/go-sqlite3/com_github_mattn_go_sqlite3-v1.14.15.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_goveralls",
        build_file_proto_mode = "disable",
        importpath = "github.com/mattn/goveralls",
        sum = "h1:7eJB6EqsPhRVxvwEXGnqdO2sJI0PTsrWoTMXEk9/OQc=",
        version = "v0.0.2",
    )

    go_repository(
        name = "com_github_matttproud_golang_protobuf_extensions",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/matttproud/golang_protobuf_extensions",
        sum = "h1:4hp9jkHxhMHkqkrB3Ix0jegS5sx/RkqARlsWZ6pIwiU=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_maxatome_go_testdeep",
        build_file_proto_mode = "disable",
        importpath = "github.com/maxatome/go-testdeep",
        sum = "h1:Tgh5efyCYyJFGUYiT0qxBSIDeXw0F5zSoatlou685kk=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_github_mbilski_exhaustivestruct",
        build_file_proto_mode = "disable",
        importpath = "github.com/mbilski/exhaustivestruct",
        sum = "h1:wCBmUnSYufAHO6J4AVWY6ff+oxWxsVFrwgOdMUQePUo=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_mediocregopher_mediocre_go_lib",
        build_file_proto_mode = "disable",
        importpath = "github.com/mediocregopher/mediocre-go-lib",
        sum = "h1:3dQJqqDouawQgl3gBE1PNHKFkJYGEuFb1DbSlaxdosE=",
        version = "v0.0.0-20181029021733-cb65787f37ed",
    )
    go_repository(
        name = "com_github_mediocregopher_radix_v3",
        build_file_proto_mode = "disable",
        importpath = "github.com/mediocregopher/radix/v3",
        sum = "h1:oacPXPKHJg0hcngVVrdtTnfGJiS+PtwoQwTBZGFlV4k=",
        version = "v3.3.0",
    )

    go_repository(
        name = "com_github_mgechev_dots",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mgechev/dots",
        sum = "h1:zpIH83+oKzcpryru8ceC6BxnoG8TBrhgAvRg8obzup0=",
        version = "v0.0.0-20210922191527-e955255bf517",
    )
    go_repository(
        name = "com_github_mgechev_revive",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mgechev/revive",
        sum = "h1:+2Hd/S8oO2H0Ikq2+egtNwQsVhAeELHjxjIUFX5ajLI=",
        version = "v1.2.4",
    )
    go_repository(
        name = "com_github_microcosm_cc_bluemonday",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/microcosm-cc/bluemonday",
        sum = "h1:5lPfLTTAvAbtS0VqT+94yOtFnGfUWYyx0+iToC3Os3s=",
        version = "v1.0.2",
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
        sum = "h1:oN9gL93BkuPrer2rehDbDx86k4zbYJEnMP6Krh82nh0=",
        version = "v1.1.10",
    )
    go_repository(
        name = "com_github_minio_asm2plan9s",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/minio/asm2plan9s",
        sha256 = "39a2e28284764fd5423247d7469875046d0c8c4c2773333abf1c544197e9d946",
        strip_prefix = "github.com/minio/asm2plan9s@v0.0.0-20200509001527-cdd76441f9d8",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/minio/asm2plan9s/com_github_minio_asm2plan9s-v0.0.0-20200509001527-cdd76441f9d8.zip",
            "http://ats.apps.svc/gomod/github.com/minio/asm2plan9s/com_github_minio_asm2plan9s-v0.0.0-20200509001527-cdd76441f9d8.zip",
            "https://cache.hawkingrei.com/gomod/github.com/minio/asm2plan9s/com_github_minio_asm2plan9s-v0.0.0-20200509001527-cdd76441f9d8.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/minio/asm2plan9s/com_github_minio_asm2plan9s-v0.0.0-20200509001527-cdd76441f9d8.zip",
        ],
    )
    go_repository(
        name = "com_github_minio_c2goasm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/minio/c2goasm",
        sha256 = "04367ddf0fc5cd0f293e2c4f1acefb131b572539d88b5804d92efc905eb718b5",
        strip_prefix = "github.com/minio/c2goasm@v0.0.0-20190812172519-36a3d3bbc4f3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/minio/c2goasm/com_github_minio_c2goasm-v0.0.0-20190812172519-36a3d3bbc4f3.zip",
            "http://ats.apps.svc/gomod/github.com/minio/c2goasm/com_github_minio_c2goasm-v0.0.0-20190812172519-36a3d3bbc4f3.zip",
            "https://cache.hawkingrei.com/gomod/github.com/minio/c2goasm/com_github_minio_c2goasm-v0.0.0-20190812172519-36a3d3bbc4f3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/minio/c2goasm/com_github_minio_c2goasm-v0.0.0-20190812172519-36a3d3bbc4f3.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_cli",
        build_file_proto_mode = "disable",
        importpath = "github.com/mitchellh/cli",
        sum = "h1:iGBIsUe3+HZ/AD/Vd7DErOt5sU9fa8Uj7A2s1aggv1Y=",
        version = "v1.0.0",
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
        sum = "h1:lukF9ziXFxDFPkA1vsr5zpc1XuPDn/wFntq5mG+4E0Y=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_mitchellh_go_ps",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-ps",
        sum = "h1:i6ampVEEF4wQFF+bkYfwYgY+F/uYJDktmvLPf7qIgjc=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_mitchellh_go_testing_interface",
        build_file_proto_mode = "disable",
        importpath = "github.com/mitchellh/go-testing-interface",
        sum = "h1:fzU/JVNcaqHQEcVFAKeR41fkiLdIPrefOvVG1VZ96U0=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_mitchellh_go_wordwrap",
        build_file_proto_mode = "disable",
        importpath = "github.com/mitchellh/go-wordwrap",
        sum = "h1:6GlHJ/LTGMrIJbwgdqdl2eEH8o+Exx/0m8ir9Gns0u4=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_mitchellh_gox",
        build_file_proto_mode = "disable",
        importpath = "github.com/mitchellh/gox",
        sum = "h1:lfGJxY7ToLJQjHHwi0EX6uYBdK78egf954SQl13PQJc=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_mitchellh_iochan",
        build_file_proto_mode = "disable",
        importpath = "github.com/mitchellh/iochan",
        sum = "h1:C+X3KsSTLFVBr/tK1eYN/vs4rJcvsiLU338UhYPJWeY=",
        version = "v1.0.0",
    )

    go_repository(
        name = "com_github_mitchellh_mapstructure",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/mapstructure",
        sum = "h1:jeMsZIYE/09sWLaz43PL7Gy6RuMjD2eJVyuac5Z2hdY=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_mitchellh_reflectwalk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/reflectwalk",
        sum = "h1:FVzMWA5RllMAKIdUSC8mdWo3XtwoecrH79BY70sEEpE=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_moby_spdystream",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moby/spdystream",
        sum = "h1:cjW1zVyyoiM0T7b6UoySUFqzXMoqRckQtXwGPiBhOM8=",
        version = "v0.2.0",
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
        sum = "h1:TRLaZ9cD/w8PVh93nsPXa1VrQ6jlwL5oN8l14QlcNfg=",
        version = "v0.0.0-20180306012644-bacd9c7ef1dd",
    )
    go_repository(
        name = "com_github_modern_go_reflect2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/modern-go/reflect2",
        sum = "h1:xBagoLtFs94CBntxluKeaWgTMpvLxC4ur3nMaC9Gz0M=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_modocache_gover",
        build_file_proto_mode = "disable",
        importpath = "github.com/modocache/gover",
        sum = "h1:8Q0qkMVC/MmWkpIdlvZgcv2o2jrlF6zqVOh7W5YHdMA=",
        version = "v0.0.0-20171022184752-b58185e213c5",
    )

    go_repository(
        name = "com_github_montanaflynn_stats",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/montanaflynn/stats",
        sum = "h1:r3y12KyNxj/Sb/iOE46ws+3mS1+MZca1wlHQFPsY/JU=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_github_moricho_tparallel",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moricho/tparallel",
        sum = "h1:95FytivzT6rYzdJLdtfn6m1bfFJylOJK41+lgv/EHf4=",
        version = "v0.2.1",
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
        build_file_proto_mode = "disable",
        importpath = "github.com/moul/http2curl",
        sum = "h1:dRMWoAtb+ePxMlLkrCbAqh4TlPHXvoGUSQ323/9Zahs=",
        version = "v1.0.0",
    )

    go_repository(
        name = "com_github_munnerz_goautoneg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/munnerz/goautoneg",
        sum = "h1:C3w9PqII01/Oq1c1nUAm88MOHcQC9l5mIlSMApZMrHA=",
        version = "v0.0.0-20191010083416-a7dc8b61c822",
    )
    go_repository(
        name = "com_github_mwitkow_go_conntrack",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mwitkow/go-conntrack",
        sum = "h1:KUppIJq7/+SVif2QVs3tOP0zanoHgBEVAwHxUSIzRqU=",
        version = "v0.0.0-20190716064945-2f068394615f",
    )
    go_repository(
        name = "com_github_mxk_go_flowrate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mxk/go-flowrate",
        sum = "h1:y5//uYreIhSUg3J1GEMiLbxo1LJaP8RfCpH6pymGZus=",
        version = "v0.0.0-20140419014527-cca7078d478f",
    )
    go_repository(
        name = "com_github_nakabonne_nestif",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nakabonne/nestif",
        sum = "h1:wm28nZjhQY5HyYPx+weN3Q65k6ilSBxDb8v5S81B81U=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_nats_io_nats_go",
        build_file_proto_mode = "disable",
        importpath = "github.com/nats-io/nats.go",
        sum = "h1:6lF/f1/NN6kzUDBz6pyvQDEXO39jqXcWRLu/tKjtOUQ=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_github_nats_io_nkeys",
        build_file_proto_mode = "disable",
        importpath = "github.com/nats-io/nkeys",
        sum = "h1:+qM7QpgXnvDDixitZtQUBDY9w/s9mu1ghS+JIbsrx6M=",
        version = "v0.0.2",
    )
    go_repository(
        name = "com_github_nats_io_nuid",
        build_file_proto_mode = "disable",
        importpath = "github.com/nats-io/nuid",
        sum = "h1:5iA8DT8V7q8WK2EScv2padNa/rTESc1KdnPw4TC2paw=",
        version = "v1.0.1",
    )

    go_repository(
        name = "com_github_nbutton23_zxcvbn_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nbutton23/zxcvbn-go",
        sum = "h1:4kuARK6Y6FxaNu/BnU2OAaLF86eTVhP2hjTB6iMvItA=",
        version = "v0.0.0-20210217022336-fa2cb2858354",
    )
    go_repository(
        name = "com_github_ncw_directio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ncw/directio",
        sum = "h1:JSUBhdjEvVaJvOoyPAbcW0fnd0tvRXD76wEfZ1KcQz4=",
        version = "v1.0.5",
    )
    go_repository(
        name = "com_github_ngaut_pools",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ngaut/pools",
        sum = "h1:7KAv7KMGTTqSmYZtNdcNTgsos+vFzULLwyElndwn+5c=",
        version = "v0.0.0-20180318154953-b7bc8c42aac7",
    )
    go_repository(
        name = "com_github_ngaut_sync2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ngaut/sync2",
        sum = "h1:K0Fn+DoFqNqktdZtdV3bPQ/0cuYh2H4rkg0tytX/07k=",
        version = "v0.0.0-20141008032647-7a24ed77b2ef",
    )
    go_repository(
        name = "com_github_niemeyer_pretty",
        build_file_proto_mode = "disable",
        importpath = "github.com/niemeyer/pretty",
        sum = "h1:fD57ERR4JtEqsWbfPhv4DMiApHyliiK5xCTNVSPiaAs=",
        version = "v0.0.0-20200227124842-a10e7caefd8e",
    )

    go_repository(
        name = "com_github_nishanths_exhaustive",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nishanths/exhaustive",
        sum = "h1:pw5O09vwg8ZaditDp/nQRqVnrMczSJDxRDJMowvhsrM=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_github_nishanths_predeclared",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nishanths/predeclared",
        sum = "h1:V2EPdZPliZymNAn79T8RkNApBjMmVKh5XRpLm/w98Vk=",
        version = "v0.2.2",
    )
    go_repository(
        name = "com_github_nsf_jsondiff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nsf/jsondiff",
        sha256 = "d468c06359490d96ea701107f848acc7d9497e36eeb9eb81e38c8bef69c42fd2",
        strip_prefix = "github.com/nsf/jsondiff@v0.0.0-20230430225905-43f6cf3098c1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nsf/jsondiff/com_github_nsf_jsondiff-v0.0.0-20230430225905-43f6cf3098c1.zip",
            "http://ats.apps.svc/gomod/github.com/nsf/jsondiff/com_github_nsf_jsondiff-v0.0.0-20230430225905-43f6cf3098c1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nsf/jsondiff/com_github_nsf_jsondiff-v0.0.0-20230430225905-43f6cf3098c1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nsf/jsondiff/com_github_nsf_jsondiff-v0.0.0-20230430225905-43f6cf3098c1.zip",
        ],
    )
    go_repository(
        name = "com_github_nunnatsa_ginkgolinter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nunnatsa/ginkgolinter",
        sha256 = "33db4b181990597bffde2144e0a264a552d6fa3b67dea4d5c4213bc325d69d20",
        strip_prefix = "github.com/nunnatsa/ginkgolinter@v0.16.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.16.2.zip",
            "http://ats.apps.svc/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.16.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.16.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/nunnatsa/ginkgolinter/com_github_nunnatsa_ginkgolinter-v0.16.2.zip",
        ],
    )
    go_repository(
        name = "com_github_nxadm_tail",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nxadm/tail",
        sum = "h1:DQuhQpB1tVlglWS2hLQ5OV6B5r8aGxSrPc5Qo6uTN78=",
        version = "v1.4.4",
    )
    go_repository(
        name = "com_github_nytimes_gziphandler",
        build_file_proto_mode = "disable",
        importpath = "github.com/NYTimes/gziphandler",
        sum = "h1:lsxEuwrXEAokXB9qhlbKWPpo3KMLZQ5WB5WLQRW1uq0=",
        version = "v0.0.0-20170623195520-56545f4a5d46",
    )

    go_repository(
        name = "com_github_oklog_run",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/oklog/run",
        sum = "h1:Ru7dDtJNOyC66gQ5dQmaCa0qIsAUFY3sFpK1Xk8igrw=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_oklog_ulid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/oklog/ulid",
        sum = "h1:EGfNDEx6MqHz8B3uNV6QAib1UR2Lm97sHi3ocA6ESJ4=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_olekukonko_tablewriter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/olekukonko/tablewriter",
        sum = "h1:P2Ga83D34wi1o9J6Wh1mRuqd4mF/x/lgBS7N7AbDhec=",
        version = "v0.0.5",
    )
    go_repository(
        name = "com_github_oneofone_xxhash",
        build_file_proto_mode = "disable",
        importpath = "github.com/OneOfOne/xxhash",
        sum = "h1:zl/OfRA6nftbBK9qTohYBJ5xvw6C/oNKizR7cZGl3cI=",
        version = "v1.2.5",
    )

    go_repository(
        name = "com_github_onsi_ginkgo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/onsi/ginkgo",
        sum = "h1:8xi0RTUf59SOSfEtZMvwTvXYMzG4gV23XVHOZiXNtnE=",
        version = "v1.16.5",
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
        sum = "h1:M1GfJqGRrBrrGGsbxzV5dqM2U2ApXefZCQpkukxYRLE=",
        version = "v1.18.1",
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
        name = "com_github_opencontainers_runtime_spec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/runtime-spec",
        sha256 = "bd1531bb27014e2a16ea4bf0b56bff7688555bb859f651c1e4375f4b782269ec",
        strip_prefix = "github.com/opencontainers/runtime-spec@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/opencontainers/runtime-spec/com_github_opencontainers_runtime_spec-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/opencontainers/runtime-spec/com_github_opencontainers_runtime_spec-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/opencontainers/runtime-spec/com_github_opencontainers_runtime_spec-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/opencontainers/runtime-spec/com_github_opencontainers_runtime_spec-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_openpeedeep_depguard",
        build_file_proto_mode = "disable",
        importpath = "github.com/OpenPeeDeeP/depguard",
        sum = "h1:TSUznLjvp/4IUP+OQ0t/4jF4QUyxIcVX8YnghZdunyA=",
        version = "v1.1.1",
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
        sum = "h1:YyUAhaEfjoWXclZVJ9sGoNct7j4TVk7lZWlQw5UXuoo=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_opentracing_contrib_go_stdlib",
        build_file_proto_mode = "disable",
        importpath = "github.com/opentracing-contrib/go-stdlib",
        sum = "h1:8KbikWulLUcMM96hBxjgoo6gTmCkG6HYSDohv/WygYU=",
        version = "v0.0.0-20170113013457-1de4cc2120e7",
    )

    go_repository(
        name = "com_github_opentracing_opentracing_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing/opentracing-go",
        sum = "h1:uEJPy/1a5RIPAJ0Ov+OIO8OxWu77jEv+1B0VhjKrZUs=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_otiai10_copy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/otiai10/copy",
        sum = "h1:HvG945u96iNadPoG2/Ja2+AUJeW5YuFQMixq9yirC+k=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_otiai10_curr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/otiai10/curr",
        sum = "h1:TJIWdbX0B+kpNagQrjgq8bCMrbhiuX73M2XwgtDMoOI=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_otiai10_mint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/otiai10/mint",
        sum = "h1:BCmzIS3n71sGfHB5NMNDB3lHYPz8fWSkCAErHed//qc=",
        version = "v1.3.1",
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
        name = "com_github_pascaldekloe_goe",
        build_file_proto_mode = "disable",
        importpath = "github.com/pascaldekloe/goe",
        sum = "h1:Lgl0gzECD8GnQ5QCWA8o6BtfL6mDH5rQgM4/fX3avOs=",
        version = "v0.0.0-20180627143212-57f6aae5913c",
    )

    go_repository(
        name = "com_github_pbnjay_memory",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pbnjay/memory",
        sha256 = "5caf461e903c1060392e03a59eb84eb08c4a0976d0cc2d751fa9715cc6fc03bd",
        strip_prefix = "github.com/pbnjay/memory@v0.0.0-20210728143218-7b4eea64cf58",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/pbnjay/memory/com_github_pbnjay_memory-v0.0.0-20210728143218-7b4eea64cf58.zip",
            "http://ats.apps.svc/gomod/github.com/pbnjay/memory/com_github_pbnjay_memory-v0.0.0-20210728143218-7b4eea64cf58.zip",
            "https://cache.hawkingrei.com/gomod/github.com/pbnjay/memory/com_github_pbnjay_memory-v0.0.0-20210728143218-7b4eea64cf58.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/pbnjay/memory/com_github_pbnjay_memory-v0.0.0-20210728143218-7b4eea64cf58.zip",
        ],
    )
    go_repository(
        name = "com_github_pborman_getopt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pborman/getopt",
        sum = "h1:7822vZ646Atgxkp3tqrSufChvAAYgIy+iFEGpQntwlI=",
        version = "v0.0.0-20180729010549-6fdd0a2c7117",
    )
    go_repository(
        name = "com_github_pelletier_go_toml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pelletier/go-toml",
        sum = "h1:4yBQzkHv+7BHq2PQUZF3Mx0IYxG7LsP222s7Agd3ve8=",
        version = "v1.9.5",
    )
    go_repository(
        name = "com_github_pelletier_go_toml_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pelletier/go-toml/v2",
        sum = "h1:ipoSadvV8oGUjnUbMub59IDPPwfxF694nG/jwbMiyQg=",
        version = "v2.0.5",
    )
    go_repository(
        name = "com_github_peterbourgon_g2s",
        build_file_proto_mode = "disable",
        importpath = "github.com/peterbourgon/g2s",
        sum = "h1:sKwxy1H95npauwu8vtF95vG/syrL0p8fSZo/XlDg5gk=",
        version = "v0.0.0-20170223122336-d4e7ad98afea",
    )

    go_repository(
        name = "com_github_petermattis_goid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/petermattis/goid",
        sum = "h1:rUMC+oZ89Om6l9wvUNjzI0ZrKrSnXzV+opsgAohYUNc=",
        version = "v0.0.0-20170504144140-0ded85884ba5",
    )
    go_repository(
        name = "com_github_phayes_checkstyle",
        build_file_proto_mode = "disable",
        importpath = "github.com/phayes/checkstyle",
        sum = "h1:CdDQnGF8Nq9ocOS/xlSptM1N3BbrA6/kmaep5ggwaIA=",
        version = "v0.0.0-20170904204023-bfd46e6a821d",
    )

    go_repository(
        name = "com_github_phayes_freeport",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/phayes/freeport",
        sum = "h1:JhzVVoYvbOACxoUmOs6V/G4D5nPVUW73rKvXxP4XUJc=",
        version = "v0.0.0-20180830031419-95f893ade6f2",
    )
    go_repository(
        name = "com_github_phpdave11_gofpdf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/phpdave11/gofpdf",
        sha256 = "4db05258f281b40d8a17392fd71648779ea758a9aa506a8d1346ded737ede43f",
        strip_prefix = "github.com/phpdave11/gofpdf@v1.4.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/phpdave11/gofpdf/com_github_phpdave11_gofpdf-v1.4.2.zip",
            "http://ats.apps.svc/gomod/github.com/phpdave11/gofpdf/com_github_phpdave11_gofpdf-v1.4.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/phpdave11/gofpdf/com_github_phpdave11_gofpdf-v1.4.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/phpdave11/gofpdf/com_github_phpdave11_gofpdf-v1.4.2.zip",
        ],
    )
    go_repository(
        name = "com_github_phpdave11_gofpdi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/phpdave11/gofpdi",
        sha256 = "09b728136cf290f4ee87aa47b60f2f9df2b3f4f64119ff10f12319bc3438b58d",
        strip_prefix = "github.com/phpdave11/gofpdi@v1.0.13",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/phpdave11/gofpdi/com_github_phpdave11_gofpdi-v1.0.13.zip",
            "http://ats.apps.svc/gomod/github.com/phpdave11/gofpdi/com_github_phpdave11_gofpdi-v1.0.13.zip",
            "https://cache.hawkingrei.com/gomod/github.com/phpdave11/gofpdi/com_github_phpdave11_gofpdi-v1.0.13.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/phpdave11/gofpdi/com_github_phpdave11_gofpdi-v1.0.13.zip",
        ],
    )
    go_repository(
        name = "com_github_pierrec_lz4",
        build_file_proto_mode = "disable",
        importpath = "github.com/pierrec/lz4",
        sum = "h1:9UY3+iC23yxF0UfGaYrGplQ+79Rg+h/q9FV9ix19jjM=",
        version = "v2.6.1+incompatible",
    )

    go_repository(
        name = "com_github_pierrec_lz4_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pierrec/lz4/v4",
        sum = "h1:yOVMLb6qSIDP67pl/5F7RepeKYu/VmTyEXvuMI5d9mQ=",
        version = "v4.1.21",
    )
    go_repository(
        name = "com_github_pingcap_badger",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/badger",
        sum = "h1:MKVFZuqFvAMiDtv3AbihOQ6rY5IE8LWflI1BuZ/hF0Y=",
        version = "v1.5.1-0.20220314162537-ab58fbf40580",
    )
    go_repository(
        name = "com_github_pingcap_check",
        build_file_proto_mode = "disable",
        importpath = "github.com/pingcap/check",
        sum = "h1:R8gStypOBmpnHEx1qi//SaqxJVI4inOqljg/Aj5/390=",
        version = "v0.0.0-20200212061837-5e12011dc712",
    )

    go_repository(
        name = "com_github_pingcap_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/errors",
        sum = "h1:3Dm0DWeQlwV8LbpQxP2tojHhxd9aY59KI+QN0ns6bBo=",
        version = "v0.11.5-0.20220729040631-518f63d66278",
    )
    go_repository(
        name = "com_github_pingcap_failpoint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/failpoint",
        sum = "h1:kJolJWbyadVeL8RKBlqmXQR7FRKPsIeU85TUYyhbhiQ=",
        version = "v0.0.0-20220423142525-ae43b7f4e5c3",
    )
    go_repository(
        name = "com_github_pingcap_fn",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/fn",
        sum = "h1:Pe2LbxRmbTfAoKJ65bZLmhahmvHm7n9DUxGRQT00208=",
        version = "v0.0.0-20200306044125-d5540d389059",
    )
    go_repository(
        name = "com_github_pingcap_goleveldb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/goleveldb",
        sum = "h1:surzm05a8C9dN8dIUmo4Be2+pMRb6f55i+UIYrluu2E=",
        version = "v0.0.0-20191226122134-f82aafb29989",
    )
    go_repository(
        name = "com_github_pingcap_kvproto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/kvproto",
        sum = "h1:tBKPWWqgWEBs04BV4UN7RhtUkZDs0oz+WyMbtRDVtL8=",
        version = "v0.0.0-20230928035022-1bdcc25ed63c",
    )
    go_repository(
        name = "com_github_pingcap_log",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/log",
        sum = "h1:crhkw6DD+07Bg1wYhW5Piw+kYNKZqFQqfC2puUf6gMI=",
        version = "v1.1.1-0.20221116035753-734d527bc87c",
    )
    go_repository(
        name = "com_github_pingcap_sysutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/sysutil",
        sum = "h1:HYbcxtnkN3s5tqrZ/z3eJS4j3Db8wMphEm1q10lY/TM=",
        version = "v0.0.0-20220114020952-ea68d2dbf5b4",
    )
    go_repository(
        name = "com_github_pingcap_tipb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/tipb",
        sum = "h1:DbmCfCbcavo0JG+gSp0ySvv1ub/c/j3hsnYzyYPzONo=",
        version = "v0.0.0-20221123081521-2fb828910813",
    )
    go_repository(
        name = "com_github_pkg_browser",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/browser",
        sum = "h1:+mdjkGKdHQG3305AYmdv1U2eRNDiU2ErMBj1gwrq8eQ=",
        version = "v0.0.0-20240102092130-5ac0b6a4141c",
    )
    go_repository(
        name = "com_github_pkg_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/diff",
        sum = "h1:aoZm08cpOy4WuID//EZDgcC4zIxODThtZNPirFr42+A=",
        version = "v0.0.0-20210226163009-20ebb0f2a09e",
    )
    go_repository(
        name = "com_github_pkg_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/errors",
        sum = "h1:FEBLx1zS214owpjy7qsBeixbURkuhQAwrK5UwLGTwt4=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_github_pkg_profile",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/profile",
        sum = "h1:F++O52m40owAmADcojzM+9gyjmMOY/T4oYJkgFDH8RE=",
        version = "v1.2.1",
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
        sum = "h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_polyfloyd_go_errorlint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/polyfloyd/go-errorlint",
        sum = "h1:AHB5JRCjlmelh9RrLxT9sgzpalIwwq4hqE8EkwIwKdY=",
        version = "v1.0.5",
    )
    go_repository(
        name = "com_github_posener_complete",
        build_file_proto_mode = "disable",
        importpath = "github.com/posener/complete",
        sum = "h1:ccV59UEOTzVDnDUEFdT95ZzHVZ+5+158q8+SJb2QV5w=",
        version = "v1.1.1",
    )

    go_repository(
        name = "com_github_power_devops_perfstat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/power-devops/perfstat",
        sum = "h1:ncq/mPwQF4JjgDlrVEn3C11VoGHZN7m8qihwgMEtzYw=",
        version = "v0.0.0-20210106213030-5aafc221ea8c",
    )
    go_repository(
        name = "com_github_prashantv_gostub",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prashantv/gostub",
        sum = "h1:BTyx3RfQjRHnUWaGF9oQos79AlQ5k8WNktv7VGvVH4g=",
        version = "v1.1.0",
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
        sum = "h1:cxppBPuYhUnsO6yo/aoRol4L7q7UFfdm+bR9r+8l63Y=",
        version = "v1.20.5",
    )
    go_repository(
        name = "com_github_prometheus_client_model",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/client_model",
        sum = "h1:ZKSh/rekM+n3CeS952MLRAdFwIKqeY8b62p8ais2e9E=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_prometheus_common",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/common",
        sum = "h1:KEi6DK7lXW/m7Ig5i47x0vRzuBsHuvJdi5ee6Y3G1dc=",
        version = "v0.55.0",
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
        sha256 = "30605de7fc911194ff45289870b4a56d9da2aca9e8f60de810bc65af77cdd2cb",
        strip_prefix = "github.com/prometheus/exporter-toolkit@v0.11.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/prometheus/exporter-toolkit/com_github_prometheus_exporter_toolkit-v0.11.0.zip",
            "http://ats.apps.svc/gomod/github.com/prometheus/exporter-toolkit/com_github_prometheus_exporter_toolkit-v0.11.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/prometheus/exporter-toolkit/com_github_prometheus_exporter_toolkit-v0.11.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/prometheus/exporter-toolkit/com_github_prometheus_exporter_toolkit-v0.11.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_procfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/procfs",
        sum = "h1:YagwOFzUgYfKKHX6Dr+sHT7km/hxC76UB0learggepc=",
        version = "v0.15.1",
    )
    go_repository(
        name = "com_github_prometheus_prometheus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/prometheus",
        sum = "h1:3DyLm+sTAJkfLyR/1pJ3L+fU2lFufWbpcgMFlGtqeyA=",
        version = "v0.0.0-20190525122359-d20e84d0fb64",
    )
    go_repository(
        name = "com_github_prometheus_tsdb",
        build_file_proto_mode = "disable",
        importpath = "github.com/prometheus/tsdb",
        sum = "h1:w1tAGxsBMLkuGrFMhqgcCeBkM5d1YI24udArs+aASuQ=",
        version = "v0.8.0",
    )
    go_repository(
        name = "com_github_puerkitobio_purell",
        build_file_proto_mode = "disable",
        importpath = "github.com/PuerkitoBio/purell",
        sum = "h1:WEQqlqaGbrPkxLJWfBwQmfEAE1Z7ONdDLqrN38tNFfI=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_puerkitobio_urlesc",
        build_file_proto_mode = "disable",
        importpath = "github.com/PuerkitoBio/urlesc",
        sum = "h1:d+Bc7a5rLufV/sSk/8dngufqelfh6jnri85riMAaF/M=",
        version = "v0.0.0-20170810143723-de5bf2ad4578",
    )

    go_repository(
        name = "com_github_qri_io_jsonpointer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/qri-io/jsonpointer",
        sha256 = "6870d4b9fc5ac8efb9226447975fecfb07241133e23c7e661f5aac1a3088f338",
        strip_prefix = "github.com/qri-io/jsonpointer@v0.1.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/qri-io/jsonpointer/com_github_qri_io_jsonpointer-v0.1.1.zip",
            "http://ats.apps.svc/gomod/github.com/qri-io/jsonpointer/com_github_qri_io_jsonpointer-v0.1.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/qri-io/jsonpointer/com_github_qri_io_jsonpointer-v0.1.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/qri-io/jsonpointer/com_github_qri_io_jsonpointer-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_qri_io_jsonschema",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/qri-io/jsonschema",
        sha256 = "51305cc45fd383b24de94e2eb421ffba8d83679520c18348842c4255025c5940",
        strip_prefix = "github.com/qri-io/jsonschema@v0.2.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/qri-io/jsonschema/com_github_qri_io_jsonschema-v0.2.1.zip",
            "http://ats.apps.svc/gomod/github.com/qri-io/jsonschema/com_github_qri_io_jsonschema-v0.2.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/qri-io/jsonschema/com_github_qri_io_jsonschema-v0.2.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/qri-io/jsonschema/com_github_qri_io_jsonschema-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_quasilyte_go_ruleguard",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/go-ruleguard",
        sum = "h1:sd+abO1PEI9fkYennwzHn9kl3nqP6M5vE7FiOzZ+5CE=",
        version = "v0.3.18",
    )
    go_repository(
        name = "com_github_quasilyte_go_ruleguard_dsl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/go-ruleguard/dsl",
        sum = "h1:vNkC6fC6qMLzCOGbnIHOd5ixUGgTbp3Z4fGnUgULlDA=",
        version = "v0.3.21",
    )
    go_repository(
        name = "com_github_quasilyte_gogrep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/gogrep",
        sum = "h1:6Gtn2i04RD0gVyYf2/IUMTIs+qYleBt4zxDqkLTcu4U=",
        version = "v0.0.0-20220828223005-86e4605de09f",
    )
    go_repository(
        name = "com_github_quasilyte_regex_syntax",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/regex/syntax",
        sum = "h1:L8QM9bvf68pVdQ3bCFZMDmnt9yqcMBro1pC7F+IPYMY=",
        version = "v0.0.0-20200407221936-30656e2c4a95",
    )
    go_repository(
        name = "com_github_quasilyte_stdinfo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/stdinfo",
        sum = "h1:M8mH9eK4OUR4lu7Gd+PU1fV2/qnDNfzT635KRSObncs=",
        version = "v0.0.0-20220114132959-f7386bf02567",
    )
    go_repository(
        name = "com_github_raeperd_recvcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/raeperd/recvcheck",
        sha256 = "865384b04219bb139113a1108a467bc198b7a2dd02a8d830c4ab1a3abb0f4670",
        strip_prefix = "github.com/raeperd/recvcheck@v0.1.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/raeperd/recvcheck/com_github_raeperd_recvcheck-v0.1.2.zip",
            "http://ats.apps.svc/gomod/github.com/raeperd/recvcheck/com_github_raeperd_recvcheck-v0.1.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/raeperd/recvcheck/com_github_raeperd_recvcheck-v0.1.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/raeperd/recvcheck/com_github_raeperd_recvcheck-v0.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_rcrowley_go_metrics",
        build_file_proto_mode = "disable",
        importpath = "github.com/rcrowley/go-metrics",
        sum = "h1:N/ElC8H3+5XpJzTSTfLsJV/mx9Q9g7kxmchpfZyxgzM=",
        version = "v0.0.0-20201227073835-cf1acfcdf475",
    )
    go_repository(
        name = "com_github_redis_go_redis_v9",
        build_file_proto_mode = "disable",
        importpath = "github.com/redis/go-redis/v9",
        sum = "h1:HHDteefn6ZkTtY5fGUE8tj8uy85AHk6zP7CpzIAM0y4=",
        version = "v9.6.1",
    )

    go_repository(
        name = "com_github_remyoudompheng_bigfft",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/remyoudompheng/bigfft",
        sum = "h1:OdAsTTz6OkFY5QxjkYwrChwuRruF69c169dPK26NUlk=",
        version = "v0.0.0-20200410134404-eec4a21b6bb0",
    )
    go_repository(
        name = "com_github_rivo_uniseg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rivo/uniseg",
        sum = "h1:YwD0ulJSJytLpiaWua0sBDusfsCZohxjxzVTYjwxfV8=",
        version = "v0.4.2",
    )
    go_repository(
        name = "com_github_rlmcpherson_s3gof3r",
        build_file_proto_mode = "disable",
        importpath = "github.com/rlmcpherson/s3gof3r",
        sum = "h1:1izOJpTiohSibfOHuNyEA/yQnAirh05enzEdmhez43k=",
        version = "v0.5.0",
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
        sum = "h1:Ppwyp6VYCF1nvBTXL3trRso7mXMlRrw9ooo375wvi2s=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_rogpeppe_go_internal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rogpeppe/go-internal",
        sum = "h1:exVL4IDcn6na9z1rAb56Vxr+CgyK3nn3O+epU5NdKM8=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_github_rubyist_circuitbreaker",
        build_file_proto_mode = "disable",
        importpath = "github.com/rubyist/circuitbreaker",
        sum = "h1:KUKd/pV8Geg77+8LNDwdow6rVCAYOp8+kHUyFvL6Mhk=",
        version = "v2.2.1+incompatible",
    )

    go_repository(
        name = "com_github_russross_blackfriday",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/russross/blackfriday",
        sum = "h1:HyvC0ARfnZBqnXwABFeSZHpKvJHJJfPz81GNueLj0oo=",
        version = "v1.5.2",
    )
    go_repository(
        name = "com_github_russross_blackfriday_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/russross/blackfriday/v2",
        sum = "h1:JIOH55/0cWyOuilr9/qlrm0BSXldqnqwMsf35Ld67mk=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_ruudk_golang_pdf417",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ruudk/golang-pdf417",
        sha256 = "f0006c0f60789da76c1b3fef73bb63f5581744fbe3ab5973ec718b40c6822f69",
        strip_prefix = "github.com/ruudk/golang-pdf417@v0.0.0-20201230142125-a7e3863a1245",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/ruudk/golang-pdf417/com_github_ruudk_golang_pdf417-v0.0.0-20201230142125-a7e3863a1245.zip",
            "http://ats.apps.svc/gomod/github.com/ruudk/golang-pdf417/com_github_ruudk_golang_pdf417-v0.0.0-20201230142125-a7e3863a1245.zip",
            "https://cache.hawkingrei.com/gomod/github.com/ruudk/golang-pdf417/com_github_ruudk_golang_pdf417-v0.0.0-20201230142125-a7e3863a1245.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/ruudk/golang-pdf417/com_github_ruudk_golang_pdf417-v0.0.0-20201230142125-a7e3863a1245.zip",
        ],
    )
    go_repository(
        name = "com_github_ryancurrah_gomodguard",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryancurrah/gomodguard",
        sum = "h1:CpMSDKan0LtNGGhPrvupAoLeObRFjND8/tU1rEOtBp4=",
        version = "v1.2.4",
    )
    go_repository(
        name = "com_github_ryanrolds_sqlclosecheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryanrolds/sqlclosecheck",
        sum = "h1:AZx+Bixh8zdUBxUA1NxbxVAS78vTPq4rCb8OUZI9xFw=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_ryanuber_columnize",
        build_file_proto_mode = "disable",
        importpath = "github.com/ryanuber/columnize",
        sum = "h1:j1Wcmh8OrK4Q7GXY+V7SVSY8nUWQxHW5TkBe7YUl+2s=",
        version = "v2.1.0+incompatible",
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
        name = "com_github_samuel_go_zookeeper",
        build_file_proto_mode = "disable",
        importpath = "github.com/samuel/go-zookeeper",
        sum = "h1:4AQBn5RJY4WH8t8TLEMZUsWeXHAUcoao42TCAfpEJJE=",
        version = "v0.0.0-20161028232340-1d7be4effb13",
    )

    go_repository(
        name = "com_github_sanposhiho_wastedassign_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sanposhiho/wastedassign/v2",
        sum = "h1:+6/hQIHKNJAUixEj6EmOngGIisyeI+T3335lYTyxRoA=",
        version = "v2.0.6",
    )
    go_repository(
        name = "com_github_santhosh_tekuri_jsonschema_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/santhosh-tekuri/jsonschema/v5",
        sha256 = "6c953c3751cca3003d0e7f6d775c7c3b2e4b1eeb1fa2e8d68786ead53b083094",
        strip_prefix = "github.com/santhosh-tekuri/jsonschema/v5@v5.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/santhosh-tekuri/jsonschema/v5/com_github_santhosh_tekuri_jsonschema_v5-v5.3.1.zip",
            "http://ats.apps.svc/gomod/github.com/santhosh-tekuri/jsonschema/v5/com_github_santhosh_tekuri_jsonschema_v5-v5.3.1.zip",
            "https://cache.hawkingrei.com/gomod/github.com/santhosh-tekuri/jsonschema/v5/com_github_santhosh_tekuri_jsonschema_v5-v5.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/santhosh-tekuri/jsonschema/v5/com_github_santhosh_tekuri_jsonschema_v5-v5.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_sasha_s_go_deadlock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sasha-s/go-deadlock",
        sum = "h1:yVBZEAirqhDYAc7xftf/swe8eHcg63jqfwdqN8KSoR8=",
        version = "v0.0.0-20161201235124-341000892f3d",
    )
    go_repository(
        name = "com_github_sashamelentyev_interfacebloat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sashamelentyev/interfacebloat",
        sum = "h1:xdRdJp0irL086OyW1H/RTZTr1h/tMEOsumirXcOJqAw=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_sashamelentyev_usestdlibvars",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sashamelentyev/usestdlibvars",
        sum = "h1:K6CXjqqtSYSsuyRDDC7Sjn6vTMLiSJa4ZmDkiokoqtw=",
        version = "v1.20.0",
    )
    go_repository(
        name = "com_github_satori_go_uuid",
        build_file_proto_mode = "disable",
        importpath = "github.com/satori/go.uuid",
        sum = "h1:0uYX9dsZ2yD7q2RtLRtPSdGDWzjeM3TbMJP9utgA0ww=",
        version = "v1.2.0",
    )

    go_repository(
        name = "com_github_scaleway_scaleway_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/scaleway/scaleway-sdk-go",
        sha256 = "b7f9a702ee1e899d81bc23ca5e761cd2bc6c0202797d9b5193b83a50bad16698",
        strip_prefix = "github.com/scaleway/scaleway-sdk-go@v1.0.0-beta.22",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/scaleway/scaleway-sdk-go/com_github_scaleway_scaleway_sdk_go-v1.0.0-beta.22.zip",
            "http://ats.apps.svc/gomod/github.com/scaleway/scaleway-sdk-go/com_github_scaleway_scaleway_sdk_go-v1.0.0-beta.22.zip",
            "https://cache.hawkingrei.com/gomod/github.com/scaleway/scaleway-sdk-go/com_github_scaleway_scaleway_sdk_go-v1.0.0-beta.22.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/scaleway/scaleway-sdk-go/com_github_scaleway_scaleway_sdk_go-v1.0.0-beta.22.zip",
        ],
    )
    go_repository(
        name = "com_github_schollz_closestmatch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/schollz/closestmatch",
        sha256 = "f267729efc7a639bb816e2586a17237a8c8e7ff327c0c3dd58766d1433ad2d3a",
        strip_prefix = "github.com/schollz/closestmatch@v2.1.0+incompatible",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/schollz/closestmatch/com_github_schollz_closestmatch-v2.1.0+incompatible.zip",
            "http://ats.apps.svc/gomod/github.com/schollz/closestmatch/com_github_schollz_closestmatch-v2.1.0+incompatible.zip",
            "https://cache.hawkingrei.com/gomod/github.com/schollz/closestmatch/com_github_schollz_closestmatch-v2.1.0+incompatible.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/schollz/closestmatch/com_github_schollz_closestmatch-v2.1.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_sclevine_agouti",
        build_file_proto_mode = "disable",
        importpath = "github.com/sclevine/agouti",
        sum = "h1:8IBJS6PWz3uTlMP3YBIR5f+KAldcGuOeFkFbUWfBgK4=",
        version = "v3.0.0+incompatible",
    )
    go_repository(
        name = "com_github_sean_seed",
        build_file_proto_mode = "disable",
        importpath = "github.com/sean-/seed",
        sum = "h1:nn5Wsu0esKSJiIVhscUtVbo7ada43DJhG55ua/hjS5I=",
        version = "v0.0.0-20170313163322-e2103e2c3529",
    )

    go_repository(
        name = "com_github_securego_gosec_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/securego/gosec/v2",
        sum = "h1:7mU32qn2dyC81MH9L2kefnQyRMUarfDER3iQyMHcjYM=",
        version = "v2.13.1",
    )
    go_repository(
        name = "com_github_segmentio_asm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/segmentio/asm",
        sum = "h1:9BQrFxC+YOHJlTlHGkTrFWf59nbL3XnCoFLTwDCI7ys=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_sergi_go_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sergi/go-diff",
        sum = "h1:we8PVUC3FE2uYfodKH/nBHMSetSfHDR6scGdBi+erh0=",
        version = "v1.1.0",
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
        sum = "h1:W65qqJCIOVP4jpqPQ0YvHYKwcMEMVWIzWC5iNQQfBTU=",
        version = "v0.0.0-20160112020656-b6b7b6733b8c",
    )
    go_repository(
        name = "com_github_shirou_gopsutil_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shirou/gopsutil/v3",
        sum = "h1:yibtJhIVEMcdw+tCTbOPiF1VcsuDeTE4utJ8Dm4c5eA=",
        version = "v3.22.9",
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
        sum = "h1:WDC6ySpJzbxGWFh4aMxFFC28wwGp5pEuoTtvA4q/qQ4=",
        version = "v0.0.0-20181106222321-ec9c9a553398",
    )
    go_repository(
        name = "com_github_shopify_sarama",
        build_file_proto_mode = "disable",
        importpath = "github.com/Shopify/sarama",
        sum = "h1:ARid8o8oieau9XrHI55f/L3EoRAhm9px6sonbD7yuUE=",
        version = "v1.29.0",
    )
    go_repository(
        name = "com_github_shopify_toxiproxy",
        build_file_proto_mode = "disable",
        importpath = "github.com/Shopify/toxiproxy",
        sum = "h1:TKdv8HiTLgE5wdJuEML90aBgNWsokNbMijUGhmcoBJc=",
        version = "v2.1.4+incompatible",
    )

    go_repository(
        name = "com_github_shopspring_decimal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shopspring/decimal",
        sum = "h1:pntxY8Ary0t43dCZ5dqY4YTJCObLY1kIXl0uzMv+7DE=",
        version = "v0.0.0-20180709203117-cd690d0c9e24",
    )
    go_repository(
        name = "com_github_shurcool_httpfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/httpfs",
        sum = "h1:bUGsEnyNbVPw06Bs80sCeARAlK8lhwqGyi6UT8ymuGk=",
        version = "v0.0.0-20190707220628-8d4bc4ba7749",
    )
    go_repository(
        name = "com_github_shurcool_httpgzip",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/httpgzip",
        sum = "h1:mj/nMDAwTBiaCqMEs4cYCqF7pO6Np7vhy1D1wcQGz+E=",
        version = "v0.0.0-20190720172056-320755c1c1b0",
    )
    go_repository(
        name = "com_github_shurcool_sanitized_anchor_name",
        build_file_proto_mode = "disable",
        importpath = "github.com/shurcooL/sanitized_anchor_name",
        sum = "h1:PdmoCO6wvbs+7yrJyMORt4/BmY5IYyJwS/kOiWx8mHo=",
        version = "v1.0.0",
    )

    go_repository(
        name = "com_github_shurcool_vfsgen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/vfsgen",
        sum = "h1:ug7PpSOB5RBPK1Kg6qskGBoP3Vnj/aNYFTznWvlkGo0=",
        version = "v0.0.0-20181202132449-6a9ea43bcacd",
    )
    go_repository(
        name = "com_github_sirupsen_logrus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sirupsen/logrus",
        sum = "h1:trlNQbNUG3OdDrDil03MCb1H2o9nJ1x4/5LYw7byDE0=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_github_sivchari_containedctx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sivchari/containedctx",
        sum = "h1:0hLQKpgC53OVF1VT7CeoFHk9YKstur1XOgfYIc1yrHI=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_sivchari_nosnakecase",
        build_file_proto_mode = "disable",
        importpath = "github.com/sivchari/nosnakecase",
        sum = "h1:7QkpWIRMe8x25gckkFd2A5Pi6Ymo0qgr4JrhGt95do8=",
        version = "v1.7.0",
    )

    go_repository(
        name = "com_github_sivchari_tenv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sivchari/tenv",
        sum = "h1:d4laZMBK6jpe5PWepxlV9S+LC0yXqvYHiq8E6ceoVVE=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_smartystreets_assertions",
        build_file_proto_mode = "disable",
        importpath = "github.com/smartystreets/assertions",
        sum = "h1:zE9ykElWQ6/NYmHa3jpm/yHnI4xSofP+UP6SpjHcSeM=",
        version = "v0.0.0-20180927180507-b2de0cb4f26d",
    )
    go_repository(
        name = "com_github_smartystreets_goconvey",
        build_file_proto_mode = "disable",
        importpath = "github.com/smartystreets/goconvey",
        sum = "h1:fv0U8FUIMPNf1L9lnHLvLhgicrIVChEkdzIKYqbNC9s=",
        version = "v1.6.4",
    )

    go_repository(
        name = "com_github_soheilhy_cmux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/soheilhy/cmux",
        sum = "h1:jjzc5WVemNEDTLwv9tlmemhC73tI08BNOIGwBOo10Js=",
        version = "v0.1.5",
    )
    go_repository(
        name = "com_github_sonatard_noctx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sonatard/noctx",
        sum = "h1:VC1Qhl6Oxx9vvWo3UDgrGXYCeKCe3Wbw7qAWL6FrmTY=",
        version = "v0.0.1",
    )
    go_repository(
        name = "com_github_sourcegraph_go_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sourcegraph/go-diff",
        sum = "h1:hmA1LzxW0n1c3Q4YbrFgg4P99GSnebYa3x8gr0HZqLQ=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_spaolacci_murmur3",
        build_file_proto_mode = "disable",
        importpath = "github.com/spaolacci/murmur3",
        sum = "h1:7c1g84S4BPRrfL5Xrdp6fOJ206sU9y293DDHaoy0bLI=",
        version = "v1.1.0",
    )

    go_repository(
        name = "com_github_spf13_afero",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/afero",
        sum = "h1:xehSyVa0YnHWsJ49JFljMpg1HX19V6NDZ1fkm1Xznbo=",
        version = "v1.8.2",
    )
    go_repository(
        name = "com_github_spf13_cast",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/cast",
        sum = "h1:rj3WzYc11XZaIZMPKmwP96zkFEnnAmV8s6XbB2aY32w=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_spf13_cobra",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/cobra",
        sum = "h1:o94oiPyS4KD1mPy2fmcYYHHfCxLqYjJOhGsCHFZtEzA=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_github_spf13_jwalterweatherman",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/jwalterweatherman",
        sum = "h1:ue6voC5bR5F8YxI5S67j9i582FU4Qvo2bmqnqMYADFk=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_spf13_pflag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/pflag",
        sum = "h1:iy+VFUOCP1a+8yFto/drg2CJ5u0yRoB7fZw3DKv/JXA=",
        version = "v1.0.5",
    )
    go_repository(
        name = "com_github_spf13_viper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/viper",
        sum = "h1:CZ7eSOd3kZoaYDLbXnmzgQI5RlciuXBMA+18HwHRfZQ=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_github_spkg_bom",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spkg/bom",
        sum = "h1:S939THe0ukL5WcTGiGqkgtaW5JW+O6ITaIlpJXTYY64=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_ssgreg_nlreturn_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ssgreg/nlreturn/v2",
        sum = "h1:X4XDI7jstt3ySqGU86YGAURbxw3oTDPK9sPEi6YEwQ0=",
        version = "v2.2.1",
    )
    go_repository(
        name = "com_github_stackexchange_wmi",
        build_file_proto_mode = "disable",
        importpath = "github.com/StackExchange/wmi",
        sum = "h1:5ZfJxyXo8KyX8DgGXC5B7ILL8y51fci/qYz2B4j8iLY=",
        version = "v0.0.0-20180725035823-b12b22c5341f",
    )

    go_repository(
        name = "com_github_stathat_consistent",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stathat/consistent",
        sum = "h1:ZFJ1QTRn8npNBKW065raSZ8xfOqhpb8vLOkfp4CcL/U=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_stbenjam_no_sprintf_host_port",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stbenjam/no-sprintf-host-port",
        sum = "h1:tYugd/yrm1O0dV+ThCbaKZh195Dfm07ysF0U6JQXczc=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_stoewer_go_strcase",
        build_file_proto_mode = "disable",
        importpath = "github.com/stoewer/go-strcase",
        sum = "h1:Z2iHWqGXH00XYgqDmNgQbIBxf3wrNq0F3feEy0ainaU=",
        version = "v1.2.0",
    )

    go_repository(
        name = "com_github_stretchr_objx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stretchr/objx",
        sum = "h1:xuMeJ0Sdp5ZMRXx/aWO6RZxdr3beISkG5/G/aIRr3pY=",
        version = "v0.5.2",
    )
    go_repository(
        name = "com_github_stretchr_testify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stretchr/testify",
        sum = "h1:HtqpIVDClZ4nwg75+f6Lvsy/wHu+3BoSGCbBAcpTsTg=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_github_subosito_gotenv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/subosito/gotenv",
        sum = "h1:jyEFiXpy21Wm81FBN71l9VoMMV8H8jG+qIK3GCpY6Qs=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_github_tdakkota_asciicheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tdakkota/asciicheck",
        sum = "h1:PKzG7JUTUmVspQTDqtkX9eSiLGossXTybutHwTXuO0A=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_tdewolff_minify_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tdewolff/minify/v2",
        sha256 = "6f76f152c15fee3a36b0496175d7e075046c3b47b50327428b10d32af6549f5f",
        strip_prefix = "github.com/tdewolff/minify/v2@v2.12.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tdewolff/minify/v2/com_github_tdewolff_minify_v2-v2.12.4.zip",
            "http://ats.apps.svc/gomod/github.com/tdewolff/minify/v2/com_github_tdewolff_minify_v2-v2.12.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tdewolff/minify/v2/com_github_tdewolff_minify_v2-v2.12.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tdewolff/minify/v2/com_github_tdewolff_minify_v2-v2.12.4.zip",
        ],
    )
    go_repository(
        name = "com_github_tdewolff_parse_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tdewolff/parse/v2",
        sha256 = "5bfdded67b0164d0fbfc8c5d308a4c9c2f5ebecdcf3e769b5e9ca8586335c543",
        strip_prefix = "github.com/tdewolff/parse/v2@v2.6.4",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/tdewolff/parse/v2/com_github_tdewolff_parse_v2-v2.6.4.zip",
            "http://ats.apps.svc/gomod/github.com/tdewolff/parse/v2/com_github_tdewolff_parse_v2-v2.6.4.zip",
            "https://cache.hawkingrei.com/gomod/github.com/tdewolff/parse/v2/com_github_tdewolff_parse_v2-v2.6.4.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/tdewolff/parse/v2/com_github_tdewolff_parse_v2-v2.6.4.zip",
        ],
    )
    go_repository(
        name = "com_github_tenntenn_modver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tenntenn/modver",
        sum = "h1:2klLppGhDgzJrScMpkj9Ujy3rXPUspSjAcev9tSEBgA=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_tenntenn_text_transform",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tenntenn/text/transform",
        sum = "h1:f+jULpRQGxTSkNYKJ51yaw6ChIqO+Je8UqsTKN/cDag=",
        version = "v0.0.0-20200319021203-7eef512accb3",
    )
    go_repository(
        name = "com_github_tetafro_godot",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tetafro/godot",
        sum = "h1:BVoBIqAf/2QdbFmSwAWnaIqDivZdOV0ZRwEm6jivLKw=",
        version = "v1.4.11",
    )
    go_repository(
        name = "com_github_tiancaiamao_appdash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tiancaiamao/appdash",
        sum = "h1:mbAskLJ0oJfDRtkanvQPiooDH8HvJ2FBh+iKT/OmiQQ=",
        version = "v0.0.0-20181126055449-889f96f722a2",
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
        sum = "h1:gj/NorS/bO3Y7zi9KS09s7VyinfOZHxpQl0gxmbJ1aI=",
        version = "v2.0.4-0.20231121073938-194639470f84",
    )
    go_repository(
        name = "com_github_tikv_pd_client",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tikv/pd/client",
        sum = "h1:e4hLUKfgfPeJPZwOfU+/I/03G0sn6IZqVcbX/5o+hvM=",
        version = "v0.0.0-20230904040343-947701a32c05",
    )
    go_repository(
        name = "com_github_timakin_bodyclose",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/timakin/bodyclose",
        sum = "h1:kl4KhGNsJIbDHS9/4U9yQo1UcPQM0kOMJHn29EoH/Ro=",
        version = "v0.0.0-20210704033933-f49887972144",
    )
    go_repository(
        name = "com_github_timonwong_loggercheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/timonwong/loggercheck",
        sum = "h1:ecACo9fNiHxX4/Bc02rW2+kaJIAMAes7qJ7JKxt0EZI=",
        version = "v0.9.3",
    )
    go_repository(
        name = "com_github_tklauser_go_sysconf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tklauser/go-sysconf",
        sum = "h1:IJ1AZGZRWbY8T5Vfk04D9WOA5WSejdflXxP03OUqALw=",
        version = "v0.3.10",
    )
    go_repository(
        name = "com_github_tklauser_numcpus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tklauser/numcpus",
        sum = "h1:E53Dm1HjH1/R2/aoCtXtPgzmElmn51aOkhCFSuZq//o=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_tmc_grpc_websocket_proxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tmc/grpc-websocket-proxy",
        sum = "h1:uruHq4dN7GR16kFc5fp3d1RIYzJW5onx8Ybykw2YQFA=",
        version = "v0.0.0-20201229170055-e5319fda7802",
    )
    go_repository(
        name = "com_github_tomarrell_wrapcheck_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tomarrell/wrapcheck/v2",
        sum = "h1:J/F8DbSKJC83bAvC6FoZaRjZiZ/iKoueSdrEkmGeacA=",
        version = "v2.7.0",
    )
    go_repository(
        name = "com_github_tommy_muehle_go_mnd_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tommy-muehle/go-mnd/v2",
        sum = "h1:NowYhSdyE/1zwK9QCLeRb6USWdoif80Ie+v+yU8u1Zw=",
        version = "v2.5.1",
    )
    go_repository(
        name = "com_github_twmb_murmur3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/twmb/murmur3",
        sum = "h1:D83U0XYKcHRYwYIpBKf3Pks91Z0Byda/9SJ8B6EMRcA=",
        version = "v1.1.3",
    )
    go_repository(
        name = "com_github_uber_jaeger_client_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber/jaeger-client-go",
        sum = "h1:NHcubEkVbahf9t3p75TOCR83gdUHXjRJvjoBh1yACsM=",
        version = "v2.22.1+incompatible",
    )
    go_repository(
        name = "com_github_uber_jaeger_lib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber/jaeger-lib",
        sum = "h1:td4jdvLcExb4cBISKIpHuGoVXh+dVKhn2Um6rjCsSsg=",
        version = "v2.4.1+incompatible",
    )
    go_repository(
        name = "com_github_ugorji_go",
        build_file_proto_mode = "disable",
        importpath = "github.com/ugorji/go",
        sum = "h1:j4s+tAvLfL3bZyefP2SEWmhBzmuIlH/eqNuPdFPgngw=",
        version = "v1.1.4",
    )

    go_repository(
        name = "com_github_ugorji_go_codec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ugorji/go/codec",
        sum = "h1:3SVOIvH7Ae1KRYyQWRjXWJEA9sS/c/pjvH++55Gr648=",
        version = "v0.0.0-20181204163529-d75b2dcb6bc8",
    )
    go_repository(
        name = "com_github_ultraware_funlen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ultraware/funlen",
        sum = "h1:5ylVWm8wsNwH5aWo9438pwvsK0QiqVuUrt9bn7S/iLA=",
        version = "v0.0.3",
    )
    go_repository(
        name = "com_github_ultraware_whitespace",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ultraware/whitespace",
        sum = "h1:hh+/cpIcopyMYbZNVov9iSxvJU3OYQg78Sfaqzi/CzI=",
        version = "v0.0.5",
    )
    go_repository(
        name = "com_github_urfave_negroni",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/urfave/negroni",
        sum = "h1:kIimOitoypq34K7TG7DUaJ9kq/N4Ofuwi1sjz0KipXc=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_uudashr_gocognit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uudashr/gocognit",
        sum = "h1:2Cgi6MweCsdB6kpcVQp7EW4U23iBFQWfTXiWlyp842Y=",
        version = "v1.0.6",
    )
    go_repository(
        name = "com_github_valyala_bytebufferpool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/bytebufferpool",
        sum = "h1:GqA5TC/0021Y/b9FG4Oi9Mr3q7XYx6KllzawFIhcdPw=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_valyala_fasthttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/fasthttp",
        sum = "h1:uWF8lgKmeaIewWVPwi4GRq2P6+R46IgYZdxWtM+GtEY=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_valyala_fasttemplate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/fasttemplate",
        sum = "h1:tY9CJiPnMXf1ERmG2EyK7gNUd+c6RKGD0IfU8WdUSz8=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_valyala_quicktemplate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/quicktemplate",
        sum = "h1:LUPTJmlVcb46OOUY3IeD9DojFpAVbsG+5WFTcjMJzCM=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_valyala_tcplisten",
        build_file_proto_mode = "disable",
        importpath = "github.com/valyala/tcplisten",
        sum = "h1:0R4NLDRDZX6JcmhJgXi5E4b8Wg84ihbmUKp/GvSPEzc=",
        version = "v0.0.0-20161114210144-ceec8f93295a",
    )

    go_repository(
        name = "com_github_vbauerster_mpb_v7",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vbauerster/mpb/v7",
        sum = "h1:BkGfmb6nMrrBQDFECR/Q7RkKCw7ylMetCb4079CGs4w=",
        version = "v7.5.3",
    )
    go_repository(
        name = "com_github_vividcortex_ewma",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/VividCortex/ewma",
        sum = "h1:f58SaIzcDXrSy3kWaHNvuJgJ3Nmz59Zji6XoJR/q1ow=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_vmihailenco_msgpack_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vmihailenco/msgpack/v5",
        sha256 = "3437e7dc0e9a55985c6a68b4a331e685f1125aeb98a0cec0585145b8353a66ae",
        strip_prefix = "github.com/vmihailenco/msgpack/v5@v5.3.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/vmihailenco/msgpack/v5/com_github_vmihailenco_msgpack_v5-v5.3.5.zip",
            "http://ats.apps.svc/gomod/github.com/vmihailenco/msgpack/v5/com_github_vmihailenco_msgpack_v5-v5.3.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/vmihailenco/msgpack/v5/com_github_vmihailenco_msgpack_v5-v5.3.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/vmihailenco/msgpack/v5/com_github_vmihailenco_msgpack_v5-v5.3.5.zip",
        ],
    )
    go_repository(
        name = "com_github_vmihailenco_tagparser_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vmihailenco/tagparser/v2",
        sha256 = "70096ead331b4ac4efc0bf740674cbe55772beee6eace39507a610c5652aa8b5",
        strip_prefix = "github.com/vmihailenco/tagparser/v2@v2.0.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/vmihailenco/tagparser/v2/com_github_vmihailenco_tagparser_v2-v2.0.0.zip",
            "http://ats.apps.svc/gomod/github.com/vmihailenco/tagparser/v2/com_github_vmihailenco_tagparser_v2-v2.0.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/vmihailenco/tagparser/v2/com_github_vmihailenco_tagparser_v2-v2.0.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/vmihailenco/tagparser/v2/com_github_vmihailenco_tagparser_v2-v2.0.0.zip",
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
        sum = "h1:9DDCDwOyEy/gId+IEMrFHLuQ5R/WV0KNxWLler8X2OY=",
        version = "v0.0.0-20161129230411-ed8402a42d5f",
    )
    go_repository(
        name = "com_github_xdg_scram",
        build_file_proto_mode = "disable",
        importpath = "github.com/xdg/scram",
        sum = "h1:nTadYh2Fs4BK2xdldEa2g5bbaZp0/+1nJMMPtPxS/to=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_xdg_stringprep",
        build_file_proto_mode = "disable",
        importpath = "github.com/xdg/stringprep",
        sum = "h1:cmL5Enob4W83ti/ZHuZLuKD/xqJfus4fVPwE+/BDm+4=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonpointer",
        build_file_proto_mode = "disable",
        importpath = "github.com/xeipuuv/gojsonpointer",
        sum = "h1:J9EGpcZtP0E/raorCMxlFGSTBrsSlaDGf3jU/qvAE2c=",
        version = "v0.0.0-20180127040702-4e3ac2762d5f",
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonreference",
        build_file_proto_mode = "disable",
        importpath = "github.com/xeipuuv/gojsonreference",
        sum = "h1:EzJWgHovont7NscjpAxXsDA8S8BMYve8Y5+7cuRE7R0=",
        version = "v0.0.0-20180127040603-bd5ef7bd5415",
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonschema",
        build_file_proto_mode = "disable",
        importpath = "github.com/xeipuuv/gojsonschema",
        sum = "h1:LhYJRs+L4fBtjZUfuSZIKGeVu0QRy8e5Xi7D17UxZ74=",
        version = "v1.2.0",
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
        sum = "h1:lxklc02Drh6ynqX+DdPyp5pCKLUQpRT8bp8Ydu2Bstc=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_xiang90_probing",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xiang90/probing",
        sum = "h1:eY9dn8+vbi4tKz5Qo6v2eYzo7kUS51QINcR5jNpbZS8=",
        version = "v0.0.0-20190116061207-43a291ad63a2",
    )
    go_repository(
        name = "com_github_xitongsys_parquet_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xitongsys/parquet-go",
        sum = "h1:tBbuFCtyJNKT+BFAv6qjvTFpVdy97IYNaBwGUXifIUs=",
        version = "v1.5.5-0.20201110004701-b09c49d6d457",
    )
    go_repository(
        name = "com_github_xitongsys_parquet_go_source",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xitongsys/parquet-go-source",
        sum = "h1:a742S4V5A15F93smuVxA60LQWsrCnN8bKeWDBARU1/k=",
        version = "v0.0.0-20200817004010-026bad9b25d0",
    )
    go_repository(
        name = "com_github_xordataexchange_crypt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xordataexchange/crypt",
        sum = "h1:ESFSdwYZvkeru3RtdrYueztKhOBCSAAzS4Gf+k0tEow=",
        version = "v0.0.3-0.20170626215501-b2862e3d0a77",
    )
    go_repository(
        name = "com_github_yagipy_maintidx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yagipy/maintidx",
        sum = "h1:h5NvIsCz+nRDapQ0exNv4aJ0yXSI0420omVANTv3GJM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_yalp_jsonpath",
        build_file_proto_mode = "disable",
        importpath = "github.com/yalp/jsonpath",
        sum = "h1:6fRhSjgLCkTD3JnJxvaJ4Sj+TYblw757bqYgZaOq5ZY=",
        version = "v0.0.0-20180802001716-5cc68e5049a0",
    )

    go_repository(
        name = "com_github_yangkeao_go_mysql_driver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/YangKeao/go-mysql-driver",
        sha256 = "66ba9bed8b68899ea4adc729fbaf160bd634fe1afa51621a3dc5b153b538eb57",
        strip_prefix = "github.com/YangKeao/go-mysql-driver@v0.0.0-20240627104025-dd5589458cfa",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/YangKeao/go-mysql-driver/com_github_yangkeao_go_mysql_driver-v0.0.0-20240627104025-dd5589458cfa.zip",
            "http://ats.apps.svc/gomod/github.com/YangKeao/go-mysql-driver/com_github_yangkeao_go_mysql_driver-v0.0.0-20240627104025-dd5589458cfa.zip",
            "https://cache.hawkingrei.com/gomod/github.com/YangKeao/go-mysql-driver/com_github_yangkeao_go_mysql_driver-v0.0.0-20240627104025-dd5589458cfa.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/YangKeao/go-mysql-driver/com_github_yangkeao_go_mysql_driver-v0.0.0-20240627104025-dd5589458cfa.zip",
        ],
    )
    go_repository(
        name = "com_github_yeya24_promlinter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yeya24/promlinter",
        sum = "h1:xFKDQ82orCU5jQujdaD8stOHiv8UN68BSdn2a8u8Y3o=",
        version = "v0.2.0",
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
        name = "com_github_yosssi_ace",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yosssi/ace",
        sha256 = "96157dbef72f2f69a900e09b3e58093ee24f7df341ac287bddfb15f8c3f530db",
        strip_prefix = "github.com/yosssi/ace@v0.0.5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/yosssi/ace/com_github_yosssi_ace-v0.0.5.zip",
            "http://ats.apps.svc/gomod/github.com/yosssi/ace/com_github_yosssi_ace-v0.0.5.zip",
            "https://cache.hawkingrei.com/gomod/github.com/yosssi/ace/com_github_yosssi_ace-v0.0.5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/yosssi/ace/com_github_yosssi_ace-v0.0.5.zip",
        ],
    )
    go_repository(
        name = "com_github_yudai_gojsondiff",
        build_file_proto_mode = "disable",
        importpath = "github.com/yudai/gojsondiff",
        sum = "h1:27cbfqXLVEJ1o8I6v3y9lg8Ydm53EKqHXAOMxEGlCOA=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_yudai_golcs",
        build_file_proto_mode = "disable",
        importpath = "github.com/yudai/golcs",
        sum = "h1:BHyfKlQyqbsFN5p3IfnEUduWvb9is428/nNb5L3U01M=",
        version = "v0.0.0-20170316035057-ecda9a501e82",
    )
    go_repository(
        name = "com_github_yudai_pp",
        build_file_proto_mode = "disable",
        importpath = "github.com/yudai/pp",
        sum = "h1:Q4//iY4pNF6yPLZIigmvcl7k/bPgrcTPIFIcmawg5bI=",
        version = "v2.0.1+incompatible",
    )

    go_repository(
        name = "com_github_yuin_goldmark",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yuin/goldmark",
        sum = "h1:fVcFKWvrslecOb/tg+Cc05dkeYx540o0FuFt3nUVDoE=",
        version = "v1.4.13",
    )
    go_repository(
        name = "com_github_yusufpapurcu_wmi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yusufpapurcu/wmi",
        sum = "h1:KBNDSne4vP5mbSWnJbO+51IMOXJB67QiYCSBrubbPRg=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_zeebo_assert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/zeebo/assert",
        sha256 = "1f01421d74ff37cb8247988155be9e6877d336029bcd887a1d035fd32d7ab6ae",
        strip_prefix = "github.com/zeebo/assert@v1.3.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/zeebo/assert/com_github_zeebo_assert-v1.3.0.zip",
            "http://ats.apps.svc/gomod/github.com/zeebo/assert/com_github_zeebo_assert-v1.3.0.zip",
            "https://cache.hawkingrei.com/gomod/github.com/zeebo/assert/com_github_zeebo_assert-v1.3.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/zeebo/assert/com_github_zeebo_assert-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_zeebo_xxh3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/zeebo/xxh3",
        sha256 = "190e5ef1f672e9321a1580bdd31c6440fde6044ca8168d2b489cf50cdc4f58a6",
        strip_prefix = "github.com/zeebo/xxh3@v1.0.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/github.com/zeebo/xxh3/com_github_zeebo_xxh3-v1.0.2.zip",
            "http://ats.apps.svc/gomod/github.com/zeebo/xxh3/com_github_zeebo_xxh3-v1.0.2.zip",
            "https://cache.hawkingrei.com/gomod/github.com/zeebo/xxh3/com_github_zeebo_xxh3-v1.0.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/github.com/zeebo/xxh3/com_github_zeebo_xxh3-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_gitlab_bosi_decorder",
        build_file_proto_mode = "disable_global",
        importpath = "gitlab.com/bosi/decorder",
        sum = "h1:gX4/RgK16ijY8V+BRQHAySfQAb354T7/xQpDB2n10P0=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_google_cloud_go",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go",
        sum = "h1:rJyC7nWRg2jWGZ4wSJ5nY65GTdYJkg0cd/uXb+ACI6o=",
        version = "v0.110.7",
    )
    go_repository(
        name = "com_google_cloud_go_accessapproval",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/accessapproval",
        sum = "h1:/5YjNhR6lzCvmJZAnByYkfEgWjfAKwYP6nkuTk6nKFE=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_accesscontextmanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/accesscontextmanager",
        sum = "h1:WIAt9lW9AXtqw/bnvrEUaE8VG/7bAAeMzRCBGMkc4+w=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_aiplatform",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/aiplatform",
        sum = "h1:M5davZWCTzE043rJCn+ZLW6hSxfG1KAx4vJTtas2/ec=",
        version = "v1.48.0",
    )
    go_repository(
        name = "com_google_cloud_go_analytics",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/analytics",
        sum = "h1:TFBC1ZAqX9/jL56GEXdLrVe5vT3I22bDVWyDwZX4IEg=",
        version = "v0.21.3",
    )
    go_repository(
        name = "com_google_cloud_go_apigateway",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigateway",
        sum = "h1:aBSwCQPcp9rZ0zVEUeJbR623palnqtvxJlUyvzsKGQc=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeconnect",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigeeconnect",
        sum = "h1:6u/jj0P2c3Mcm+H9qLsXI7gYcTiG9ueyQL3n6vCmFJM=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeregistry",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigeeregistry",
        sum = "h1:hgq0ANLDx7t2FDZDJQrCMtCtddR/pjCqVuvQWGrQbXw=",
        version = "v0.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_appengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/appengine",
        sum = "h1:J+aaUZ6IbTpBegXbmEsh8qZZy864ZVnOoWyfa1XSNbI=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_area120",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/area120",
        sum = "h1:wiOq3KDpdqXmaHzvZwKdpoM+3lDcqsI2Lwhyac7stss=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_artifactregistry",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/artifactregistry",
        sum = "h1:k6hNqab2CubhWlGcSzunJ7kfxC7UzpAfQ1UPb9PDCKI=",
        version = "v1.14.1",
    )
    go_repository(
        name = "com_google_cloud_go_asset",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/asset",
        sum = "h1:vlHdznX70eYW4V1y1PxocvF6tEwxJTTarwIGwOhFF3U=",
        version = "v1.14.1",
    )
    go_repository(
        name = "com_google_cloud_go_assuredworkloads",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/assuredworkloads",
        sum = "h1:yaO0kwS+SnhVSTF7BqTyVGt3DTocI6Jqo+S3hHmCwNk=",
        version = "v1.11.1",
    )
    go_repository(
        name = "com_google_cloud_go_automl",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/automl",
        sum = "h1:iP9iQurb0qbz+YOOMfKSEjhONA/WcoOIjt6/m+6pIgo=",
        version = "v1.13.1",
    )
    go_repository(
        name = "com_google_cloud_go_baremetalsolution",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/baremetalsolution",
        sum = "h1:0Ge9PQAy6cZ1tRrkc44UVgYV15nw2TVnzJzYsMHXF+E=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_google_cloud_go_batch",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/batch",
        sum = "h1:uE0Q//W7FOGPjf7nuPiP0zoE8wOT3ngoIO2HIet0ilY=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_google_cloud_go_beyondcorp",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/beyondcorp",
        sum = "h1:VPg+fZXULQjs8LiMeWdLaB5oe8G9sEoZ0I0j6IMiG1Q=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/bigquery",
        sum = "h1:K3wLbjbnSlxhuG5q4pntHv5AEbQM1QqHKGYgwFIqOTg=",
        version = "v1.53.0",
    )
    go_repository(
        name = "com_google_cloud_go_billing",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/billing",
        sum = "h1:1iktEAIZ2uA6KpebC235zi/rCXDdDYQ0bTXTNetSL80=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_google_cloud_go_binaryauthorization",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/binaryauthorization",
        sum = "h1:cAkOhf1ic92zEN4U1zRoSupTmwmxHfklcp1X7CCBKvE=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_certificatemanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/certificatemanager",
        sum = "h1:uKsohpE0hiobx1Eak9jNcPCznwfB6gvyQCcS28Ah9E8=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_channel",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/channel",
        sum = "h1:dqRkK2k7Ll/HHeYGxv18RrfhozNxuTJRkspW0iaFZoY=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_google_cloud_go_cloudbuild",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/cloudbuild",
        sum = "h1:YBbAWcvE4x6xPWTyS+OU4eiUpz5rCS3VCM/aqmfddPA=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_clouddms",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/clouddms",
        sum = "h1:rjR1nV6oVf2aNNB7B5uz1PDIlBjlOiBgR+q5n7bbB7M=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_cloudtasks",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/cloudtasks",
        sum = "h1:cMh9Q6dkvh+Ry5LAPbD/U2aw6KAqdiU6FttwhbTo69w=",
        version = "v1.12.1",
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/compute",
        sum = "h1:tP41Zoavr8ptEqaW6j+LQOnyBBhO7OkOMAGrgLopTwY=",
        version = "v1.23.0",
    )
    go_repository(
        name = "com_google_cloud_go_compute_metadata",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/compute/metadata",
        sum = "h1:Tz+eQXMEqDIKRsmY3cHTL6FVaynIjX2QxYC4trgAKZc=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_contactcenterinsights",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/contactcenterinsights",
        sum = "h1:YR2aPedGVQPpFBZXJnPkqRj8M//8veIZZH5ZvICoXnI=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_container",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/container",
        sum = "h1:N51t/cgQJFqDD/W7Mb+IvmAPHrf8AbPx7Bb7aF4lROE=",
        version = "v1.24.0",
    )
    go_repository(
        name = "com_google_cloud_go_containeranalysis",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/containeranalysis",
        sum = "h1:SM/ibWHWp4TYyJMwrILtcBtYKObyupwOVeceI9pNblw=",
        version = "v0.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_datacatalog",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datacatalog",
        sum = "h1:qVeQcw1Cz93/cGu2E7TYUPh8Lz5dn5Ws2siIuQ17Vng=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataflow",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataflow",
        sum = "h1:VzG2tqsk/HbmOtq/XSfdF4cBvUWRK+S+oL9k4eWkENQ=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_google_cloud_go_dataform",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataform",
        sum = "h1:xcWso0hKOoxeW72AjBSIp/UfkvpqHNzzS0/oygHlcqY=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_datafusion",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datafusion",
        sum = "h1:eX9CZoyhKQW6g1Xj7+RONeDj1mV8KQDKEB9KLELX9/8=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_datalabeling",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datalabeling",
        sum = "h1:zxsCD/BLKXhNuRssen8lVXChUj8VxF3ofN06JfdWOXw=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_dataplex",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataplex",
        sum = "h1:yoBWuuUZklYp7nx26evIhzq8+i/nvKYuZr1jka9EqLs=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataproc_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataproc/v2",
        sum = "h1:4OpSiPMMGV3XmtPqskBU/RwYpj3yMFjtMLj/exi425Q=",
        version = "v2.0.1",
    )
    go_repository(
        name = "com_google_cloud_go_dataqna",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataqna",
        sum = "h1:ITpUJep04hC9V7C+gcK390HO++xesQFSUJ7S4nSnF3U=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datastore",
        sum = "h1:ktbC66bOQB3HJPQe8qNI1/aiQ77PMu7hD4mzE6uxe3w=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_datastream",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datastream",
        sum = "h1:ra/+jMv36zTAGPfi8TRne1hXme+UsKtdcK4j6bnqQiw=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_deploy",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/deploy",
        sum = "h1:A+w/xpWgz99EYzB6e31gMGAI/P5jTZ2UO7veQK5jQ8o=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_dialogflow",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dialogflow",
        sum = "h1:sCJbaXt6ogSbxWQnERKAzos57f02PP6WkGbOZvXUdwc=",
        version = "v1.40.0",
    )
    go_repository(
        name = "com_google_cloud_go_dlp",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dlp",
        sum = "h1:tF3wsJ2QulRhRLWPzWVkeDz3FkOGVoMl6cmDUHtfYxw=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_documentai",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/documentai",
        sum = "h1:dW8ex9yb3oT9s1yD2+yLcU8Zq15AquRZ+wd0U+TkxFw=",
        version = "v1.22.0",
    )
    go_repository(
        name = "com_google_cloud_go_domains",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/domains",
        sum = "h1:rqz6KY7mEg7Zs/69U6m6LMbB7PxFDWmT3QWNXIqhHm0=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_google_cloud_go_edgecontainer",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/edgecontainer",
        sum = "h1:zhHWnLzg6AqzE+I3gzJqiIwHfjEBhWctNQEzqb+FaRo=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_google_cloud_go_errorreporting",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/errorreporting",
        sum = "h1:kj1XEWMu8P0qlLhm3FwcaFsUvXChV/OraZwA70trRR0=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_essentialcontacts",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/essentialcontacts",
        sum = "h1:OEJ0MLXXCW/tX1fkxzEZOsv/wRfyFsvDVNaHWBAvoV0=",
        version = "v1.6.2",
    )
    go_repository(
        name = "com_google_cloud_go_eventarc",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/eventarc",
        sum = "h1:xIP3XZi0Xawx8DEfh++mE2lrIi5kQmCr/KcWhJ1q0J4=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_filestore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/filestore",
        sum = "h1:Eiz8xZzMJc5ppBWkuaod/PUdUZGCFR8ku0uS+Ah2fRw=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/firestore",
        sum = "h1:aeEA/N7DW7+l2u5jtkO8I0qv0D95YwjggD8kUHrTHO4=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_functions",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/functions",
        sum = "h1:LtAyqvO1TFmNLcROzHZhV0agEJfBi+zfMZsF4RT/a7U=",
        version = "v1.15.1",
    )
    go_repository(
        name = "com_google_cloud_go_gkebackup",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkebackup",
        sum = "h1:lgyrpdhtJKV7l1GM15YFt+OCyHMxsQZuSydyNmS0Pxo=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkeconnect",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkeconnect",
        sum = "h1:a1ckRvVznnuvDWESM2zZDzSVFvggeBaVY5+BVB8tbT0=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_gkehub",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkehub",
        sum = "h1:2BLSb8i+Co1P05IYCKATXy5yaaIw/ZqGvVSBTLdzCQo=",
        version = "v0.14.1",
    )
    go_repository(
        name = "com_google_cloud_go_gkemulticloud",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkemulticloud",
        sum = "h1:MluqhtPVZReoriP5+adGIw+ij/RIeRik8KApCW2WMTw=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_google_cloud_go_gsuiteaddons",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gsuiteaddons",
        sum = "h1:mi9jxZpzVjLQibTS/XfPZvl+Jr6D5Bs8pGqUjllRb00=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iam",
        sum = "h1:lW7fzj15aVIXYHREOqjRBV9PsH0Z6u8Y46a1YGvQP4Y=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_google_cloud_go_iap",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iap",
        sum = "h1:X1tcp+EoJ/LGX6cUPt3W2D4H2Kbqq0pLAsldnsCjLlE=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_ids",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/ids",
        sum = "h1:khXYmSoDDhWGEVxHl4c4IgbwSRR+qE/L4hzP3vaU9Hc=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_google_cloud_go_iot",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iot",
        sum = "h1:yrH0OSmicD5bqGBoMlWG8UltzdLkYzNUwNVUVz7OT54=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/kms",
        sum = "h1:xYl5WEaSekKYN5gGRyhjvZKM22GVBBCzegGNVPy+aIs=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_language",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/language",
        sum = "h1:3MXeGEv8AlX+O2LyV4pO4NGpodanc26AmXwOuipEym0=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_lifesciences",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/lifesciences",
        sum = "h1:axkANGx1wiBXHiPcJZAE+TDjjYoJRIDzbHC/WYllCBU=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/logging",
        sum = "h1:CJYxlNNNNAMkHp9em/YEXcfJg+rPDg7YfwoRpMU+t5I=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_longrunning",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/longrunning",
        sum = "h1:Fr7TXftcqTudoyRJa113hyaqlGdiBQkp0Gq7tErFDWI=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_google_cloud_go_managedidentities",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/managedidentities",
        sum = "h1:2/qZuOeLgUHorSdxSQGtnOu9xQkBn37+j+oZQv/KHJY=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_maps",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/maps",
        sum = "h1:PdfgpBLhAoSzZrQXP+/zBc78fIPLZSJp5y8+qSMn2UU=",
        version = "v1.4.0",
    )
    go_repository(
        name = "com_google_cloud_go_mediatranslation",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/mediatranslation",
        sum = "h1:50cF7c1l3BanfKrpnTCaTvhf+Fo6kdF21DG0byG7gYU=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_memcache",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/memcache",
        sum = "h1:7lkLsF0QF+Mre0O/NvkD9Q5utUNwtzvIYjrOLOs0HO0=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_metastore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/metastore",
        sum = "h1:+9DsxUOHvsqvC0ylrRc/JwzbXJaaBpfIK3tX0Lx8Tcc=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_monitoring",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/monitoring",
        sum = "h1:65JhLMd+JiYnXr6j5Z63dUYCuOg770p8a/VC+gil/58=",
        version = "v1.15.1",
    )
    go_repository(
        name = "com_google_cloud_go_networkconnectivity",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networkconnectivity",
        sum = "h1:LnrYM6lBEeTq+9f2lR4DjBhv31EROSAQi/P5W4Q0AEc=",
        version = "v1.12.1",
    )
    go_repository(
        name = "com_google_cloud_go_networkmanagement",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networkmanagement",
        sum = "h1:/3xP37eMxnyvkfLrsm1nv1b2FbMMSAEAOlECTvoeCq4=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_networksecurity",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networksecurity",
        sum = "h1:TBLEkMp3AE+6IV/wbIGRNTxnqLXHCTEQWoxRVC18TzY=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_google_cloud_go_notebooks",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/notebooks",
        sum = "h1:CUqMNEtv4EHFnbogV+yGHQH5iAQLmijOx191innpOcs=",
        version = "v1.9.1",
    )
    go_repository(
        name = "com_google_cloud_go_optimization",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/optimization",
        sum = "h1:pEwOAmO00mxdbesCRSsfj8Sd4rKY9kBrYW7Vd3Pq7cA=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_google_cloud_go_orchestration",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/orchestration",
        sum = "h1:KmN18kE/xa1n91cM5jhCh7s1/UfIguSCisw7nTMUzgE=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_orgpolicy",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/orgpolicy",
        sum = "h1:I/7dHICQkNwym9erHqmlb50LRU588NPCvkfIY0Bx9jI=",
        version = "v1.11.1",
    )
    go_repository(
        name = "com_google_cloud_go_osconfig",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/osconfig",
        sum = "h1:dgyEHdfqML6cUW6/MkihNdTVc0INQst0qSE8Ou1ub9c=",
        version = "v1.12.1",
    )
    go_repository(
        name = "com_google_cloud_go_oslogin",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/oslogin",
        sum = "h1:LdSuG3xBYu2Sgr3jTUULL1XCl5QBx6xwzGqzoDUw1j0=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_phishingprotection",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/phishingprotection",
        sum = "h1:aK/lNmSd1vtbft/vLe2g7edXK72sIQbqr2QyrZN/iME=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_policytroubleshooter",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/policytroubleshooter",
        sum = "h1:XTMHy31yFmXgQg57CB3w9YQX8US7irxDX0Fl0VwlZyY=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_privatecatalog",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/privatecatalog",
        sum = "h1:B/18xGo+E0EMS9LOEQ0zXz7F2asMgmVgTYGSI89MHOA=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/pubsub",
        sum = "h1:6SPCPvWav64tj0sVX/+npCBKhUi/UjJehy9op/V3p2g=",
        version = "v1.33.0",
    )
    go_repository(
        name = "com_google_cloud_go_pubsublite",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/pubsublite",
        sum = "h1:pX+idpWMIH30/K7c0epN6V703xpIcMXWRjKJsz0tYGY=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_recaptchaenterprise_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recaptchaenterprise/v2",
        sum = "h1:IGkbudobsTXAwmkEYOzPCQPApUCsN4Gbq3ndGVhHQpI=",
        version = "v2.7.2",
    )
    go_repository(
        name = "com_google_cloud_go_recommendationengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recommendationengine",
        sum = "h1:nMr1OEVHuDambRn+/y4RmNAmnR/pXCuHtH0Y4tCgGRQ=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_recommender",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recommender",
        sum = "h1:UKp94UH5/Lv2WXSQe9+FttqV07x/2p1hFTMMYVFtilg=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_redis",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/redis",
        sum = "h1:YrjQnCC7ydk+k30op7DSjSHw1yAYhqYXFcOq1bSXRYA=",
        version = "v1.13.1",
    )
    go_repository(
        name = "com_google_cloud_go_resourcemanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/resourcemanager",
        sum = "h1:QIAMfndPOHR6yTmMUB0ZN+HSeRmPjR/21Smq5/xwghI=",
        version = "v1.9.1",
    )
    go_repository(
        name = "com_google_cloud_go_resourcesettings",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/resourcesettings",
        sum = "h1:Fdyq418U69LhvNPFdlEO29w+DRRjwDA4/pFamm4ksAg=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_retail",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/retail",
        sum = "h1:gYBrb9u/Hc5s5lUTFXX1Vsbc/9BEvgtioY6ZKaK0DK8=",
        version = "v1.14.1",
    )
    go_repository(
        name = "com_google_cloud_go_run",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/run",
        sum = "h1:kHeIG8q+N6Zv0nDkBjSOYfK2eWqa5FnaiDPH/7/HirE=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_google_cloud_go_scheduler",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/scheduler",
        sum = "h1:yoZbZR8880KgPGLmACOMCiY2tPk+iX4V/dkxqTirlz8=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/secretmanager",
        sum = "h1:cLTCwAjFh9fKvU6F13Y4L9vPcx9yiWPyWXE4+zkuEQs=",
        version = "v1.11.1",
    )
    go_repository(
        name = "com_google_cloud_go_security",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/security",
        sum = "h1:jR3itwycg/TgGA0uIgTItcVhA55hKWiNJxaNNpQJaZE=",
        version = "v1.15.1",
    )
    go_repository(
        name = "com_google_cloud_go_securitycenter",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/securitycenter",
        sum = "h1:XOGJ9OpnDtqg8izd7gYk/XUhj8ytjIalyjjsR6oyG0M=",
        version = "v1.23.0",
    )
    go_repository(
        name = "com_google_cloud_go_servicedirectory",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/servicedirectory",
        sum = "h1:pBWpjCFVGWkzVTkqN3TBBIqNSoSHY86/6RL0soSQ4z8=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_shell",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/shell",
        sum = "h1:aHbwH9LSqs4r2rbay9f6fKEls61TAjT63jSyglsw7sI=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_spanner",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/spanner",
        sum = "h1:aqiMP8dhsEXgn9K5EZBWxPG7dxIiyM2VaikqeU4iteg=",
        version = "v1.47.0",
    )
    go_repository(
        name = "com_google_cloud_go_speech",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/speech",
        sum = "h1:MCagaq8ObV2tr1kZJcJYgXYbIn8Ai5rp42tyGYw9rls=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/storage",
        sum = "h1:uOdMxAs8HExqBlnLtnQyP0YkvbiDpdGShGKtx6U/oNM=",
        version = "v1.30.1",
    )
    go_repository(
        name = "com_google_cloud_go_storagetransfer",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/storagetransfer",
        sum = "h1:+ZLkeXx0K0Pk5XdDmG0MnUVqIR18lllsihU/yq39I8Q=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_talent",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/talent",
        sum = "h1:j46ZgD6N2YdpFPux9mc7OAf4YK3tiBCsbLKc8rQx+bU=",
        version = "v1.6.2",
    )
    go_repository(
        name = "com_google_cloud_go_texttospeech",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/texttospeech",
        sum = "h1:S/pR/GZT9p15R7Y2dk2OXD/3AufTct/NSxT4a7nxByw=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_tpu",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/tpu",
        sum = "h1:kQf1jgPY04UJBYYjNUO+3GrZtIb57MfGAW2bwgLbR3A=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_trace",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/trace",
        sum = "h1:EwGdOLCNfYOOPtgqo+D2sDLZmRCEO1AagRTJCU6ztdg=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_translate",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/translate",
        sum = "h1:PQHamiOzlehqLBJMnM72lXk/OsMQewZB12BKJ8zXrU0=",
        version = "v1.8.2",
    )
    go_repository(
        name = "com_google_cloud_go_video",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/video",
        sum = "h1:BRyyS+wU+Do6VOXnb8WfPr42ZXti9hzmLKLUCkggeK4=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_google_cloud_go_videointelligence",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/videointelligence",
        sum = "h1:MBMWnkQ78GQnRz5lfdTAbBq/8QMCF3wahgtHh3s/J+k=",
        version = "v1.11.1",
    )
    go_repository(
        name = "com_google_cloud_go_vision_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vision/v2",
        sum = "h1:ccK6/YgPfGHR/CyESz1mvIbsht5Y2xRsWCPqmTNydEw=",
        version = "v2.7.2",
    )
    go_repository(
        name = "com_google_cloud_go_vmmigration",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vmmigration",
        sum = "h1:gnjIclgqbEMc+cF5IJuPxp53wjBIlqZ8h9hE8Rkwp7A=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_vmwareengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vmwareengine",
        sum = "h1:qsJ0CPlOQu/3MFBGklu752v3AkD+Pdu091UmXJ+EjTA=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_google_cloud_go_vpcaccess",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vpcaccess",
        sum = "h1:ram0GzjNWElmbxXMIzeOZUkQ9J8ZAahD6V8ilPGqX0Y=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_webrisk",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/webrisk",
        sum = "h1:Ssy3MkOMOnyRV5H2bkMQ13Umv7CwB/kugo3qkAX83Fk=",
        version = "v1.9.1",
    )
    go_repository(
        name = "com_google_cloud_go_websecurityscanner",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/websecurityscanner",
        sum = "h1:CfEF/vZ+xXyAR3zC9iaC/QRdf1MEgS20r5UR17Q4gOg=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_workflows",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/workflows",
        sum = "h1:2akeQ/PgtRhrNuD/n1WvJd5zb7YyuDZrlOanBj2ihPg=",
        version = "v1.11.1",
    )
    go_repository(
        name = "com_lukechampine_uint128",
        build_file_proto_mode = "disable_global",
        importpath = "lukechampine.com/uint128",
        sha256 = "9ff6e9ad553a69fdb961ab2d92f92cda183ef616a6709c15972c2d4bedf33de5",
        strip_prefix = "lukechampine.com/uint128@v1.2.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/lukechampine.com/uint128/com_lukechampine_uint128-v1.2.0.zip",
            "http://ats.apps.svc/gomod/lukechampine.com/uint128/com_lukechampine_uint128-v1.2.0.zip",
            "https://cache.hawkingrei.com/gomod/lukechampine.com/uint128/com_lukechampine_uint128-v1.2.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/lukechampine.com/uint128/com_lukechampine_uint128-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_shuralyov_dmitri_gpu_mtl",
        build_file_proto_mode = "disable_global",
        importpath = "dmitri.shuralyov.com/gpu/mtl",
        sum = "h1:VpgP7xuJadIUuKccphEpTJnWhS2jkQyMt6Y7pJCD7fY=",
        version = "v0.0.0-20190408044501-666a987793e9",
    )
    go_repository(
        name = "com_sourcegraph_sourcegraph_appdash",
        build_file_proto_mode = "disable_global",
        importpath = "sourcegraph.com/sourcegraph/appdash",
        replace = "github.com/sourcegraph/appdash",
        sum = "h1:IJ3DuWHPTJrsqtIqjfdmPTELdTFGefvrOa2eTeRBleQ=",
        version = "v0.0.0-20190731080439-ebfcffb1b5c0",
    )
    go_repository(
        name = "com_sourcegraph_sourcegraph_appdash_data",
        build_file_proto_mode = "disable_global",
        importpath = "sourcegraph.com/sourcegraph/appdash-data",
        replace = "github.com/sourcegraph/appdash-data",
        sum = "h1:8ZnTA26bBOoPkAbbitKPgNlpw0Bwt7ZlpYgZWHWJR/w=",
        version = "v0.0.0-20151005221446-73f23eafcf67",
    )
    go_repository(
        name = "com_stathat_c_consistent",
        build_file_proto_mode = "disable_global",
        importpath = "stathat.com/c/consistent",
        sum = "h1:ezyc51EGcRPJUxfHGSgJjWzJdj3NiMU9pNfLNGiXV0c=",
        version = "v1.0.0",
    )
    go_repository(
        name = "ht_sr_git_~sbinet_gg",
        build_file_proto_mode = "disable_global",
        importpath = "git.sr.ht/~sbinet/gg",
        sha256 = "435103529c4f24aecf7e4550bc816db2482dda4ee0123d337daba99971a8c498",
        strip_prefix = "git.sr.ht/~sbinet/gg@v0.3.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/git.sr.ht/~sbinet/gg/ht_sr_git_~sbinet_gg-v0.3.1.zip",
            "http://ats.apps.svc/gomod/git.sr.ht/~sbinet/gg/ht_sr_git_~sbinet_gg-v0.3.1.zip",
            "https://cache.hawkingrei.com/gomod/git.sr.ht/~sbinet/gg/ht_sr_git_~sbinet_gg-v0.3.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/git.sr.ht/~sbinet/gg/ht_sr_git_~sbinet_gg-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_alecthomas_kingpin_v2",
        build_file_proto_mode = "disable",
        importpath = "gopkg.in/alecthomas/kingpin.v2",
        sum = "h1:jMFz6MfLP0/4fUyZle81rXUoxOBFi19VUFKVDOQfozc=",
        version = "v2.2.6",
    )

    go_repository(
        name = "in_gopkg_check_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/check.v1",
        sum = "h1:Hei/4ADfdWqJk1ZMxUNpqntNwaWcugrBjAiHlqqRiVk=",
        version = "v1.0.0-20201130134442-10cb98267c6c",
    )
    go_repository(
        name = "in_gopkg_errgo_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/errgo.v2",
        sum = "h1:0vLT13EuvQ0hNvakwLuFZ/jYrLp5F3kcWHXdRggjCE8=",
        version = "v2.1.0",
    )
    go_repository(
        name = "in_gopkg_fsnotify_fsnotify_v1",
        build_file_proto_mode = "disable",
        importpath = "gopkg.in/fsnotify/fsnotify.v1",
        sum = "h1:2fkCHbPQZNYRAyRyIV9VX0bpRkxIorlQDiYRmufHnhA=",
        version = "v1.3.1",
    )
    go_repository(
        name = "in_gopkg_fsnotify_v1",
        build_file_proto_mode = "disable",
        importpath = "gopkg.in/fsnotify.v1",
        sum = "h1:xOHLXZwVvI9hhs+cLKq5+I5onOuwQLhQwiu63xxlHs4=",
        version = "v1.4.7",
    )
    go_repository(
        name = "in_gopkg_go_playground_assert_v1",
        build_file_proto_mode = "disable",
        importpath = "gopkg.in/go-playground/assert.v1",
        sum = "h1:xoYuJVE7KT85PYWrN730RguIQO0ePzVRfFMXadIrXTM=",
        version = "v1.2.1",
    )
    go_repository(
        name = "in_gopkg_go_playground_validator_v8",
        build_file_proto_mode = "disable",
        importpath = "gopkg.in/go-playground/validator.v8",
        sum = "h1:lFB4DoMU6B626w8ny76MV7VX6W2VHct2GVOI3xgiMrQ=",
        version = "v8.18.2",
    )

    go_repository(
        name = "in_gopkg_inf_v0",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/inf.v0",
        sum = "h1:73M5CoZyi3ZLMOyDlQh031Cx6N9NDJ2Vvfl76EDAgDc=",
        version = "v0.9.1",
    )
    go_repository(
        name = "in_gopkg_ini_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/ini.v1",
        sum = "h1:Dgnx+6+nfE+IfzjUEISNeydPJh9AXNNsWbGP9KzCsOA=",
        version = "v1.67.0",
    )
    go_repository(
        name = "in_gopkg_jcmturner_aescts_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/aescts.v1",
        sum = "h1:cVVZBK2b1zY26haWB4vbBiZrfFQnfbTVrE3xZq6hrEw=",
        version = "v1.0.1",
    )
    go_repository(
        name = "in_gopkg_jcmturner_dnsutils_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/dnsutils.v1",
        sum = "h1:cIuC1OLRGZrld+16ZJvvZxVJeKPsvd5eUIvxfoN5hSM=",
        version = "v1.0.1",
    )
    go_repository(
        name = "in_gopkg_jcmturner_goidentity_v3",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/goidentity.v3",
        sum = "h1:1duIyWiTaYvVx3YX2CYtpJbUFd7/UuPYCfgXtQ3VTbI=",
        version = "v3.0.0",
    )
    go_repository(
        name = "in_gopkg_jcmturner_gokrb5_v7",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/gokrb5.v7",
        sum = "h1:0709Jtq/6QXEuWRfAm260XqlpcwL1vxtO1tUE2qK8Z4=",
        version = "v7.3.0",
    )
    go_repository(
        name = "in_gopkg_jcmturner_rpc_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/rpc.v1",
        sum = "h1:QHIUxTX1ISuAv9dD2wJ9HWQVuWDX/Zc0PfeC2tjc4rU=",
        version = "v1.1.0",
    )
    go_repository(
        name = "in_gopkg_mgo_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/mgo.v2",
        sum = "h1:xcEWjVhvbDy+nHP67nPDDpbYrY+ILlfndk4bRioVHaU=",
        version = "v2.0.0-20180705113604-9856a29383ce",
    )
    go_repository(
        name = "in_gopkg_natefinch_lumberjack_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/natefinch/lumberjack.v2",
        sum = "h1:1Lc07Kr7qY4U2YPouBjpCLxpiyxIVoxqXgkXLknAOE8=",
        version = "v2.0.0",
    )
    go_repository(
        name = "in_gopkg_resty_v1",
        build_file_proto_mode = "disable",
        importpath = "gopkg.in/resty.v1",
        sum = "h1:CuXP0Pjfw9rOuY6EP+UvtNvt5DSqHpIxILZKT/quCZI=",
        version = "v1.12.0",
    )

    go_repository(
        name = "in_gopkg_tomb_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/tomb.v1",
        sum = "h1:uRGJdciOHaEIrze2W8Q3AKkepLTh2hOroT7a+7czfdQ=",
        version = "v1.0.0-20141024135613-dd632973f1e7",
    )
    go_repository(
        name = "in_gopkg_yaml_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/yaml.v2",
        sum = "h1:D8xgwECY7CYvx+Y2n4sBz93Jn9JRvxdiyyo8CTfuKaY=",
        version = "v2.4.0",
    )
    go_repository(
        name = "in_gopkg_yaml_v3",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/yaml.v3",
        sum = "h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=",
        version = "v3.0.1",
    )
    go_repository(
        name = "io_etcd_go_bbolt",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/bbolt",
        sum = "h1:xs88BrvEv273UsB79e0hcVrlUWmS0a8upikMFhSyAtA=",
        version = "v1.3.8",
    )
    go_repository(
        name = "io_etcd_go_etcd_api_v3",
        build_file_proto_mode = "disable",
        importpath = "go.etcd.io/etcd/api/v3",
        sum = "h1:szRajuUUbLyppkhs9K6BRtjY37l66XQQmw7oZRANE4k=",
        version = "v3.5.10",
    )
    go_repository(
        name = "io_etcd_go_etcd_client_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/pkg/v3",
        sum = "h1:kfYIdQftBnbAq8pUWFXfpuuxFSKzlmM5cSn76JByiT0=",
        version = "v3.5.10",
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v2",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/v2",
        sum = "h1:MrmRktzv/XF8CvtQt+P6wLUlURaNpSDJHFZhe//2QE4=",
        version = "v2.305.10",
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/v3",
        sum = "h1:W9TXNZ+oB3MCd/8UjxHTWK5J9Nquw9fQBLJd5ne5/Ao=",
        version = "v3.5.10",
    )
    go_repository(
        name = "io_etcd_go_etcd_etcdutl_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/etcdutl/v3",
        sum = "h1:XDNv2bGD6Ylz3Gb9lIGV/IYLk1bwTvyCIi1EI4hyyqo=",
        version = "v3.5.2",
    )
    go_repository(
        name = "io_etcd_go_etcd_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/pkg/v3",
        sum = "h1:WPR8K0e9kWl1gAhB5A7gEa5ZBTNkT9NdNWrR8Qpo1CM=",
        version = "v3.5.10",
    )
    go_repository(
        name = "io_etcd_go_etcd_raft_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/raft/v3",
        sum = "h1:cgNAYe7xrsrn/5kXMSaH8kM/Ky8mAdMqGOxyYwpP0LA=",
        version = "v3.5.10",
    )
    go_repository(
        name = "io_etcd_go_etcd_server_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/server/v3",
        sum = "h1:4NOGyOwD5sUZ22PiWYKmfxqoeh72z6EhYjNosKGLmZg=",
        version = "v3.5.10",
    )
    go_repository(
        name = "io_etcd_go_etcd_tests_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/tests/v3",
        sum = "h1:uk7/uMGVebpBDl+roivowHt6gJ5Fnqwik3syDkoSKdo=",
        version = "v3.5.2",
    )
    go_repository(
        name = "io_etcd_go_gofail",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/gofail",
        sum = "h1:XItAMIhOojXFQMgrxjnd2EIIHun/d5qL0Pf7FzVTkFg=",
        version = "v0.1.0",
    )
    go_repository(
        name = "io_filippo_edwards25519",
        build_file_proto_mode = "disable_global",
        importpath = "filippo.io/edwards25519",
        sha256 = "9ac43a686d06fdebd719f7af3866c87eb069302272dfb131007adf471c308b65",
        strip_prefix = "filippo.io/edwards25519@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/filippo.io/edwards25519/io_filippo_edwards25519-v1.1.0.zip",
            "http://ats.apps.svc/gomod/filippo.io/edwards25519/io_filippo_edwards25519-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/filippo.io/edwards25519/io_filippo_edwards25519-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/filippo.io/edwards25519/io_filippo_edwards25519-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "io_gorm_driver_mysql",
        build_file_proto_mode = "disable_global",
        importpath = "gorm.io/driver/mysql",
        sha256 = "4a529d09c3a0082e313ed76f4d3fe5bfb2667711ef36d7ac6e6804dba43e7978",
        strip_prefix = "gorm.io/driver/mysql@v1.5.7",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gorm.io/driver/mysql/io_gorm_driver_mysql-v1.5.7.zip",
            "http://ats.apps.svc/gomod/gorm.io/driver/mysql/io_gorm_driver_mysql-v1.5.7.zip",
            "https://cache.hawkingrei.com/gomod/gorm.io/driver/mysql/io_gorm_driver_mysql-v1.5.7.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gorm.io/driver/mysql/io_gorm_driver_mysql-v1.5.7.zip",
        ],
    )
    go_repository(
        name = "io_gorm_gorm",
        build_file_proto_mode = "disable_global",
        importpath = "gorm.io/gorm",
        sha256 = "e33460f1d29271c188f4920275adc1d8f83890a17dfa6a9f03face8cdda32cb5",
        strip_prefix = "gorm.io/gorm@v1.25.11",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gorm.io/gorm/io_gorm_gorm-v1.25.11.zip",
            "http://ats.apps.svc/gomod/gorm.io/gorm/io_gorm_gorm-v1.25.11.zip",
            "https://cache.hawkingrei.com/gomod/gorm.io/gorm/io_gorm_gorm-v1.25.11.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gorm.io/gorm/io_gorm_gorm-v1.25.11.zip",
        ],
    )
    go_repository(
        name = "io_k8s_api",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/api",
        sum = "h1:J0hann2hfxWr1hinZIDefw7Q96wmCBx6SSB8IY0MdDg=",
        version = "v0.24.0",
    )
    go_repository(
        name = "io_k8s_apimachinery",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/apimachinery",
        sum = "h1:ydFCyC/DjCvFCHK5OPMKBlxayQytB8pxy8YQInd5UyQ=",
        version = "v0.24.0",
    )
    go_repository(
        name = "io_k8s_client_go",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/client-go",
        sum = "h1:U5Bt+dab9K8qaUmXINrkXO135kA11/i5Kg1RUydgaMQ=",
        version = "v11.0.1-0.20190409021438-1a26190bd76a+incompatible",
    )
    go_repository(
        name = "io_k8s_gengo",
        build_file_proto_mode = "disable",
        importpath = "k8s.io/gengo",
        sum = "h1:GohjlNKauSai7gN4wsJkeZ3WAJx4Sh+oT/b5IYn5suA=",
        version = "v0.0.0-20210813121822-485abfe95c7c",
    )

    go_repository(
        name = "io_k8s_klog",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog",
        sum = "h1:0VPpR+sizsiivjIfIAQH/rl8tan6jvWkS7lU+0di3lE=",
        version = "v0.3.0",
    )
    go_repository(
        name = "io_k8s_klog_v2",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog/v2",
        sum = "h1:VW25q3bZx9uE3vvdL6M8ezOX79vA2Aq1nEWLqNQclHc=",
        version = "v2.60.1",
    )
    go_repository(
        name = "io_k8s_kube_openapi",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/kube-openapi",
        sum = "h1:Gii5eqf+GmIEwGNKQYQClCayuJCe2/4fZUvF7VG99sU=",
        version = "v0.0.0-20220328201542-3ee0da9b0b42",
    )
    go_repository(
        name = "io_k8s_sigs_json",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/json",
        sum = "h1:kDi4JBNAsJWfz1aEXhO8Jg87JJaPNLh5tIzYHgStQ9Y=",
        version = "v0.0.0-20211208200746-9f7c6b3444d2",
    )
    go_repository(
        name = "io_k8s_sigs_structured_merge_diff_v4",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/structured-merge-diff/v4",
        sum = "h1:bKCqE9GvQ5tiVHn5rfn1r+yao3aLQEaLzkkmAkf+A6Y=",
        version = "v4.2.1",
    )
    go_repository(
        name = "io_k8s_sigs_yaml",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/yaml",
        sum = "h1:kr/MCeFWJWTwyaHoR9c8EjH9OumOmoF9YGiZd7lFm/Q=",
        version = "v1.2.0",
    )
    go_repository(
        name = "io_k8s_utils",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/utils",
        sum = "h1:HNSDgDCrr/6Ly3WEGKZftiE7IY19Vz2GdbOCyI4qqhc=",
        version = "v0.0.0-20220210201930-3a6ce19ff2f9",
    )
    go_repository(
        name = "io_opencensus_go",
        build_file_proto_mode = "disable_global",
        importpath = "go.opencensus.io",
        replace = "go.opencensus.io",
        sum = "h1:+KpZCwn3HdqM4KgXC+ywfGPIC40XIwj6C5p+6mbC9a8=",
        version = "v0.23.1-0.20220331163232-052120675fac",
    )
    go_repository(
        name = "io_opencensus_go_contrib_exporter_ocagent",
        build_file_proto_mode = "disable",
        importpath = "contrib.go.opencensus.io/exporter/ocagent",
        sum = "h1:jGFvw3l57ViIVEPKKEUXPcLYIXJmQxLUh6ey1eJhwyc=",
        version = "v0.4.12",
    )

    go_repository(
        name = "io_opentelemetry_go_collector_featuregate",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/collector/featuregate",
        sha256 = "b8f74a6e4e4e68dc630b54917755c1604525ce161b9f8a2730079fa19bc11676",
        strip_prefix = "go.opentelemetry.io/collector/featuregate@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/collector/featuregate/io_opentelemetry_go_collector_featuregate-v1.0.1.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/collector/featuregate/io_opentelemetry_go_collector_featuregate-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/collector/featuregate/io_opentelemetry_go_collector_featuregate-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/collector/featuregate/io_opentelemetry_go_collector_featuregate-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_collector_pdata",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/collector/pdata",
        sha256 = "696f8737e0dd15c76a683c7ab00f373a50a4c1f27890ed288ffc994b1bb19d15",
        strip_prefix = "go.opentelemetry.io/collector/pdata@v1.0.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/collector/pdata/io_opentelemetry_go_collector_pdata-v1.0.1.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/collector/pdata/io_opentelemetry_go_collector_pdata-v1.0.1.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/collector/pdata/io_opentelemetry_go_collector_pdata-v1.0.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/collector/pdata/io_opentelemetry_go_collector_pdata-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_collector_semconv",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/collector/semconv",
        sha256 = "7ee5e8d4b9f9bbdefdec35bc49866ab628ca740344a8940d33874868debfb034",
        strip_prefix = "go.opentelemetry.io/collector/semconv@v0.93.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/collector/semconv/io_opentelemetry_go_collector_semconv-v0.93.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/collector/semconv/io_opentelemetry_go_collector_semconv-v0.93.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/collector/semconv/io_opentelemetry_go_collector_semconv-v0.93.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/collector/semconv/io_opentelemetry_go_collector_semconv-v0.93.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_contrib",
        build_file_proto_mode = "disable",
        importpath = "go.opentelemetry.io/contrib",
        sum = "h1:ubFQUn0VCZ0gPwIoJfBJVpeBlyRMxu8Mm/huKWYd9p0=",
        version = "v0.20.0",
    )

    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
        sum = "h1:Wx7nFnvCaissIUZxPkBqDz2963Z+Cl+PkYbDKzTxDqQ=",
        version = "v0.25.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
        sha256 = "205c8117aebdc6f6ebab7fbb946d260933716c68a1b2dda8d43ab142b6622b14",
        strip_prefix = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp@v0.49.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp-v0.49.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp-v0.49.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp-v0.49.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp-v0.49.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel",
        sum = "h1:4XKyXmfqJLOQ7feyV5DB6gsBFZ0ltB8vLtp6pj4JIcc=",
        version = "v1.0.1",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp",
        build_file_proto_mode = "disable",
        importpath = "go.opentelemetry.io/otel/exporters/otlp",
        sum = "h1:PTNgq9MRmQqqJY0REVbZFvwkYOA85vbdQU/nVfxDyqg=",
        version = "v0.20.0",
    )

    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace",
        sum = "h1:ofMbch7i29qIUf7VtF+r0HRF6ac0SBaPSziSsKp7wkk=",
        version = "v1.0.1",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc",
        sum = "h1:CFMFNoz+CGprjFAFy+RJFrfEe4GBia3RRm2a4fREvCA=",
        version = "v1.0.1",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp",
        sha256 = "6b7c01ca38a5b1c4216adcf54c422e0a78c257d918e89b8dd02721c6098b9dec",
        strip_prefix = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp@v1.22.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp-v1.22.0.zip",
            "http://ats.apps.svc/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp-v1.22.0.zip",
            "https://cache.hawkingrei.com/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp-v1.22.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp-v1.22.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_metric",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/metric",
        sum = "h1:4kzhXFP+btKm4jwxpjIqjs41A7MakRFUS86bqLHTIw8=",
        version = "v0.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_oteltest",
        build_file_proto_mode = "disable",
        importpath = "go.opentelemetry.io/otel/oteltest",
        sum = "h1:HiITxCawalo5vQzdHfKeZurV8x7ljcqAgiWzF6Vaeaw=",
        version = "v0.20.0",
    )

    go_repository(
        name = "io_opentelemetry_go_otel_sdk",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/sdk",
        sum = "h1:wXxFEWGo7XfXupPwVJvTBOaPBC9FEg0wB8hMNrKk+cA=",
        version = "v1.0.1",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk_export_metric",
        build_file_proto_mode = "disable",
        importpath = "go.opentelemetry.io/otel/sdk/export/metric",
        sum = "h1:c5VRjxCXdQlx1HjzwGdQHzZaVI82b5EbBgOu2ljD92g=",
        version = "v0.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk_metric",
        build_file_proto_mode = "disable",
        importpath = "go.opentelemetry.io/otel/sdk/metric",
        sum = "h1:7ao1wpzHRVKf0OQ7GIxiQJA6X7DLX9o14gmVon7mMK8=",
        version = "v0.20.0",
    )

    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/trace",
        sum = "h1:StTeIH6Q3G4r0Fiw34LTokUFESZgIDUr0qIJ7mKmAfw=",
        version = "v1.0.1",
    )
    go_repository(
        name = "io_opentelemetry_go_proto_otlp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/proto/otlp",
        sum = "h1:C0g6TWmQYvjKRnljRULLWUVJGy8Uvu0NEL/5frY2/t4=",
        version = "v0.9.0",
    )
    go_repository(
        name = "io_rsc_binaryregexp",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/binaryregexp",
        sum = "h1:HfqmD5MEmC0zvwBuF187nq9mdnXjXsSivRiXN7SmRkE=",
        version = "v0.2.0",
    )
    go_repository(
        name = "io_rsc_pdf",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/pdf",
        sum = "h1:k1MczvYDUvJBe93bYd7wrZLLUEcLZAuF824/I4e5Xr4=",
        version = "v0.1.1",
    )
    go_repository(
        name = "io_rsc_quote_v3",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/quote/v3",
        sum = "h1:9JKUTTIUgS6kzR9mK1YuGKv6Nl+DijDNIc0ghT58FaY=",
        version = "v3.1.0",
    )
    go_repository(
        name = "io_rsc_sampler",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/sampler",
        sum = "h1:7uVkIFmeBqHfdjD+gZwtXXI+RODJ2Wc4O7MPEh/QiW4=",
        version = "v1.3.0",
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
        name = "org_gioui",
        build_file_proto_mode = "disable_global",
        importpath = "gioui.org",
        sha256 = "fcbab2a0ea09ff775c1ff4fa99299d95b94aad496b1ac329e3c7389119168fc0",
        strip_prefix = "gioui.org@v0.0.0-20210308172011-57750fc8a0a6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/gioui.org/org_gioui-v0.0.0-20210308172011-57750fc8a0a6.zip",
            "http://ats.apps.svc/gomod/gioui.org/org_gioui-v0.0.0-20210308172011-57750fc8a0a6.zip",
            "https://cache.hawkingrei.com/gomod/gioui.org/org_gioui-v0.0.0-20210308172011-57750fc8a0a6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/gioui.org/org_gioui-v0.0.0-20210308172011-57750fc8a0a6.zip",
        ],
    )
    go_repository(
        name = "org_go_simpler_musttag",
        build_file_proto_mode = "disable_global",
        importpath = "go-simpler.org/musttag",
        sha256 = "43fc7398f946c26ec6afc510865995253bf929e335ae0eedb98410d26bdc00a7",
        strip_prefix = "go-simpler.org/musttag@v0.12.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go-simpler.org/musttag/org_go_simpler_musttag-v0.12.2.zip",
            "http://ats.apps.svc/gomod/go-simpler.org/musttag/org_go_simpler_musttag-v0.12.2.zip",
            "https://cache.hawkingrei.com/gomod/go-simpler.org/musttag/org_go_simpler_musttag-v0.12.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go-simpler.org/musttag/org_go_simpler_musttag-v0.12.2.zip",
        ],
    )
    go_repository(
        name = "org_go_simpler_sloglint",
        build_file_proto_mode = "disable_global",
        importpath = "go-simpler.org/sloglint",
        sha256 = "3c82534ba2bac463c0f965046f08e06b435cc51254dc2a44260ac30ac462493f",
        strip_prefix = "go-simpler.org/sloglint@v0.7.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/go-simpler.org/sloglint/org_go_simpler_sloglint-v0.7.2.zip",
            "http://ats.apps.svc/gomod/go-simpler.org/sloglint/org_go_simpler_sloglint-v0.7.2.zip",
            "https://cache.hawkingrei.com/gomod/go-simpler.org/sloglint/org_go_simpler_sloglint-v0.7.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/go-simpler.org/sloglint/org_go_simpler_sloglint-v0.7.2.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_api",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/api",
        sum = "h1:RjPESny5CnQRn9V6siglged+DZCgfu9l6mO9dkX9VOg=",
        version = "v0.128.0",
    )
    go_repository(
        name = "org_golang_google_appengine",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/appengine",
        sum = "h1:FZR1q0exgwxzPzp/aF+VccGrSfxfPpkBqjIIEq3ru6c=",
        version = "v1.6.7",
    )
    go_repository(
        name = "org_golang_google_genproto",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto",
        sum = "h1:VBu5YqKPv6XiJ199exd8Br+Aetz+o08F+PLMnwJQHAY=",
        version = "v0.0.0-20230822172742-b8732ec3820d",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_api",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto/googleapis/api",
        sum = "h1:DoPTO70H+bcDXcd39vOqb2viZxgqeBeSGtZ55yZU4/Q=",
        version = "v0.0.0-20230822172742-b8732ec3820d",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_bytestream",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto/googleapis/bytestream",
        sum = "h1:g3hIDl0jRNd9PPTs2uBzYuaD5mQuwOkZY0vSc0LR32o=",
        version = "v0.0.0-20230530153820-e85fd2cbaebc",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_rpc",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto/googleapis/rpc",
        sum = "h1:uvYuEyMHKNt+lT4K3bN6fGswmK8qSvcreM3BwjDh+y4=",
        version = "v0.0.0-20230822172742-b8732ec3820d",
    )
    go_repository(
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/grpc",
        sum = "h1:Z5Iec2pjwb+LEOqzpB2MR12/eKFhDPhuqW91O+4bwUk=",
        version = "v1.59.0",
    )
    go_repository(
        name = "org_golang_google_grpc_cmd_protoc_gen_go_grpc",
        build_file_proto_mode = "disable",
        importpath = "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
        sum = "h1:rNBFJjBCOgVr9pWD7rs/knKL4FRTKgpZmsRfV214zcA=",
        version = "v1.3.0",
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
        sum = "h1:6xV6lTsCfpGD21XK49h7MhtcApnLqkfYgPcdHftf6hg=",
        version = "v1.34.2",
    )
    go_repository(
        name = "org_golang_x_crypto",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/crypto",
        sum = "h1:L5SG1JTTXupVV3n6sUqMTeWbjAyfPwoda2DLX8J8FrQ=",
        version = "v0.29.0",
    )
    go_repository(
        name = "org_golang_x_exp",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/exp",
        sum = "h1:FRnLl4eNAQl8hwxVVC17teOw8kdjVDVAiFMtgUdTSRQ=",
        version = "v0.0.0-20231110203233-9a3e6036ecaa",
    )
    go_repository(
        name = "org_golang_x_exp_typeparams",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/exp/typeparams",
        sum = "h1:1P7xPZEwZMoBoz0Yze5Nx2/4pxj6nw9ZqHWXqP0iRgQ=",
        version = "v0.0.0-20231108232855-2478ac86f678",
    )
    go_repository(
        name = "org_golang_x_image",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/image",
        sum = "h1:+qEpEAPhDZ1o0x3tHzZTQDArnOixOzGD9HUJfcg0mb4=",
        version = "v0.0.0-20190802002840-cff245a6509b",
    )
    go_repository(
        name = "org_golang_x_lint",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/lint",
        sum = "h1:VLliZ0d+/avPrXXH+OakdXhpJuEoBZuwh1m2j7U6Iug=",
        version = "v0.0.0-20210508222113-6edffad5e616",
    )
    go_repository(
        name = "org_golang_x_mobile",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/mobile",
        sum = "h1:4+4C/Iv2U4fMZBiMCc98MG1In4gJY5YRhtpDNeDeHWs=",
        version = "v0.0.0-20190719004257-d2bd2a29d028",
    )
    go_repository(
        name = "org_golang_x_mod",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/mod",
        sum = "h1:zY54UmvipHiNd+pm+m0x9KhZ9hl1/7QNMyxXbc6ICqA=",
        version = "v0.17.0",
    )
    go_repository(
        name = "org_golang_x_net",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/net",
        sum = "h1:68CPQngjLL0r2AlUKiSxtQFKvzRVbnzLwMUn5SzcLHo=",
        version = "v0.31.0",
    )
    go_repository(
        name = "org_golang_x_oauth2",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/oauth2",
        sum = "h1:tsimM75w1tF/uws5rbeHzIWxEqElMehnc+iW793zsZs=",
        version = "v0.21.0",
    )
    go_repository(
        name = "org_golang_x_perf",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/perf",
        sha256 = "bc1b902e645fdd5d210b7db8f3280833af225b131dab5842d7a6d32a676f80f5",
        strip_prefix = "golang.org/x/perf@v0.0.0-20230113213139-801c7ef9e5c5",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/golang.org/x/perf/org_golang_x_perf-v0.0.0-20230113213139-801c7ef9e5c5.zip",
            "http://ats.apps.svc/gomod/golang.org/x/perf/org_golang_x_perf-v0.0.0-20230113213139-801c7ef9e5c5.zip",
            "https://cache.hawkingrei.com/gomod/golang.org/x/perf/org_golang_x_perf-v0.0.0-20230113213139-801c7ef9e5c5.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/golang.org/x/perf/org_golang_x_perf-v0.0.0-20230113213139-801c7ef9e5c5.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_sync",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sync",
        sum = "h1:fEo0HyrW1GIgZdpbhCRO0PkJajUS5H9IFUztCgEo2jQ=",
        version = "v0.9.0",
    )
    go_repository(
        name = "org_golang_x_sys",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sys",
        sum = "h1:wBqf8DvsY9Y/2P8gAfPDEYNuS30J4lPHJxXSb/nJZ+s=",
        version = "v0.27.0",
    )
    go_repository(
        name = "org_golang_x_telemetry",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/telemetry",
        sum = "h1:zf5N6UOrA487eEFacMePxjXAJctxKmyjKUsjA11Uzuk=",
        version = "v0.0.0-20240521205824-bda55230c457",
    )
    go_repository(
        name = "org_golang_x_term",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/term",
        sum = "h1:WEQa6V3Gja/BhNxg540hBip/kkaYtRg3cxg4oXSw4AU=",
        version = "v0.26.0",
    )
    go_repository(
        name = "org_golang_x_text",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/text",
        sum = "h1:gK/Kv2otX8gz+wn7Rmb3vT96ZwuoxnQlY+HlJVj7Qug=",
        version = "v0.20.0",
    )
    go_repository(
        name = "org_golang_x_time",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/time",
        sum = "h1:52I/1L54xyEQAYdtcSuxtiT84KGYTBGXwayxmIpNJhE=",
        version = "v0.2.0",
    )
    go_repository(
        name = "org_golang_x_tools",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/tools",
        sum = "h1:SHq4Rl+B7WvyM4XODon1LXtP7gcG49+7Jubt1gWWswY=",
        version = "v0.21.1-0.20240531212143-b6235391adb3",
    )
    go_repository(
        name = "org_golang_x_xerrors",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/xerrors",
        sum = "h1:H2TDz8ibqkAF6YGhCdN3jS9O0/s90v0rJh3X/OLHEUk=",
        version = "v0.0.0-20220907171357-04be3eba64a2",
    )
    go_repository(
        name = "org_gonum_v1_gonum",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/gonum",
        sum = "h1:CCXrcPKiGGotvnN6jfUsKk4rRqm7q09/YbKb5xCEvtM=",
        version = "v0.8.2",
    )
    go_repository(
        name = "org_gonum_v1_netlib",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/netlib",
        sum = "h1:OE9mWmgKkjJyEmDAAtGMPjXu+YNeGvK9VTSHY6+Qihc=",
        version = "v0.0.0-20190313105609-8cb42192e0e0",
    )
    go_repository(
        name = "org_gonum_v1_plot",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/plot",
        sum = "h1:Qh4dB5D/WpoUUp3lSod7qgoyEHbDGPUWjIbnqdqqe1k=",
        version = "v0.0.0-20190515093506-e2840ee46a6b",
    )
    go_repository(
        name = "org_modernc_cc_v3",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/cc/v3",
        sha256 = "fe3aeb761e55ce77a95b297321a122b4273aeffe1c08f48fc99310e065211f74",
        strip_prefix = "modernc.org/cc/v3@v3.40.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/cc/v3/org_modernc_cc_v3-v3.40.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/cc/v3/org_modernc_cc_v3-v3.40.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/cc/v3/org_modernc_cc_v3-v3.40.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/cc/v3/org_modernc_cc_v3-v3.40.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_ccgo_v3",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/ccgo/v3",
        sha256 = "bfc293300cd1ce656ba0ce0cee1f508afec2518bc4214a6b10ccfad6e8e6046e",
        strip_prefix = "modernc.org/ccgo/v3@v3.16.13",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/ccgo/v3/org_modernc_ccgo_v3-v3.16.13.zip",
            "http://ats.apps.svc/gomod/modernc.org/ccgo/v3/org_modernc_ccgo_v3-v3.16.13.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/ccgo/v3/org_modernc_ccgo_v3-v3.16.13.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/ccgo/v3/org_modernc_ccgo_v3-v3.16.13.zip",
        ],
    )
    go_repository(
        name = "org_modernc_ccorpus",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/ccorpus",
        sha256 = "3831b62a73a379b81ac927e17e3e9ffe2d44ad07c934505e1ae24eea8a26a6d3",
        strip_prefix = "modernc.org/ccorpus@v1.11.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/ccorpus/org_modernc_ccorpus-v1.11.6.zip",
            "http://ats.apps.svc/gomod/modernc.org/ccorpus/org_modernc_ccorpus-v1.11.6.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/ccorpus/org_modernc_ccorpus-v1.11.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/ccorpus/org_modernc_ccorpus-v1.11.6.zip",
        ],
    )
    go_repository(
        name = "org_modernc_golex",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/golex",
        sum = "h1:EYKY1a3wStt0RzHaH8mdSRNg78Ub0OHxYfCRWw35YtM=",
        version = "v1.0.1",
    )
    go_repository(
        name = "org_modernc_httpfs",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/httpfs",
        sha256 = "0b5314649c1327a199397eb6fd52b3ce41c9d3bc6dd2a4dea565b5fb87c13f41",
        strip_prefix = "modernc.org/httpfs@v1.0.6",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/httpfs/org_modernc_httpfs-v1.0.6.zip",
            "http://ats.apps.svc/gomod/modernc.org/httpfs/org_modernc_httpfs-v1.0.6.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/httpfs/org_modernc_httpfs-v1.0.6.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/httpfs/org_modernc_httpfs-v1.0.6.zip",
        ],
    )
    go_repository(
        name = "org_modernc_libc",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/libc",
        sha256 = "5f98bedf9f0663b3b87555904ee41b82fe9d8e9ac5c47c9fac9a42a7fe232313",
        strip_prefix = "modernc.org/libc@v1.22.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/libc/org_modernc_libc-v1.22.2.zip",
            "http://ats.apps.svc/gomod/modernc.org/libc/org_modernc_libc-v1.22.2.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/libc/org_modernc_libc-v1.22.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/libc/org_modernc_libc-v1.22.2.zip",
        ],
    )
    go_repository(
        name = "org_modernc_mathutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/mathutil",
        sum = "h1:ij3fYGe8zBF4Vu+g0oT7mB06r8sqGWKuJu1yXeR4by8=",
        version = "v1.4.1",
    )
    go_repository(
        name = "org_modernc_memory",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/memory",
        sha256 = "f79e8ada14c36d08817ee2bf6b2103f65c1a61a91b042b59016465869624043c",
        strip_prefix = "modernc.org/memory@v1.5.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/memory/org_modernc_memory-v1.5.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/memory/org_modernc_memory-v1.5.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/memory/org_modernc_memory-v1.5.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/memory/org_modernc_memory-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_opt",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/opt",
        sha256 = "294b1b80137cb86292c8893481d545eee90b17b84b6ad1dcb2e6c9bb523a2d9e",
        strip_prefix = "modernc.org/opt@v0.1.3",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/opt/org_modernc_opt-v0.1.3.zip",
            "http://ats.apps.svc/gomod/modernc.org/opt/org_modernc_opt-v0.1.3.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/opt/org_modernc_opt-v0.1.3.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/opt/org_modernc_opt-v0.1.3.zip",
        ],
    )
    go_repository(
        name = "org_modernc_parser",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/parser",
        sum = "h1:/qHLDn1ezrcRk9/XbErYp84bPPM4+w0kIDuvMdRk6Vc=",
        version = "v1.0.2",
    )
    go_repository(
        name = "org_modernc_sortutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/sortutil",
        sum = "h1:SUTM1sCR0Ldpv7dbB/KCPC2zHHsZ1KrSkhmGmmV22CQ=",
        version = "v1.0.0",
    )
    go_repository(
        name = "org_modernc_sqlite",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/sqlite",
        sha256 = "be0501f87458962a00c8fb07d1f131af010a534cd6ffb654c570be35b9b608d5",
        strip_prefix = "modernc.org/sqlite@v1.18.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/sqlite/org_modernc_sqlite-v1.18.2.zip",
            "http://ats.apps.svc/gomod/modernc.org/sqlite/org_modernc_sqlite-v1.18.2.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/sqlite/org_modernc_sqlite-v1.18.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/sqlite/org_modernc_sqlite-v1.18.2.zip",
        ],
    )
    go_repository(
        name = "org_modernc_strutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/strutil",
        sum = "h1:+1/yCzZxY2pZwwrsbH+4T7BQMoLQ9QiBshRC9eicYsc=",
        version = "v1.1.0",
    )
    go_repository(
        name = "org_modernc_tcl",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/tcl",
        sha256 = "f966db0dd1ccbc7f8d5ac2e752b64c3be343aa3f92215ed98b6f2a51b7abbb64",
        strip_prefix = "modernc.org/tcl@v1.13.2",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/tcl/org_modernc_tcl-v1.13.2.zip",
            "http://ats.apps.svc/gomod/modernc.org/tcl/org_modernc_tcl-v1.13.2.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/tcl/org_modernc_tcl-v1.13.2.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/tcl/org_modernc_tcl-v1.13.2.zip",
        ],
    )
    go_repository(
        name = "org_modernc_token",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/token",
        sha256 = "3efaa49e9fb10569da9e09e785fa230cd5c0f50fcf605f3b5439dfcd61577c58",
        strip_prefix = "modernc.org/token@v1.1.0",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/token/org_modernc_token-v1.1.0.zip",
            "http://ats.apps.svc/gomod/modernc.org/token/org_modernc_token-v1.1.0.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/token/org_modernc_token-v1.1.0.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/token/org_modernc_token-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_y",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/y",
        sum = "h1:+QT+MtLkwkvLkh3fYQq+YD5vw2s5paVE73jdl5R/Py8=",
        version = "v1.0.1",
    )
    go_repository(
        name = "org_modernc_z",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/z",
        sha256 = "5be23ef96669963e52d25b787d71028fff4fe1c468dec20aac59c9512caa2eb7",
        strip_prefix = "modernc.org/z@v1.5.1",
        urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/modernc.org/z/org_modernc_z-v1.5.1.zip",
            "http://ats.apps.svc/gomod/modernc.org/z/org_modernc_z-v1.5.1.zip",
            "https://cache.hawkingrei.com/gomod/modernc.org/z/org_modernc_z-v1.5.1.zip",
            "https://storage.googleapis.com/pingcapmirror/gomod/modernc.org/z/org_modernc_z-v1.5.1.zip",
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
        sum = "h1:9qC72Qh0+3MqyJbAn8YU5xVq1frD8bn3JtD2oXtafVQ=",
        version = "v1.10.0",
    )
    go_repository(
        name = "org_uber_go_automaxprocs",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/automaxprocs",
        sum = "h1:kWazyxZUrS3Gs4qUpbwo5kEIMGe/DAvi5Z4tl2NW4j8=",
        version = "v1.5.3",
    )
    go_repository(
        name = "org_uber_go_goleak",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/goleak",
        sum = "h1:xqgm/S+aQvhWFTtR0XK3Jvg7z8kGV8P4X14IzwN3Eqk=",
        version = "v1.2.0",
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
        sum = "h1:dg6GjLku4EH+249NNmoIciG9N/jURbDG+pFlTkhzIC8=",
        version = "v1.8.0",
    )
    go_repository(
        name = "org_uber_go_tools",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/tools",
        sum = "h1:0mgffUl7nfd+FpvXMVz4IDEaUSmT1ysygQC7qYo7sG4=",
        version = "v0.0.0-20190618225709-2cfd321de3ee",
    )
    go_repository(
        name = "org_uber_go_zap",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/zap",
        sum = "h1:OjGQ5KQDEUawVHxNwQgPpiypGHOxo2mNZsOqTak4fFY=",
        version = "v1.23.0",
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
