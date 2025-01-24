load("@bazel_gazelle//:deps.bzl", "go_repository")

def go_deps():
    # NOTE: We ensure that we pin to these specific dependencies by calling
    # this function FIRST, before calls to pull in dependencies for
    # third-party libraries (e.g. rules_go, gazelle, etc.)
    go_repository(
        name = "cc_mvdan_gofumpt",
        build_file_proto_mode = "disable_global",
        importpath = "mvdan.cc/gofumpt",
        sum = "h1:bg91ttqXmi9y2xawvkuMXyvAA/1ZGJqYAEGjXuP0JXU=",
        version = "v0.7.0",
    )
    go_repository(
        name = "cc_mvdan_unparam",
        build_file_proto_mode = "disable_global",
        importpath = "mvdan.cc/unparam",
        sum = "h1:lMpcwN6GxNbWtbpI1+xzFLSW8XzX0u72NttUGVFjO3U=",
        version = "v0.0.0-20240528143540-8a5130ca722f",
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
        sum = "h1:AHcMYuw56NPjq/2y615IGg2kYkBdTvOaojYCBcRE7MA=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_4d63_gochecknoglobals",
        build_file_proto_mode = "disable_global",
        importpath = "4d63.com/gochecknoglobals",
        sum = "h1:1eiorGsgHOFOuoOiJDy2psSrQbRdIHrlge0IJIkUgDc=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_4meepo_tagalign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/4meepo/tagalign",
        sum = "h1:GYTu2FaPGOGb/xJalcqHeD4il5BiCywyEYZOA55P6J4=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_github_abirdcfly_dupword",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Abirdcfly/dupword",
        sum = "h1:9Pa1NuAsZvpFPi9Pqkd93I7LIYRURj+A//dFd5tgBeE=",
        version = "v0.1.3",
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
        sum = "h1:xlwdaKcTNVW4PtpQb8aKA4Pjy0CdJHEqvFbAnvR5m2g=",
        version = "v0.0.0-20210112150236-f10218a38794",
    )
    go_repository(
        name = "com_github_ajstarks_deck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajstarks/deck",
        sum = "h1:7kQgkwGRoLzC9K0oyXdJo7nve/bynv/KwUsxbiTlzAM=",
        version = "v0.0.0-20200831202436-30c9fc6549a9",
    )
    go_repository(
        name = "com_github_ajstarks_deck_generate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajstarks/deck/generate",
        sum = "h1:iXUgAaqDcIUGbRoy2TdeofRG/j1zpGRSEmNK05T+bi8=",
        version = "v0.0.0-20210309230005-c3f852c02e19",
    )
    go_repository(
        name = "com_github_ajstarks_svgo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajstarks/svgo",
        sum = "h1:slYM766cy2nI3BwyRiyQj/Ud48djTMtMebDqepE95rw=",
        version = "v0.0.0-20211024235047-1546f124cd8b",
    )
    go_repository(
        name = "com_github_alecthomas_go_check_sumtype",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/go-check-sumtype",
        sum = "h1:u9aUvbGINJxLVXiFvHUlPEaD7VDULsrxJb4Aq31NLkU=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_alecthomas_kingpin_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/kingpin/v2",
        sum = "h1:f48lwail6p8zpO1bC4TxtqACaGqHYA22qkHjHpqDjYY=",
        version = "v2.4.0",
    )
    go_repository(
        name = "com_github_alecthomas_units",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/units",
        sum = "h1:ez/4by2iGztzR4L0zgAOR8lTQK9VlyBVVd7G4omaOQs=",
        version = "v0.0.0-20231202071711-9a357b53e9c9",
    )
    go_repository(
        name = "com_github_alexbrainman_sspi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alexbrainman/sspi",
        sum = "h1:Kk6a4nehpJ3UuJRqlA3JxYxBZEqCeOmATOvrbT4p9RA=",
        version = "v0.0.0-20210105120005-909beea2cc74",
    )
    go_repository(
        name = "com_github_alexkohler_nakedret_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alexkohler/nakedret/v2",
        sum = "h1:fP5qLgtwbx9EJE8dGEERT02YwS8En4r9nnZ71RK+EVU=",
        version = "v2.0.5",
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
        name = "com_github_alingse_nilnesserr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alingse/nilnesserr",
        sum = "h1:7cYuJewpy9jFNMEA72Q1+3Nm3zKHzg+Q28D5f2bBFUA=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_aliyun_alibaba_cloud_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aliyun/alibaba-cloud-sdk-go",
        sum = "h1:Q/yk4z/cHUVZfgTqtD09qeYBxHwshQAjVRX73qs8UH0=",
        version = "v1.61.1581",
    )
    go_repository(
        name = "com_github_andybalholm_brotli",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/andybalholm/brotli",
        sum = "h1:8uQZIdzKmjc/iuPu7O2ioW48L81FgatrcpfFmiq/cCs=",
        version = "v1.0.5",
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
        sum = "h1:oJOOWR07vS1kRusl6YRSlat7HFnb3mSfMl6sDMRoTBA=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_antonboom_nilnil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Antonboom/nilnil",
        sum = "h1:C3Tkm0KUxgfO4Duk3PM+ztPncTFlOf0b2qadmS0s4xs=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_antonboom_testifylint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Antonboom/testifylint",
        sum = "h1:4s3Xhuv5AvdIgbd8wOOEeo0uZG7PbDKQyKY5lGoQazk=",
        version = "v1.5.2",
    )
    go_repository(
        name = "com_github_apache_arrow_go_v12",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/apache/arrow/go/v12",
        sum = "h1:JsR2+hzYYjgSUkBSaahpqCetqZMr76djX80fF/DiJbg=",
        version = "v12.0.1",
    )
    go_repository(
        name = "com_github_apache_skywalking_eyes",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/apache/skywalking-eyes",
        sum = "h1:O13kdRU6FCEZevfD01mdhTgCZLLfPZIQ0GXZrLl7FpQ=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_apache_thrift",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/apache/thrift",
        sum = "h1:qEy6UW60iVOlUy+b9ZR0d5WzUWYGOo4HfopoyBaNmoY=",
        version = "v0.16.0",
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
        sum = "h1:hR91U9KYmb6bLBYLQjyM+3j+rcd/UhE+G78SFnF8gJA=",
        version = "v0.4.1",
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
        sum = "h1:DklsrG3dyBCFEj5IhUbnKptjxatkF07cF2ak3yi77so=",
        version = "v0.0.0-20230301143203-a9d515a09cc2",
    )
    go_repository(
        name = "com_github_ashanbrown_forbidigo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ashanbrown/forbidigo",
        sum = "h1:D3aewfM37Yb3pxHujIPSpTf6oQk9sc9WZi8gerOIVIY=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_ashanbrown_makezero",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ashanbrown/makezero",
        sum = "h1:/2Lp1bypdmK9wDIq7uWBlDF1iMUpIIS4A+pF6C9IEUU=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go",
        sum = "h1:KKUZBfBoyqy5d3swXyiC7Q76ic40rYcbqH7qjh59kzU=",
        version = "v1.55.5",
    )
    go_repository(
        name = "com_github_aymerick_douceur",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aymerick/douceur",
        sum = "h1:Mv+mAeH1Q+n9Fr+oyamOlAkUNPWPlA8PPGR0QAaYuPk=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azcore",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azcore",
        sum = "h1:1nGuui+4POelzDwI7RG56yfQJHCnKvwfMoU7VsEp+Zg=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azidentity",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azidentity",
        sum = "h1:U2rTu3Ef+7w9FHKIAXM6ZyqF3UOWJZ12zIm8zECAFfg=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_internal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/internal",
        sum = "h1:H+U3Gk9zY56G3u872L82bk4thcsy2Gghb9ExT4Zvm1o=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_resourcemanager_compute_armcompute_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5",
        sum = "h1:QfV5XZt6iNa2aWMAt96CZEbfJ7kgG/qYIpq465Shr5E=",
        version = "v5.4.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_resourcemanager_network_armnetwork_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4",
        sum = "h1:bXwSugBiSbgtz7rOtbfGf+woewp4f06orW9OP5BjHLA=",
        version = "v4.3.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_storage_azblob",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob",
        sum = "h1:u/LLAOFgsMv7HmNL4Qufg58y+qElGOt5qv0z1mURkRY=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_azure_go_ntlmssp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-ntlmssp",
        sum = "h1:mFRzDkZVAjdal+s7s0MwaRv9igoPqLRdzOLzw/8Xvq8=",
        version = "v0.0.0-20221128193559-754e69321358",
    )
    go_repository(
        name = "com_github_azuread_microsoft_authentication_library_for_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/AzureAD/microsoft-authentication-library-for-go",
        sum = "h1:XHOnouVk1mxXfQidrMEnLlPk9UMeRtyBTnEFtxkV0kU=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_bazelbuild_buildtools",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bazelbuild/buildtools",
        sum = "h1:4k69c5E7Sa7jmNtv9itBHYA4Z5pfurInuRrtgohxZeA=",
        version = "v0.0.0-20230926111657-7d855c59baeb",
    )
    go_repository(
        name = "com_github_bazelbuild_rules_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bazelbuild/rules_go",
        sum = "h1:CPn7VHaV3czTgk4LdEO+Od5DyYb6HXLL5CUIPignRLE=",
        version = "v0.42.1-0.20231101215950-df20c987afcb",
    )
    go_repository(
        name = "com_github_bboreham_go_loser",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bboreham/go-loser",
        sum = "h1:6df1vn4bBlDDo4tARvBm7l6KA9iVMnE3NWizDeWSrps=",
        version = "v0.0.0-20230920113527-fcc2c21820a3",
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
        name = "com_github_bits_and_blooms_bitset",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bits-and-blooms/bitset",
        sum = "h1:Gd2c8lSNf9pKXom5JtD7AaKO8o7fGQ2LtFj1436qilA=",
        version = "v1.14.3",
    )
    go_repository(
        name = "com_github_bkielbasa_cyclop",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bkielbasa/cyclop",
        sum = "h1:faIVMIGDIANuGPWH031CZJTi2ymOQBULs9H21HSMa5w=",
        version = "v1.2.3",
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
        sum = "h1:6I6oUiT/sU27eE2OFcWqBhL1SwjyvQuOssxT4a1yidI=",
        version = "v2.0.4",
    )
    go_repository(
        name = "com_github_bombsimon_wsl_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bombsimon/wsl/v4",
        sum = "h1:iZRsEvDdyhd2La0FVi5k6tYehpOR/R7qIUjmKk7N74A=",
        version = "v4.5.0",
    )
    go_repository(
        name = "com_github_boombuler_barcode",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/boombuler/barcode",
        sum = "h1:NDBbPmhS+EqABEs5Kg3n/5ZNjy73Pz7SIV+KCeqyXcs=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_breml_bidichk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/breml/bidichk",
        sum = "h1:xV4flJ9V5xWTqxL+/PMFF6dtJPvZLPsyixAoPe8BGJs=",
        version = "v0.3.2",
    )
    go_repository(
        name = "com_github_breml_errchkjson",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/breml/errchkjson",
        sum = "h1:gftf6uWZMtIa/Is3XJgibewBm2ksAQSY/kABDNFTAdk=",
        version = "v0.4.0",
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
        sum = "h1:mFgbEI6m+9W8oP/oDdfA34dLisRFCj2G6o/yiI1yZrY=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_butuzov_mirror",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/butuzov/mirror",
        sum = "h1:HdWCXzmwlQHdVhwvsfBb2Au0r3HyINry3bDWLYXiKoc=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_cakturk_go_netstat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cakturk/go-netstat",
        sum = "h1:BjkPE3785EwPhhyuFkbINB+2a1xATwk8SNDWnJiD41g=",
        version = "v0.0.0-20200220111822-e5b49efee7a5",
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
        sum = "h1:PGW5G/Kxn+YrN04cRAZKC+ZuvlVwolYMrIyyTJ/rMmc=",
        version = "v0.7.1",
    )
    go_repository(
        name = "com_github_ccojocar_zxcvbn_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ccojocar/zxcvbn-go",
        sum = "h1:na/czXU8RrhXO4EZme6eQJLR4PzcGsahsBOAwU6I3Vg=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_cenkalti_backoff_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cenkalti/backoff/v4",
        sum = "h1:y4OZtCnogmCPw98Zjyt5a6+QwPLGkiQsYW5oUqylYbM=",
        version = "v4.2.1",
    )
    go_repository(
        name = "com_github_census_instrumentation_opencensus_proto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/census-instrumentation/opencensus-proto",
        sum = "h1:iKLQ0xPNFxR/2hzXZMrBo8f1j86j5WHzznCCQxV/b8g=",
        version = "v0.4.1",
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
        sum = "h1:wgw73BiocdBDQPik+zcEoBG/ob8uyBHf2iyoHGPf5w4=",
        version = "v0.0.10",
    )
    go_repository(
        name = "com_github_chavacava_garif",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chavacava/garif",
        sum = "h1:2JHa3hbYf5D9dsgseMKAmc/MZ109otzgNFk5s87H9Pc=",
        version = "v0.1.0",
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
        sum = "h1:upd/6fQk4src78LMRzh5vItIt361/o4uq553V8B5sGI=",
        version = "v1.5.1",
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
        sum = "h1:V8gS/bTCCjX9uUnkUFUpPsksM8n1lXBAvHcpiFk1X2Y=",
        version = "v0.11.0",
    )
    go_repository(
        name = "com_github_ckaznocha_intrange",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ckaznocha/intrange",
        sum = "h1:VqnxtK32pxgkhJgYQEeOArVidIPg+ahLP7WBOXZd5ZY=",
        version = "v0.3.0",
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
        sum = "h1:gIc08FbB3QPb+nAQhINIK/qhf5REKkY0FTGgRGXkcVc=",
        version = "v1.3.6",
    )
    go_repository(
        name = "com_github_cloudykit_fastprinter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/CloudyKit/fastprinter",
        sum = "h1:sR+/8Yb4slttB4vD+b9btVEnWgL3Q00OBTzVT8B9C0c=",
        version = "v0.0.0-20200109182630-33d98a066a53",
    )
    go_repository(
        name = "com_github_cloudykit_jet_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/CloudyKit/jet/v6",
        sum = "h1:EpcZ6SR9n28BUGtNJSvlBqf90IpjeFr36Tizxhn/oME=",
        version = "v6.2.0",
    )
    go_repository(
        name = "com_github_cncf_udpa_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cncf/udpa/go",
        sum = "h1:hzAQntlaYRkVSFEfj9OTWlVV1H155FMD8BTKktLv0QI=",
        version = "v0.0.0-20210930031921-04548b0d99d4",
    )
    go_repository(
        name = "com_github_cncf_xds_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cncf/xds/go",
        sum = "h1:jQCWAUqqlij9Pgj2i/PB79y4KOPYVyFYdROxgaCwdTQ=",
        version = "v0.0.0-20231128003011-0fa0005c9caa",
    )
    go_repository(
        name = "com_github_cockroachdb_datadriven",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/datadriven",
        sum = "h1:otljaYPt5hWxV3MUfO5dFPFiOXg9CyG5/kCfayTqsJ4=",
        version = "v1.0.3-0.20230413201302-be42291fc80f",
    )
    go_repository(
        name = "com_github_cockroachdb_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/errors",
        sum = "h1:5bA+k2Y6r+oz/6Z/RFlNeVCesGARKuC6YymtcDrbC/I=",
        version = "v1.11.3",
    )
    go_repository(
        name = "com_github_cockroachdb_fifo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/fifo",
        sum = "h1:giXvy4KSc/6g/esnpM7Geqxka4WSqI1SZc7sMJFd3y4=",
        version = "v0.0.0-20240606204812-0bbfbd93a7ce",
    )
    go_repository(
        name = "com_github_cockroachdb_logtags",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/logtags",
        sum = "h1:r6VH0faHjZeQy818SGhaone5OnYfxFR/+AzdY3sf5aE=",
        version = "v0.0.0-20230118201751-21c54148d20b",
    )
    go_repository(
        name = "com_github_cockroachdb_pebble",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/pebble",
        sum = "h1:hQEoy4WiJNekmR/vD9/ALerTXyB/MVzb0fPE0P4ywno=",
        version = "v1.1.4-0.20250120151818-5dd133a1e6fb",
    )
    go_repository(
        name = "com_github_cockroachdb_redact",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/redact",
        sum = "h1:u1PMllDkdFfPWaNGMyLD1+so+aq3uUItthCFqzwPJ30=",
        version = "v1.1.5",
    )
    go_repository(
        name = "com_github_cockroachdb_tokenbucket",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/tokenbucket",
        sum = "h1:zuQyyAKVxetITBuuhv3BI9cMrmStnpT18zmgmTxunpo=",
        version = "v0.0.0-20230807174530-cc333fc44b06",
    )
    go_repository(
        name = "com_github_code_hex_go_generics_cache",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Code-Hex/go-generics-cache",
        sum = "h1:i8rLwyhoyhaerr7JpjtYjJZUcCbWOdiYO3fZXLiEC4g=",
        version = "v1.3.1",
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
        sum = "h1:S5ByHZ/h9PMe5IOQoN7E+nMc2UcLEM/V48DGDJ9kip0=",
        version = "v3.0.3",
    )
    go_repository(
        name = "com_github_containerd_log",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/log",
        sum = "h1:TCJt7ioM2cr/tfR8GPbGf9/VRAX8D2B4PjzCpfX540I=",
        version = "v0.1.0",
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
        name = "com_github_coreos_etcd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/etcd",
        sum = "h1:jFneRYjIvLMLhDLCzuTuU4rSJUjRplcJQ7pD7MnhC04=",
        version = "v3.3.10+incompatible",
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
        name = "com_github_coreos_go_systemd_v22",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-systemd/v22",
        sum = "h1:RrqgGjYQKalulkV8NGVIfkXQf6YYmOyiJKk8iXXhfZs=",
        version = "v22.5.0",
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
        sum = "h1:wfIWP927BUkWJb2NmU/kNDYIBTh/ziUX91+lVfRxZq4=",
        version = "v2.0.4",
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
        sum = "h1:zCh/wjc9oyeF+Gmp+V60wetm8ph2tlsxocgg/J0hOps=",
        version = "v0.5.3",
    )
    go_repository(
        name = "com_github_curioswitch_go_reassign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/curioswitch/go-reassign",
        sum = "h1:dh3kpQHuADL3cobV/sSGETA8DOv457dwl+fbBAhrQPs=",
        version = "v0.3.0",
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
        sum = "h1:MZRmHqDBd0vxNwenEbKSQqRVT24d3C05ft8kduSwlqM=",
        version = "v0.0.0-20181122101858-275e90344537",
    )
    go_repository(
        name = "com_github_daixiang0_gci",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/daixiang0/gci",
        sum = "h1:kThgmH1yBmZSBCh1EJVxQ7JsHpm5Oms0AMed/0LaH4c=",
        version = "v0.13.5",
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
        sum = "h1:oWf5W7GtOLgp6bciQYDmhHHjdhYkALu6S/5Ni9ZgSvQ=",
        version = "v1.5.5",
    )
    go_repository(
        name = "com_github_davecgh_go_spew",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/davecgh/go-spew",
        sum = "h1:U9qPSI2PIWSS1VwoXQT9A3Wy9MM3WgvqSxFWenqJduM=",
        version = "v1.1.2-0.20180830191138-d8f796af33cc",
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
        sum = "h1:SRdnP5ZKvcO9KKRP1KJrhFR3RrlGuD+42t4429eC9k8=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_dennwc_varint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dennwc/varint",
        sum = "h1:kGNFFSSw8ToIy3obO/kKr8U9GZYUAxQEVuix4zfDWzE=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_dgraph_io_ristretto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgraph-io/ristretto",
        sum = "h1:6CWw5tJNgpegArSHpNHJKldNeq03FQCwYvfMVWajOK8=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_dgryski_go_farm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgryski/go-farm",
        sum = "h1:aIftn67I1fkbMa512G+w+Pxci9hJPB8oMnkcP3iZF38=",
        version = "v0.0.0-20240924180020-3414d57e47da",
    )
    go_repository(
        name = "com_github_digitalocean_godo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/digitalocean/godo",
        sum = "h1:fWyMENvtxpCpva1UbKzOFnyAS04N1FNuBWWfPeTGquQ=",
        version = "v1.108.0",
    )
    go_repository(
        name = "com_github_distribution_reference",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/distribution/reference",
        sum = "h1:/FUIFXtfc/x2gpa5/VGfiGLuOIdYa1t65IKK2OFGvA0=",
        version = "v0.5.0",
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
        sum = "h1:ReYa/UBrRyQdant9B4fNHGoCNKw6qh6P0fsdGmZpR7c=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_docker_docker",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/docker",
        sum = "h1:g9b6wZTblhMgzOT2tspESstfw6ySZ9kdm94BLDKaZac=",
        version = "v25.0.0+incompatible",
    )
    go_repository(
        name = "com_github_docker_go_connections",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/go-connections",
        sum = "h1:El9xVISelRB7BuFusrZozjnkIM5YnzCViNKohAFqRJQ=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_docker_go_units",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/go-units",
        sum = "h1:69rxXcBk27SvSaaxTtLh/8llcHD8vYHT7WSdRZ/jvr4=",
        version = "v0.5.0",
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
        sum = "h1:bsQ7JsF4FkkWyrP3oCnFJgrCUAFbFf3kOl4L/QxPDyQ=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_dolthub_swiss",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dolthub/swiss",
        sum = "h1:gs2osYs5SJkAaH5/ggVJqXQxRXtWshF6uE0lgR/Y3Gw=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_dustin_go_humanize",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dustin/go-humanize",
        sum = "h1:GzkhY7T5VNhEkwH0PVJgjz+fX1rhBrR7pRT3mDkpeCY=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_ebitengine_purego",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ebitengine/purego",
        sum = "h1:sdRKd6plj7KYW33EH5As6YKfe8m9zbN9JMrOjNVF/BE=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_github_edsrzf_mmap_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/edsrzf/mmap-go",
        sum = "h1:6EUwBLQ/Mcr1EYLE4Tn1VdW1A4ckqCQWZBw8Hr0kjpQ=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_eknkc_amber",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eknkc/amber",
        sum = "h1:clC1lXBpe2kTj2VHdaIu9ajZQe4kcEY9j0NsnDDBZ3o=",
        version = "v0.0.0-20171010120322-cdade1c07385",
    )
    go_repository(
        name = "com_github_emicklei_go_restful_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/emicklei/go-restful/v3",
        sum = "h1:rAQeMHw1c7zTmncogyy8VvRZwtkmkZ4FxERmMY4rD+g=",
        version = "v3.11.0",
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
        sum = "h1:4X+VP1GHd1Mhj6IB5mMeGbLCleqxjletLK6K0rbxyZI=",
        version = "v0.12.0",
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sum = "h1:gVPz/FMfvh57HdSJQyvBtF00j8JU4zdyUgIUNhlgg0A=",
        version = "v1.0.4",
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
        sum = "h1:fGNiVF21fHXpX1niBgk0aROov1LagYsOwV/xqKDKR/Q=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_evanphx_json_patch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/evanphx/json-patch",
        sum = "h1:jBYDEEiFBPxA0v50tFdvOzQQTCvpL6mnFh5mB2/l16U=",
        version = "v5.6.0+incompatible",
    )
    go_repository(
        name = "com_github_facette_natsort",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/facette/natsort",
        sum = "h1:IT4JYU7k4ikYg1SCxNI1/Tieq/NFvh6dzLdgi7eu0tM=",
        version = "v0.0.0-20181210072756-2cd4dd1e2dcb",
    )
    go_repository(
        name = "com_github_fatih_color",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatih/color",
        sum = "h1:S8gINlzdQ840/4pfAwic/ZE0djQEH3wM94VfqLTZcOM=",
        version = "v1.18.0",
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
        sum = "h1:VvyZxILNuCiUCSXtPtYmmtGvb65nqXh2QFWc0Wpf2/g=",
        version = "v0.9.3",
    )
    go_repository(
        name = "com_github_felixge_httpsnoop",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/felixge/httpsnoop",
        sum = "h1:NFTV2Zj1bL4mc9sqWACXbQFVBBg2W3GPvqp8/ESS2Wg=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_firefart_nonamedreturns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/firefart/nonamedreturns",
        sum = "h1:tM+Me2ZaXs8tfdDw3X6DOX++wMCOqzYUho6tUTYIdRA=",
        version = "v1.0.5",
    )
    go_repository(
        name = "com_github_flosch_pongo2_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/flosch/pongo2/v4",
        sum = "h1:gv+5Pe3vaSVmiJvh/BZa82b7/00YUGm0PIyVVLop0Hw=",
        version = "v4.0.2",
    )
    go_repository(
        name = "com_github_fogleman_gg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fogleman/gg",
        sum = "h1:/7zJX8F6AaYQc57WQCyN9cAIz+4bCJGO9B+dyW29am8=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_frankban_quicktest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/frankban/quicktest",
        sum = "h1:FJKSZTDHjyhriyC81FLQ0LY93eSai0ZyR/ZIkd3ZUKE=",
        version = "v1.14.3",
    )
    go_repository(
        name = "com_github_fsnotify_fsnotify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fsnotify/fsnotify",
        sum = "h1:8JEhPFa5W2WU7YfeZzPNqzMP6Lwt7L2715Ggo0nosvA=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_fsouza_fake_gcs_server",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fsouza/fake-gcs-server",
        sum = "h1:Lw/mrvs45AfCUPVpry6qFkZnZPqe9thpLQHW+ZwHRLs=",
        version = "v1.44.0",
    )
    go_repository(
        name = "com_github_fzipp_gocyclo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fzipp/gocyclo",
        sum = "h1:lsblElZG7d3ALtGMx9fmxeTKZaLLpU8mET09yN4BBLo=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_gaijinentertainment_go_exhaustruct_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/GaijinEntertainment/go-exhaustruct/v3",
        sum = "h1:/fTUt5vmbkAcMBt4YQiuC23cV0kEsN1MVMNqeOW43cU=",
        version = "v3.3.0",
    )
    go_repository(
        name = "com_github_getsentry_sentry_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/getsentry/sentry-go",
        sum = "h1:Pv98CIbtB3LkMWmXi4Joa5OOcwbmnX88sF5qbK3r3Ps=",
        version = "v0.27.0",
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
        sum = "h1:LYcXbYvybUyTIxN2Mj9h6rHrDZBDwZloPoKctWrFyJY=",
        version = "v0.3.8",
    )
    go_repository(
        name = "com_github_gin_contrib_sse",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gin-contrib/sse",
        sum = "h1:Y/yl/+YNO8GZSjAhjMsSuLt29uWRFHdHYUb5lYOV9qE=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_gin_gonic_gin",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gin-gonic/gin",
        sum = "h1:4+fr/el88TOO3ewCmQr8cx/CtZ/umlIRIs5M4NTNjf8=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_github_go_asn1_ber_asn1_ber",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-asn1-ber/asn1-ber",
        sum = "h1:vXT6d/FNDiELJnLb6hGNa309LMsrCoYFvpwHDF0+Y1A=",
        version = "v1.5.4",
    )
    go_repository(
        name = "com_github_go_critic_go_critic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-critic/go-critic",
        sum = "h1:TkDTOn5v7EEngMxu8KbuFqFR43USaaH8XRJLz1jhVYA=",
        version = "v0.11.5",
    )
    go_repository(
        name = "com_github_go_errors_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-errors/errors",
        sum = "h1:J6MZopCL4uSllY1OfXM374weqZFFItUbrImctkmUxIA=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_go_fonts_dejavu",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-fonts/dejavu",
        sum = "h1:JSajPXURYqpr+Cu8U9bt8K+XcACIHWqWrvWCKyeFmVQ=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_go_fonts_latin_modern",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-fonts/latin-modern",
        sum = "h1:5/Tv1Ek/QCr20C6ZOz15vw3g7GELYL98KWr8Hgo+3vk=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_go_fonts_liberation",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-fonts/liberation",
        sum = "h1:jAkAWJP4S+OsrPLZM4/eC9iW7CtHy+HBXrEwZXWo5VM=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_go_fonts_stix",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-fonts/stix",
        sum = "h1:UlZlgrvvmT/58o573ot7NFw0vZasZ5I6bcIft/oMdgg=",
        version = "v0.1.0",
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
        name = "com_github_go_kit_kit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-kit/kit",
        sum = "h1:e4o3o3IsBfAKQh5Qbbiqyfu97Ku7jrO/JbohvztANh4=",
        version = "v0.12.0",
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
        sum = "h1:6zl3BbBhdnMkpSj2YY30qV3gDcVBGtFgVsV3+/i+mKQ=",
        version = "v0.0.0-20210823091927-c0d11ff05a81",
    )
    go_repository(
        name = "com_github_go_ldap_ldap_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-ldap/ldap/v3",
        replace = "github.com/YangKeao/ldap/v3",
        sum = "h1:+OqGGFc2YHFd82aSHmjlILVt1t4JWJjrNIfV8cVEPow=",
        version = "v3.4.5-0.20230421065457-369a3bab1117",
    )
    go_repository(
        name = "com_github_go_logfmt_logfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logfmt/logfmt",
        sum = "h1:wGYYu3uicYdqXVgoYbvnkrPVXkuLM1p1ifugDMEdRi4=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_go_logr_logr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logr/logr",
        sum = "h1:pKouT5E8xu9zeFC39JXRDukb6JFQPXM5p5I91188VAQ=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_github_go_logr_stdr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logr/stdr",
        sum = "h1:hSWxHoqTgW2S2qGc0LTAI563KZ5YKYRhT3MFKZMbjag=",
        version = "v1.2.2",
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
        sum = "h1:ZDFLvSNxpDaomuCueM0BlSXxpANBlFYiBvr+GXrvIHc=",
        version = "v0.21.4",
    )
    go_repository(
        name = "com_github_go_openapi_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/errors",
        sum = "h1:FhChC/duCnfoLj1gZ0BgaBmzhJC2SL/sJr8a2vAobSY=",
        version = "v0.21.0",
    )
    go_repository(
        name = "com_github_go_openapi_jsonpointer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonpointer",
        sum = "h1:ESKJdU9ASRfaPNOPRx12IUyA1vn3R9GiE3KYD14BXdQ=",
        version = "v0.20.0",
    )
    go_repository(
        name = "com_github_go_openapi_jsonreference",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonreference",
        sum = "h1:3sVjiK66+uXK/6oQ8xgcRKcFgQ5KXa2KvnJRumpMGbE=",
        version = "v0.20.2",
    )
    go_repository(
        name = "com_github_go_openapi_loads",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/loads",
        sum = "h1:r2a/xFIYeZ4Qd2TnGpWDIQNcP80dIaZgf704za8enro=",
        version = "v0.21.2",
    )
    go_repository(
        name = "com_github_go_openapi_spec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/spec",
        sum = "h1:xnlYNQAwKd2VQRRfwTEI0DcK+2cbuvI/0c7jx3gA8/8=",
        version = "v0.20.9",
    )
    go_repository(
        name = "com_github_go_openapi_strfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/strfmt",
        sum = "h1:Ew9PnEYc246TwrEspvBdDHS4BVKXy/AOVsfqGDgAcaI=",
        version = "v0.22.0",
    )
    go_repository(
        name = "com_github_go_openapi_swag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/swag",
        sum = "h1:QLMzNJnMGPRNDCbySlcj1x01tzU8/9LTTL9hZZZogBU=",
        version = "v0.22.4",
    )
    go_repository(
        name = "com_github_go_openapi_validate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/validate",
        sum = "h1:G+c2ub6q47kfX1sOBLwIQwzBVt8qmOAARyo/9Fqs9NU=",
        version = "v0.22.1",
    )
    go_repository(
        name = "com_github_go_pdf_fpdf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-pdf/fpdf",
        sum = "h1:MlgtGIfsdMEEQJr2le6b/HNr1ZlQwxyWr77r2aj2U/8=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_go_playground_locales",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/locales",
        sum = "h1:u50s323jtVGugKlcYeyzC0etD1HifMjqmJqb8WugfUU=",
        version = "v0.14.0",
    )
    go_repository(
        name = "com_github_go_playground_universal_translator",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/universal-translator",
        sum = "h1:82dyy6p4OuJq4/CByFNOn/jYrnRPArHwAcmLoJZxyho=",
        version = "v0.18.0",
    )
    go_repository(
        name = "com_github_go_playground_validator_v10",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/validator/v10",
        sum = "h1:prmOlTVv+YjZjmRmNSF3VmspqJIxJWXmqUsHwfTRRkQ=",
        version = "v10.11.1",
    )
    go_repository(
        name = "com_github_go_resty_resty_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-resty/resty/v2",
        sum = "h1:i7jMfNOJYMp69lq7qozJP+bjgzfAzeOhuGlyDrqxT/8=",
        version = "v2.11.0",
    )
    go_repository(
        name = "com_github_go_sql_driver_mysql",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-sql-driver/mysql",
        sum = "h1:lUIinVbN1DY0xBg0eMOzmmtGoHwWBbvnWubQUrtU8EI=",
        version = "v1.7.1",
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
        sum = "h1:tfuBGBXKqDEevZMzYi5KSi8KkcZtzBcTgAUUtapy0OI=",
        version = "v0.0.0-20230315185526-52ccab3ef572",
    )
    go_repository(
        name = "com_github_go_toolsmith_astcast",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astcast",
        sum = "h1:+JN9xZV1A+Re+95pgnMgDboWNVnIMMQXwfBwLRPgSC8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astcopy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astcopy",
        sum = "h1:YGwBN0WM+ekI/6SS6+52zLDEf8Yvp3n2seZITCUBt5s=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astequal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astequal",
        sum = "h1:3Fs3CYZ1k9Vo4FzFhwwewC3CHISHDnVUPC4x0bI2+Cw=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astfmt",
        sum = "h1:iJVPDPp6/7AaeLJEruMsBUlOYCmvg0MoCfJprsOmcco=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/astp",
        sum = "h1:dXPuCl6u2llURjdPLLDxJeZInAeZ0/eZwFJmqZMnpQA=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_strparse",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/strparse",
        sum = "h1:GAioeZUK9TGxnLS+qfdqNbA4z0SSm5zVNtCQiyP2Bvw=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_typep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-toolsmith/typep",
        sum = "h1:fIRYDyF+JywLfqzyhdiHzRop/GQDxxNhLGQ6gFUNHus=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_go_viper_mapstructure_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-viper/mapstructure/v2",
        sum = "h1:ZAaOCxANMuZx5RCeg0mBdEZk7DZasvvZIxtHqx8aGss=",
        version = "v2.2.1",
    )
    go_repository(
        name = "com_github_go_xmlfmt_xmlfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-xmlfmt/xmlfmt",
        sum = "h1:t8Ey3Uy7jDSEisW2K3somuMKIpzktkWptA0iFCnRUWY=",
        version = "v1.1.3",
    )
    go_repository(
        name = "com_github_go_zookeeper_zk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-zookeeper/zk",
        sum = "h1:7M2kwOsc//9VeeFiPtf+uSJlVpU66x9Ba5+8XK7/TDg=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_gobwas_glob",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/glob",
        sum = "h1:A4xDbljILXROh+kObIiy5kIaPYD8e96x1tgBhUI5J+Y=",
        version = "v0.2.3",
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
        sum = "h1:O0T8rZCuNmGXewnATuKYnkL0xm6o8UNOJZd/gOkb9ms=",
        version = "v1.2.0",
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
        sum = "h1:MTLVXXHf8ekldpJk3AKicLij9MdwOWkZ+a/jHHZby9E=",
        version = "v0.12.1",
    )
    go_repository(
        name = "com_github_gogo_googleapis",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/googleapis",
        sum = "h1:1Yx4Myt7BxzvUr5ldGSbwYiZG6t9wGBZ+8/fX3Wvtq0=",
        version = "v1.4.1",
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
        sum = "h1:uCdmnmatrKCgMBlM4rMuJZWOkPDqdbZPnrMXDY4gI68=",
        version = "v1.2.0",
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
        sum = "h1:73Z+4BJcrTC+KczS6WvTPvRGOp1WmfEP4Q1lOd9Z/+c=",
        version = "v3.2.1+incompatible",
    )
    go_repository(
        name = "com_github_golang_jwt_jwt_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-jwt/jwt/v4",
        sum = "h1:JdqV9zKUdtaa9gdPlywC3aeoEsR681PlKC+4F5gQgeo=",
        version = "v4.5.1",
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
        sum = "h1:ErTB+efbowRARo13NNdxyJji2egdxLGQhRaY+DUumQc=",
        version = "v1.6.0",
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
        name = "com_github_golangci_dupl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/dupl",
        sum = "h1:w8hkcTqaFpzKqonE9uMCefW1WDie15eSP/4MssdenaM=",
        version = "v0.0.0-20180902072040-3e9179ac440a",
    )
    go_repository(
        name = "com_github_golangci_go_printf_func_name",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/go-printf-func-name",
        sum = "h1:dVokQP+NMTO7jwO4bwsRwLWeudOVUPPyAKJuzv8pEJU=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_golangci_gofmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/gofmt",
        sum = "h1:t5wybL6RtO83VwoMOb7U/Peqe3gGKQlPIC66wXmnkvM=",
        version = "v0.0.0-20241223200906-057b0627d9b9",
    )
    go_repository(
        name = "com_github_golangci_golangci_lint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/golangci-lint",
        sum = "h1:bJQFQ3hSfUto597dkL7ipDzOxsGEpiWdLiZ359OWOBI=",
        version = "v1.63.4",
    )
    go_repository(
        name = "com_github_golangci_gosec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/gosec",
        sum = "h1:Bi7BYmZVg4C+mKGi8LeohcP2GGUl2XJD4xCkJoZSaYc=",
        version = "v0.0.0-20180901114220-8afd9cbb6cfb",
    )
    go_repository(
        name = "com_github_golangci_misspell",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/misspell",
        sum = "h1:JCle2HUTNWirNlDIAUO44hUsKhOFqGPoC4LZxlaSXDs=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_golangci_modinfo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/modinfo",
        sum = "h1:YBQDZpDMJpe5mtd0klUFYL8tSVkmF3cmm0fZ48sc7+s=",
        version = "v0.3.3",
    )
    go_repository(
        name = "com_github_golangci_plugin_module_register",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/plugin-module-register",
        sum = "h1:TCmesur25LnyJkpsVrupv1Cdzo+2f7zX0H6Jkw1Ol6c=",
        version = "v0.1.1",
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
        sum = "h1:3tL7c1XBMtWHHqVpS5ChmiAAoe4PF/d5+ULzV9sLAzs=",
        version = "v0.5.3",
    )
    go_repository(
        name = "com_github_golangci_unconvert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/unconvert",
        sum = "h1:IURFTjxeTfNFP0hTEi1YKjB/ub8zkpaOqFFMApi2EAs=",
        version = "v0.0.0-20240309020433-c5143eacb3ed",
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
        sum = "h1:ivUb1cGomAB101ZM1T0nOiWz9pSrTMoa9+EiY7igmkM=",
        version = "v2.0.8+incompatible",
    )
    go_repository(
        name = "com_github_google_gnostic_models",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/gnostic-models",
        sum = "h1:yo/ABAfM5IMRsS1VnXjTBvUb61tFIHozhlYvRgGre9I=",
        version = "v0.6.8",
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
        sum = "h1:qAf9yP0qc54ufQxzwv+u9H0tiVOnPJxo0lI/JXqw3ZM=",
        version = "v33.0.0",
    )
    go_repository(
        name = "com_github_google_go_pkcs11",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-pkcs11",
        sum = "h1:OF1IPgv+F4NmqmJ98KTjdN97Vs1JxDPB3vbmYzV2dpk=",
        version = "v0.2.1-0.20230907215043-c6f79328ddf9",
    )
    go_repository(
        name = "com_github_google_go_querystring",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-querystring",
        sum = "h1:AnCroh3fv4ZBgVIf1Iwtovgjaw/GiKJo8M8yD/fhyJ8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_google_gofuzz",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/gofuzz",
        sum = "h1:xRy4A+RhZaiKjJ1bPfwQ8sedCA+YS2YcCHW6ec7JMi0=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_google_licensecheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/licensecheck",
        sum = "h1:QoxgoDkaeC4nFrtGN1jV7IPmDCHFNIVh54e5hSt6sPs=",
        version = "v0.3.1",
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
        sum = "h1:Jaz2JzpQaQXyET0AjLBXShrthbpqMkhGiEfkcQAiAUs=",
        version = "v0.0.0-20241001023024-f4c0cfd0cf1d",
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
        sum = "h1:UifI23ZTGY8Tt29JbYFiuyIU3eX+RNFtUwefq9qAhxg=",
        version = "v2.0.0",
    )
    go_repository(
        name = "com_github_google_s2a_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/s2a-go",
        sum = "h1:60BLSyTrOV4/haCDW4zb1guZItoSq8foHCXrAnjBo/o=",
        version = "v0.1.7",
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
        sum = "h1:Vie5ybvEvT75RniqhfFxPRy3Bf7vr3h0cechB90XaQs=",
        version = "v0.3.2",
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/gax-go/v2",
        sum = "h1:mhN09QQW1jEWeMF74zGR81R30z4VJzjZsfkUhuHF+DA=",
        version = "v2.12.2",
    )
    go_repository(
        name = "com_github_gophercloud_gophercloud",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gophercloud/gophercloud",
        sum = "h1:TM3Jawprb2NrdOnvcHhWJalmKmAmOGgfZElM/3oBYCk=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_github_gordonklaus_ineffassign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gordonklaus/ineffassign",
        sum = "h1:y2Gd/9I7MdY1oEIt+n+rowjBNDcLQq3RsH5hwJd0f9s=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_gorilla_css",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/css",
        sum = "h1:BQqNyPTi50JCFMTw/b67hByjMVXZRwGha6wxVGkeihY=",
        version = "v1.0.0",
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
        sum = "h1:TuBL49tXwgrFYWhqrNgrUNEY92u81SPhu7sTdzQEiWY=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_github_gorilla_websocket",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/websocket",
        sum = "h1:gmztn0JnHVt9JZquRuzLw3g4wouNVzKL15iLr/zn/QY=",
        version = "v1.5.1",
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
        sum = "h1:Dq4wT1DdTwTGCQQv3rl3IvD5Ld0E6HiY+3Zh0sUGqw8=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_grafana_pyroscope_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grafana/pyroscope-go",
        sum = "h1:aILLKjTj8CS8f/24OPMGPewQSYlhmdQMBmol1d3KGj8=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_grafana_pyroscope_go_godeltaprof",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grafana/pyroscope-go/godeltaprof",
        sum = "h1:iwOtYXeeVSAeYefJNaxDytgjKtUuKQbJqgAIjlnicKg=",
        version = "v0.1.8",
    )
    go_repository(
        name = "com_github_grafana_regexp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grafana/regexp",
        sum = "h1:PpuIBO5P3e9hpqBD0O/HjhShYuM6XE0i/lbE6J94kww=",
        version = "v0.0.0-20221122212121-6b5c0a4cb7fd",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_middleware",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grpc-ecosystem/go-grpc-middleware",
        sum = "h1:UH//fgunKIs4JdUbpDl1VZCDaL56wXCB/5+wF6uHfaI=",
        version = "v1.4.0",
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
        sum = "h1:/c3QmbOGMGTOumP2iT/rCwB7b0QDGLKzqOmktBjT+Is=",
        version = "v2.19.1",
    )
    go_repository(
        name = "com_github_guptarohit_asciigraph",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/guptarohit/asciigraph",
        sum = "h1:ccFnUF8xYIOUPPY3tmdvRyHqmn1MYI9iv1pLKX+/ZkQ=",
        version = "v0.5.5",
    )
    go_repository(
        name = "com_github_hashicorp_consul_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/consul/api",
        sum = "h1:gmJ6DPKQog1426xsdmgk5iqDyoRiNc+ipBdJOqKQFjc=",
        version = "v1.27.0",
    )
    go_repository(
        name = "com_github_hashicorp_cronexpr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/cronexpr",
        sum = "h1:wG/ZYIKT+RT3QkOdgYc+xsKWVRgnxJ1OJtjjy84fJ9A=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_hashicorp_errwrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/errwrap",
        sum = "h1:OxrOeh75EUXMY8TBjag2fzXGZ40LB6IKw45YeGUDY2I=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_cleanhttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-cleanhttp",
        sum = "h1:035FKYIWjmULyFRBKPs8TBQoi0x6d9G4xc9neXJWAZQ=",
        version = "v0.5.2",
    )
    go_repository(
        name = "com_github_hashicorp_go_hclog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-hclog",
        sum = "h1:bI2ocEMgcVlz55Oj1xZNBsVi900c7II+fWDyV9o+13c=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_immutable_radix",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-immutable-radix",
        sum = "h1:DKHmCUm2hRBK510BaiZlwvpD40f8bJFeZnpfm2KLowc=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_hashicorp_go_immutable_radix_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-immutable-radix/v2",
        sum = "h1:CUW5RYIcysz+D3B+l1mDeXrQ7fUvGGCwJfdASSzbrfo=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_multierror",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-multierror",
        sum = "h1:H5DkEtf6CXdFp0N0Em5UCwQpXMWke8IA0+lD48awMYo=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_hashicorp_go_retryablehttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-retryablehttp",
        sum = "h1:ZQgVdpTdAL7WpMIwLzCfbalOcSUdkDZnpUv3/+BxzFA=",
        version = "v0.7.4",
    )
    go_repository(
        name = "com_github_hashicorp_go_rootcerts",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-rootcerts",
        sum = "h1:jzhAVGtqPKbwpyCPELlgNWhE1znq+qwJtW5Oi2viEzc=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_hashicorp_go_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-uuid",
        sum = "h1:d8T6WIONl4rMCPcQ/eY3uSz3+e4/GaoflKjXrWMex1U=",
        version = "v0.0.0-20180228145832-27454136f036",
    )
    go_repository(
        name = "com_github_hashicorp_go_version",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-version",
        sum = "h1:5tqGy27NaOTB8yJKUZELlFAS/LTKJkrmONwQKeRZfjY=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_hashicorp_golang_lru",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/golang-lru",
        sum = "h1:uL2shRDx7RTrOrTCUZEGP/wJUFiUI8QT6E7z5o8jga4=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_hashicorp_golang_lru_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/golang-lru/v2",
        sum = "h1:a+bsQ5rvGLjzHuww6tVxozPZFVghXaHOwFs4luLUK2k=",
        version = "v2.0.7",
    )
    go_repository(
        name = "com_github_hashicorp_hcl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/hcl",
        sum = "h1:0Anlzjpi4vEasTeNFn2mLJgTSwt0+6sfsiTG8qcWGx4=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_nomad_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/nomad/api",
        sum = "h1:Nc3Mt2BAnq0/VoLEntF/nipX+K1S7pG+RgwiitSv6v0=",
        version = "v0.0.0-20230721134942-515895c7690c",
    )
    go_repository(
        name = "com_github_hashicorp_serf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/serf",
        sum = "h1:Z1H2J60yRKvfDYAOZLd2MU0ND4AH/WDz7xYHDWQsIPY=",
        version = "v0.10.1",
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
        sum = "h1:RJOA2hHZ7rD1pScA4O1NF6qhkHyUdbbxjHgFNot8928=",
        version = "v2.6.0",
    )
    go_repository(
        name = "com_github_hexops_gotextdiff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hexops/gotextdiff",
        sum = "h1:gitA9+qJrrTCsiCl7+kh75nPqQt1cx4ZkudSTLoUqJM=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_huandu_xstrings",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/huandu/xstrings",
        sum = "h1:4jgBlKK6tLKFvO8u5pmYjG91cqytmDCDvGh7ECVFfFs=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_hydrogen18_memlistener",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hydrogen18/memlistener",
        sum = "h1:JR7eDj8HD6eXrc5fWLbSUnfcQFL06PYvCc0DKQnWfaU=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_ianlancetaylor_demangle",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ianlancetaylor/demangle",
        sum = "h1:KwWnWVWCNtNq/ewIX7HIKnELmEx2nDP42yskD/pi7QE=",
        version = "v0.0.0-20240312041847-bd984b5ce465",
    )
    go_repository(
        name = "com_github_imdario_mergo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/imdario/mergo",
        sum = "h1:wwQJbIsHYGMUyLSPrEq1CT16AhnhNJQ51+4fdHUnCl4=",
        version = "v0.3.16",
    )
    go_repository(
        name = "com_github_inconshreveable_mousetrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/inconshreveable/mousetrap",
        sum = "h1:wN+x4NVGpMsO7ErUn/mUI3vEoE6Jt13X2s0bqwp9tc8=",
        version = "v1.1.0",
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
        sum = "h1:J/uRN4UWO3wCyGOeDdMKv8LWRzKu6UIkLEaes38Kzh8=",
        version = "v6.1.11",
    )
    go_repository(
        name = "com_github_iris_contrib_schema",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/schema",
        sum = "h1:CPSBLyx2e91H2yJzPuhGuifVRnZBBJ3pCOMbOvPZaTw=",
        version = "v0.0.6",
    )
    go_repository(
        name = "com_github_jcmturner_gofork",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/gofork",
        sum = "h1:v4CYlQ+HeysPHsr2QFiEO60gKqnvn1xwvuKhhAhuEkk=",
        version = "v0.0.0-20180107083740-2aebee971930",
    )
    go_repository(
        name = "com_github_jedib0t_go_pretty_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jedib0t/go-pretty/v6",
        sum = "h1:o3McN0rQ4X+IU+HduppSp9TwRdGLRW2rhJXy9CJaCRw=",
        version = "v6.2.2",
    )
    go_repository(
        name = "com_github_jellydator_ttlcache_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jellydator/ttlcache/v3",
        sum = "h1:cHgCSMS7TdQcoprXnWUptJZzyFsqs18Lt8VVhRuZYVU=",
        version = "v3.0.1",
    )
    go_repository(
        name = "com_github_jfcg_opt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jfcg/opt",
        sum = "h1:6zgKvv3fR5OlX2nxUYJC4wtosY30N4vypILgXmRNr34=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_jfcg_rng",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jfcg/rng",
        sum = "h1:wCAgNN4UaNAL7pMHNkXjHzPuNkNmvVa0vzk5ntYl9gY=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_jfcg_sixb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jfcg/sixb",
        sum = "h1:BKPp/mIFCkKnnqhbgasI4wO/BYas6NHNcUCowUfTzSI=",
        version = "v1.3.8",
    )
    go_repository(
        name = "com_github_jfcg_sorty_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jfcg/sorty/v2",
        sum = "h1:EjrVSL3cDRxBt/ehiYCIv10F7YHYbTzEmdv7WbkkN1k=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_jgautheron_goconst",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jgautheron/goconst",
        sum = "h1:VpdAG7Ca7yvvJk5n8dMwQhfEZJh95kl/Hl9S1OI5Jkk=",
        version = "v1.7.1",
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
        sum = "h1:K317FqzuhWc8YvSVlFMCCUb36O/S9MCKRDI7QkRKD/E=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_jinzhu_now",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jinzhu/now",
        sum = "h1:/o9tlHleP7gOFmsnYNz3RGnqzefHA47wQpKrrdTIwXQ=",
        version = "v1.1.5",
    )
    go_repository(
        name = "com_github_jjti_go_spancheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jjti/go-spancheck",
        sum = "h1:Tl7gQpYf4/TMU7AT84MN83/6PutY21Nb9fuQjFTpRRc=",
        version = "v0.6.4",
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
        sum = "h1:O7syWuYGzre3s73s+NkgB8e0ZvsIVhT/zxNU7V1gHK8=",
        version = "v0.0.0-20230506070712-04da935ef877",
    )
    go_repository(
        name = "com_github_johncgriffin_overflow",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/JohnCGriffin/overflow",
        sum = "h1:RGWPOewvKIROun94nF7v2cua9qP+thov/7M50KEoeSU=",
        version = "v0.0.0-20211019200055-46fa312c352c",
    )
    go_repository(
        name = "com_github_joho_sqltocsv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/joho/sqltocsv",
        sum = "h1:Zrb0IbuLOGHL7nrO2WrcuNWgDTlzFv3zY69QMx4ggQE=",
        version = "v0.0.0-20210428211105-a6d6801d59df",
    )
    go_repository(
        name = "com_github_joker_jade",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Joker/jade",
        sum = "h1:Qbeh12Vq6BxURXT1qZBRHsDxeURB8ztcL6f3EXSGeHk=",
        version = "v1.1.3",
    )
    go_repository(
        name = "com_github_jonboulle_clockwork",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jonboulle/clockwork",
        sum = "h1:p4Cf1aMWXnXAUh8lVfewRBx1zaTSYKrKMF2g3ST4RZ4=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_josharian_intern",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/josharian/intern",
        sum = "h1:vlS4z54oSdjm0bgjRigI+G1HpF+tI+9rE5LLzOg8HmY=",
        version = "v1.0.0",
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
        sum = "h1:y+MJN/UdL63QbFJHws9BVC5RpA2iq0kpjrFajTGivjQ=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_jung_kurt_gofpdf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jung-kurt/gofpdf",
        sum = "h1:PJr+ZMXIecYc1Ey2zucXdR73SMBtgjPgwa31099IMv0=",
        version = "v1.0.3-0.20190309125859-24315acbbda5",
    )
    go_repository(
        name = "com_github_karamaru_alpha_copyloopvar",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/karamaru-alpha/copyloopvar",
        sum = "h1:x7gNyKcC2vRBO1H2Mks5u1VxQtYvFiym7fCjIP8RPos=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_kataras_blocks",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/blocks",
        sum = "h1:cF3RDY/vxnSRezc7vLFlQFTYXG/yAr1o7WImJuZbzC4=",
        version = "v0.0.7",
    )
    go_repository(
        name = "com_github_kataras_golog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/golog",
        sum = "h1:isP8th4PJH2SrbkciKnylaND9xoTtfxv++NB+DF0l9g=",
        version = "v0.1.8",
    )
    go_repository(
        name = "com_github_kataras_iris_v12",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/iris/v12",
        sum = "h1:WzDY5nGuW/LgVaFS5BtTkW3crdSKJ/FEgWnxPnIVVLI=",
        version = "v12.2.0",
    )
    go_repository(
        name = "com_github_kataras_pio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/pio",
        sum = "h1:kqreJ5KOEXGMwHAWHDwIl+mjfNCPhAwZPa8gK7MKlyw=",
        version = "v0.0.11",
    )
    go_repository(
        name = "com_github_kataras_sitemap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/sitemap",
        sum = "h1:w71CRMMKYMJh6LR2wTgnk5hSgjVNB9KL60n5e2KHvLY=",
        version = "v0.0.6",
    )
    go_repository(
        name = "com_github_kataras_tunnel",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/tunnel",
        sum = "h1:sCAqWuJV7nPzGrlb0os3j49lk2JhILT0rID38NHNLpA=",
        version = "v0.0.4",
    )
    go_repository(
        name = "com_github_kballard_go_shellquote",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kballard/go-shellquote",
        sum = "h1:Z9n2FFNUXsshfwJMBgNA0RU6/i7WVaAegv3PtuIHPMs=",
        version = "v0.0.0-20180428030007-95032a82bc51",
    )
    go_repository(
        name = "com_github_kimmachinegun_automemlimit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/KimMachineGun/automemlimit",
        sum = "h1:BeOe+BbJc8L5chL3OwzVYjVzyvPALdd5wxVVOWuUZmQ=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_kisielk_errcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kisielk/errcheck",
        patch_args = ["-p1"],
        patches = [
            "//build/patches:com_github_kisielk_errcheck.patch",
        ],
        sum = "h1:ZX/URYa7ilESY19ik/vBmCn6zdGQLxACwjAcWbHlYlg=",
        version = "v1.8.0",
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
        sum = "h1:CdnJh63tcDe53vG+RebdpdXJTc9atMgGqdx8LXxiilg=",
        version = "v1.1.5",
    )
    go_repository(
        name = "com_github_klauspost_asmfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/asmfmt",
        sum = "h1:4Ri7ox3EwapiOjCki+hw14RyKk201CN4rzyCJRFLpK4=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_github_klauspost_compress",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/compress",
        sum = "h1:In6xLpyWOi1+C7tXUUWv2ot1QvBjxevKAaI6IXrJmUc=",
        version = "v1.17.11",
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
        sum = "h1:lgaqFMSdTdQYdZ04uHyN2d/eKdOMyi2YLSvlQIBFYa4=",
        version = "v2.0.9",
    )
    go_repository(
        name = "com_github_kolo_xmlrpc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kolo/xmlrpc",
        sum = "h1:udzkj9S/zlT5X367kqJis0QP7YMxobob6zhzq6Yre00=",
        version = "v0.0.0-20220921171641-a4b6fa1dd06b",
    )
    go_repository(
        name = "com_github_konsorten_go_windows_terminal_sequences",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/konsorten/go-windows-terminal-sequences",
        sum = "h1:mweAR1A6xJ3oS2pRaGiHgQ4OO8tzTaLawm8vnODuwDk=",
        version = "v1.0.1",
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
        sum = "h1:Eg0fM56r4Gjp9PiK1Bg9agJUxCAWCk236qq9DItfLcw=",
        version = "v1.2.9",
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
        sum = "h1:wrodoaKYzS2mdNVnc4/w31YaXFtsc21PCTdvWJ/lDDs=",
        version = "v1.0.10",
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
        sum = "h1:1Z0bcmTypkL3Q4k+IDHMWTcnCliEZcaPiIe0/ymEyhQ=",
        version = "v0.1.11",
    )
    go_repository(
        name = "com_github_labstack_echo_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/labstack/echo/v4",
        sum = "h1:5CiyngihEO4HXsz3vVsJn7f8xAlWwRr3aY6Ih280ZKA=",
        version = "v4.10.0",
    )
    go_repository(
        name = "com_github_labstack_gommon",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/labstack/gommon",
        sum = "h1:y7cvthEAEbU0yHOf4axH8ZG2NH8knB9iNSoTO8dyIk8=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_lasiar_canonicalheader",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lasiar/canonicalheader",
        sum = "h1:vZ5uqwvDbyJCnMhmFYimgMZnJMjwljN5VGY0VKbMXb4=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_ldez_exptostd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ldez/exptostd",
        sum = "h1:90yWWoAKMFHeovTK8uzBms9Ppp8Du/xQ20DRO26Ymrw=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_ldez_gomoddirectives",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ldez/gomoddirectives",
        sum = "h1:Jyf1ZdTeiIB4dd+2n4qw+g4aI9IJ6JyfOZ8BityWvnA=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_ldez_grignotin",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ldez/grignotin",
        sum = "h1:vh0dI32WhHaq6LLPZ38g7WxXuZ1+RzyrJ7iPG9JMa8c=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_github_ldez_tagliatelle",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ldez/tagliatelle",
        sum = "h1:bTgKjjc2sQcsgPiT902+aadvMjCeMHrY7ly2XKFORIk=",
        version = "v0.7.1",
    )
    go_repository(
        name = "com_github_ldez_usetesting",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ldez/usetesting",
        sum = "h1:J2WwbrFGk3wx4cZwSMiCQQ00kjGR0+tuuyW0Lqm4lwA=",
        version = "v0.4.2",
    )
    go_repository(
        name = "com_github_leodido_go_urn",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/leodido/go-urn",
        sum = "h1:BqpAaACuzVSgi/VLzGZIobT2z4v53pjosyNd9Yv6n/w=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_leonklingele_grouper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/leonklingele/grouper",
        sum = "h1:o1ARBDLOmmasUaNDesWqWCIFH3u7hoFlM84YrjT3mIY=",
        version = "v1.1.2",
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
        name = "com_github_linode_linodego",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/linode/linodego",
        sum = "h1:KoQm5g2fppw8qIClJqUEL0yKH0+f+7te3Mewagb5QKE=",
        version = "v1.27.1",
    )
    go_repository(
        name = "com_github_lufia_plan9stats",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lufia/plan9stats",
        sum = "h1:N9zuLhTvBSRt0gWSiJswwQ2HqDmtX/ZCDJURnKUt1Ik=",
        version = "v0.0.0-20230326075908-cb1d2100619a",
    )
    go_repository(
        name = "com_github_macabu_inamedparam",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/macabu/inamedparam",
        sum = "h1:2tk/phHkMlEL/1GNe/Yf6kkR/hkcUdAEY3L0hjYV1Mk=",
        version = "v0.1.3",
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
        sum = "h1:5dmlB680ZkFG2RN/0lvTAghrSxIESeu9/2aeDqACtjw=",
        version = "v2.0.48",
    )
    go_repository(
        name = "com_github_mailru_easyjson",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mailru/easyjson",
        sum = "h1:UGYAvKxe3sBsEDzO8ZeWOSlIQfWFlxbzLZe7hwFURr0=",
        version = "v0.7.7",
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
        sum = "h1:S58XVV5AD7HADMmD0fNnziNHqKvSdDuEKdPD1rNTU04=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_masterminds_goutils",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/goutils",
        sum = "h1:5nUrii3FMTL5diU80unEVvNevw1nH4+ZV4DSLVJLSYI=",
        version = "v1.1.1",
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
        sum = "h1:B8LGeaivUe71a5qox1ICM/JLl0NqZSW5CHyL+hmvYS0=",
        version = "v3.3.0",
    )
    go_repository(
        name = "com_github_masterminds_sprig_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/sprig/v3",
        sum = "h1:17jRggJu518dr3QaafizSXOjKYp94wKfABxUmyxvxX8=",
        version = "v3.2.2",
    )
    go_repository(
        name = "com_github_matoous_godox",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/matoous/godox",
        sum = "h1:gWg6ZQ4JhDfJPqlo2srm/LN17lpybq15AryXIRcWYLE=",
        version = "v0.0.0-20230222163458-006bad1f9d26",
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
        sum = "h1:xfD0iDuEKnDkl03q4limB+vH+GxLEtL/jb4xVJSWWEY=",
        version = "v0.0.20",
    )
    go_repository(
        name = "com_github_mattn_go_runewidth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-runewidth",
        sum = "h1:E5ScNMtiwvlvB5paMFdw9p4kSQzbXFikJ5SQO6TULQc=",
        version = "v0.0.16",
    )
    go_repository(
        name = "com_github_mattn_go_sqlite3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-sqlite3",
        sum = "h1:vfoHhTN1af61xCRSWzFIWzx2YskyMTwHLrExkBOjvxI=",
        version = "v1.14.15",
    )
    go_repository(
        name = "com_github_matttproud_golang_protobuf_extensions",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/matttproud/golang_protobuf_extensions",
        sum = "h1:I0XW9+e1XWDxdcEniV4rQAIOPUGDq67JSCiRCgGCZLI=",
        version = "v1.0.2-0.20181231171920-c182affec369",
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
        sum = "h1:hE+QPeq0/wIzJwOphdVyUJ82njdd8Khp4fUIHGZHW3M=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_microcosm_cc_bluemonday",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/microcosm-cc/bluemonday",
        sum = "h1:SMZe2IGa0NuHvnVNAZ+6B38gsTbi5e4sViiWJyDDqFY=",
        version = "v1.0.23",
    )
    go_repository(
        name = "com_github_microsoft_go_winio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Microsoft/go-winio",
        sum = "h1:9/kr64B9VUZrLm5YYwbGtUJnMgqWVOdUAXu6Migciow=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_miekg_dns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/miekg/dns",
        sum = "h1:ca2Hdkz+cDg/7eNF6V56jjzuZ4aCAE+DbVkILdQWG/4=",
        version = "v1.1.58",
    )
    go_repository(
        name = "com_github_minio_asm2plan9s",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/minio/asm2plan9s",
        sum = "h1:AMFGa4R4MiIpspGNG7Z948v4n35fFGB3RR3G/ry4FWs=",
        version = "v0.0.0-20200509001527-cdd76441f9d8",
    )
    go_repository(
        name = "com_github_minio_c2goasm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/minio/c2goasm",
        sum = "h1:+n/aFZefKZp7spd8DFdX7uMikMLXX4oubIzJF4kv/wI=",
        version = "v0.0.0-20190812172519-36a3d3bbc4f3",
    )
    go_repository(
        name = "com_github_mitchellh_copystructure",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/copystructure",
        sum = "h1:Laisrj+bAB6b/yJwB5Bt3ITZhGJdqmxquMKeZ+mmkFQ=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_mitchellh_go_homedir",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-homedir",
        sum = "h1:lukF9ziXFxDFPkA1vsr5zpc1XuPDn/wFntq5mG+4E0Y=",
        version = "v1.1.0",
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
        sum = "h1:dcztxKSvZ4Id8iPpHERQBbIJfabdt4wUm5qy3wOL2Zc=",
        version = "v0.0.0-20210619224110-3f7ff695adc6",
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
        sum = "h1:odr8aZVFA3NZrNybggMkYO3rgPRcqjeQUlBBFVxKHTI=",
        version = "v0.3.2",
    )
    go_repository(
        name = "com_github_morikuni_aec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/morikuni/aec",
        sum = "h1:nP9CBfwrvYnBRgY6qfDQkygYDmYwOilePFkwzv4dU8A=",
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
        name = "com_github_nishanths_exhaustive",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nishanths/exhaustive",
        sum = "h1:vIY9sALmw6T/yxiASewa4TQcFsVYZQQRUQJhKRf3Swg=",
        version = "v0.12.0",
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
        sum = "h1:dOYG7LS/WK00RWZc8XGgcUTlTxpp3mKhdR2Q9z9HbXM=",
        version = "v0.0.0-20230430225905-43f6cf3098c1",
    )
    go_repository(
        name = "com_github_nunnatsa_ginkgolinter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nunnatsa/ginkgolinter",
        sum = "h1:zmX4KUR+6fk/vhUFt8DOP6KwznekhkmVSzzVJve2vyM=",
        version = "v0.18.4",
    )
    go_repository(
        name = "com_github_nxadm_tail",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nxadm/tail",
        sum = "h1:nPr65rt6Y5JFSKQO7qToXr7pePgD6Gwiw05lkbyAQTE=",
        version = "v1.4.8",
    )
    go_repository(
        name = "com_github_oklog_run",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/oklog/run",
        sum = "h1:GEenZ1cK0+q0+wsJew9qUg/DyD8k3JzYsZAi5gYi2mA=",
        version = "v1.1.0",
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
        sum = "h1:0jY9lJquiL8fcf3M4LAXN5aMlS/b2BV86HFFPCPMgE4=",
        version = "v2.13.0",
    )
    go_repository(
        name = "com_github_onsi_gomega",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/onsi/gomega",
        sum = "h1:KIA/t2t5UBzoirT4H9tsML45GEbo3ouUnBHsCfD2tVg=",
        version = "v1.29.0",
    )
    go_repository(
        name = "com_github_opencontainers_go_digest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/go-digest",
        sum = "h1:apOUWs51W5PlhuyGyz9FCeeBIOUDA/6nW8Oi/yOhh5U=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_opencontainers_image_spec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/image-spec",
        sum = "h1:9yCKha/T5XdGtO0q9Q9a6T5NUCsTn/DrBg0D7ufOcFM=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_opencontainers_runtime_spec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/runtime-spec",
        sum = "h1:UfAcuLBJB9Coz72x1hgl8O5RVzTdNiaglX6v2DM6FI0=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_openpeedeep_depguard_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/OpenPeeDeeP/depguard/v2",
        sum = "h1:vDfG60vDtIuf0MEOhmLlLLSzqaRM8EMcgJPdp74zmpA=",
        version = "v2.2.0",
    )
    go_repository(
        name = "com_github_opentracing_basictracer_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing/basictracer-go",
        sum = "h1:YyUAhaEfjoWXclZVJ9sGoNct7j4TVk7lZWlQw5UXuoo=",
        version = "v1.0.0",
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
        sum = "h1:dCI/t1iTdYGtkvCuBG2BgR6KZa83PTclw4U5n2wAllU=",
        version = "v1.14.0",
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
        sum = "h1:XaPLeE+9vGbuyEHem1JNk3bYc7KKqyI/na0/mLd/Kks=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_ovh_go_ovh",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ovh/go-ovh",
        sum = "h1:Gs3V823zwTFpzgGLZNI6ILS4rmxZgJwJCz54Er9LwD0=",
        version = "v1.4.3",
    )
    go_repository(
        name = "com_github_pbnjay_memory",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pbnjay/memory",
        sum = "h1:onHthvaw9LFnH4t2DcNVpwGmV9E1BkGknEliJkfwQj0=",
        version = "v0.0.0-20210728143218-7b4eea64cf58",
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
        sum = "h1:YmeHyLY8mFWbdkNWwpr+qIL2bEqT0o95WSdkNHvL12M=",
        version = "v2.2.3",
    )
    go_repository(
        name = "com_github_petermattis_goid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/petermattis/goid",
        sum = "h1:Dx7Ovyv/SFnMFw3fD4oEoeorXc6saIiQ23LrGLth0Gw=",
        version = "v0.0.0-20240813172612-4fcff4a6cae7",
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
        sum = "h1:KPKiIbfwbvC/wOncwhrpRdXVj2CZTCFlw4wnoyjtHfQ=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_phpdave11_gofpdi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/phpdave11/gofpdi",
        sum = "h1:o61duiW8M9sMlkVXWlvP92sZJtGKENvW3VExs6dZukQ=",
        version = "v1.0.13",
    )
    go_repository(
        name = "com_github_pierrec_lz4_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pierrec/lz4/v4",
        sum = "h1:MO0/ucJhngq7299dKLwIMtgTfbkoSPF6AoMYDd8Q4q0=",
        version = "v4.1.15",
    )
    go_repository(
        name = "com_github_pingcap_badger",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/badger",
        sum = "h1:eHcokyHxm7HVM+7+Qy1zZwC7NhX9wVNX8oQDcSZw1qI=",
        version = "v1.5.1-0.20241015064302-38533b6cbf8d",
    )
    go_repository(
        name = "com_github_pingcap_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/errors",
        sum = "h1:FxA+NgsdHNOv+/hZGxUh8Gb3WuZqgqmxDwztEOiA1v4=",
        version = "v0.11.5-0.20240318064555-6bd07397691f",
    )
    go_repository(
        name = "com_github_pingcap_failpoint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/failpoint",
        sum = "h1:tdMsjOqUR7YXHoBitzdebTvOjs/swniBTOLy5XiMtuE=",
        version = "v0.0.0-20240528011301-b51a646c7c86",
    )
    go_repository(
        name = "com_github_pingcap_fn",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/fn",
        sum = "h1:CyA6AxcOZkQh52wIqYlAmaVmF6EvrcqFywP463pjA8g=",
        version = "v1.0.0",
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
        sum = "h1:ThJ7ddLJVk96Iai2HDeyJGuuhrcBtc3HwYKJfuKPLsI=",
        version = "v0.0.0-20250108041715-3b77f2c65c63",
    )
    go_repository(
        name = "com_github_pingcap_log",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/log",
        sum = "h1:WIhmJBlNGmnCWH6TLMdZfNEDaiU8cFpZe3iaqDbQ0M8=",
        version = "v1.1.1-0.20241212030209-7e3ff8601a2a",
    )
    go_repository(
        name = "com_github_pingcap_sysutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/sysutil",
        sum = "h1:T4pXRhBflzDeAhmOQHNPRRogMYxP13V7BkYw3ZsoSfE=",
        version = "v1.0.1-0.20240311050922-ae81ee01f3a5",
    )
    go_repository(
        name = "com_github_pingcap_tipb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/tipb",
        sum = "h1:s58UXyaWMNeaoeuVPZdrkm5Uk7NcODHqICGCUQ3A9s4=",
        version = "v0.0.0-20241212101007-246f91188357",
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
        sum = "h1:5883YPCtkSd8LFbs13nXplj9g9tlrwoJRjgpgMu1/fE=",
        version = "v0.4.9",
    )
    go_repository(
        name = "com_github_pmezard_go_difflib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pmezard/go-difflib",
        sum = "h1:Jamvg5psRIccs7FGNTlIRMkT8wgtp5eCXdBlqhYGL6U=",
        version = "v1.0.1-0.20181226105442-5d4384ee4fb2",
    )
    go_repository(
        name = "com_github_polyfloyd_go_errorlint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/polyfloyd/go-errorlint",
        sum = "h1:Zp6lzCK4hpBDj8y8a237YK4EPrMXQWvOe3nGoH4pFrU=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_power_devops_perfstat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/power-devops/perfstat",
        sum = "h1:0LFwY6Q3gMACTjAbMZBjXAqTOzOwFaj2Ld6cjeQ7Rig=",
        version = "v0.0.0-20221212215047-62379fc7944b",
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
        sum = "h1:uOMJWfIwJguc3NaM3appWNbbrh6G/OjvaHMk22aBBYc=",
        version = "v0.26.0",
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
        sum = "h1:xasJaQlnWAeyHdUBeGjXmutelfJHWMRr+Fg4QszZ2Io=",
        version = "v0.62.0",
    )
    go_repository(
        name = "com_github_prometheus_common_assets",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/common/assets",
        sum = "h1:0P5OrzoHrYBOSM1OigWL3mY8ZvV2N4zIE/5AahrSrfM=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_prometheus_common_sigv4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/common/sigv4",
        sum = "h1:qoVebwtwwEhS85Czm2dSROY5fTo2PAPEVdDeppTwGX4=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_prometheus_exporter_toolkit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/exporter-toolkit",
        sum = "h1:yNTsuZ0aNCNFQ3aFTD2uhPOvr4iD7fdBvKPAEGkNf+g=",
        version = "v0.11.0",
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
        sum = "h1:N2L+DYrxqPh4WZStU+o1p/gQlBaqFbcLBTjlp3vpdXw=",
        version = "v0.50.1",
    )
    go_repository(
        name = "com_github_qri_io_jsonpointer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/qri-io/jsonpointer",
        sum = "h1:prVZBZLL6TW5vsSB9fFHFAMBLI4b0ri5vribQlTJiBA=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_qri_io_jsonschema",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/qri-io/jsonschema",
        sum = "h1:NNFoKms+kut6ABPf6xiKNM5214jzxAhDBrPHCJ97Wg0=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_quasilyte_go_ruleguard",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/go-ruleguard",
        sum = "h1:+Wl/0aFp0hpuHM3H//KMft64WQ1yX9LdJY64Qm/gFCo=",
        version = "v0.4.3-0.20240823090925-0fe6f58b47b1",
    )
    go_repository(
        name = "com_github_quasilyte_go_ruleguard_dsl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/go-ruleguard/dsl",
        sum = "h1:wd8zkOhSNr+I+8Qeciml08ivDt1pSXe60+5DqOpCjPE=",
        version = "v0.3.22",
    )
    go_repository(
        name = "com_github_quasilyte_gogrep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/gogrep",
        sum = "h1:eTKODPXbI8ffJMN+W2aE0+oL0z/nh8/5eNdiO34SOAo=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_quasilyte_regex_syntax",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/quasilyte/regex/syntax",
        sum = "h1:TCg2WBOl980XxGFEZSS6KlBGIV0diGdySzxATTWoqaU=",
        version = "v0.0.0-20210819130434-b3f0c404a727",
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
        sum = "h1:GnU+NsbiCqdC2XX5+vMZzP+jAJC5fht7rcVTAhX74UI=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_remyoudompheng_bigfft",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/remyoudompheng/bigfft",
        sum = "h1:W09IVJc94icq4NjY3clb7Lk8O1qJ8BdBEF8z0ibU0rE=",
        version = "v0.0.0-20230129092748-24d4a6f8daec",
    )
    go_repository(
        name = "com_github_rivo_uniseg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rivo/uniseg",
        sum = "h1:WUdvkW8uEhrYfLC4ZzdpI2ztxP1I582+49Oc5Mq64VQ=",
        version = "v0.4.7",
    )
    go_repository(
        name = "com_github_robfig_cron_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/robfig/cron/v3",
        sum = "h1:WdRxkvbJztn8LMz/QEvLN5sBU+xKpSqwwUO1Pjr4qDs=",
        version = "v3.0.1",
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
        sum = "h1:KvO1DLK/DRN07sQ1LQKScxyZJuNnedQ5/wKSR38lUII=",
        version = "v1.13.1",
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
        sum = "h1:K1Xf3bKttbF+koVGaX5xngRIZ5bVjbmPnaxE/dR08uY=",
        version = "v0.0.0-20201230142125-a7e3863a1245",
    )
    go_repository(
        name = "com_github_ryancurrah_gomodguard",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryancurrah/gomodguard",
        sum = "h1:cShyguSwUEeC0jS7ylOiG/idnd1TpJ1LfHGpV3oJmPU=",
        version = "v1.3.5",
    )
    go_repository(
        name = "com_github_ryanrolds_sqlclosecheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryanrolds/sqlclosecheck",
        sum = "h1:dibWW826u0P8jNLsLN+En7+RqWWTYrjCB9fJfSfdyCU=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_github_ryszard_goskiplist",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryszard/goskiplist",
        sum = "h1:GHRpF1pTW19a8tTFrMLUcfWwyC0pnifVo2ClaLq+hP8=",
        version = "v0.0.0-20150312221310-2dfbae5fcf46",
    )
    go_repository(
        name = "com_github_sanposhiho_wastedassign_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sanposhiho/wastedassign/v2",
        sum = "h1:crurBF7fJKIORrV85u9UUpePDYGWnwvv3+A96WvwXT0=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_santhosh_tekuri_jsonschema_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/santhosh-tekuri/jsonschema/v6",
        sum = "h1:PKK9DyHxif4LZo+uQSgXNqs0jj5+xZwwfKHgph2lxBw=",
        version = "v6.0.1",
    )
    go_repository(
        name = "com_github_sasha_s_go_deadlock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sasha-s/go-deadlock",
        sum = "h1:tNCOEEDG6tBqrNDOX35j/7hL5FcFViG6awUGROb2NsU=",
        version = "v0.3.5",
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
        sum = "h1:jZnudE2zKCtYlGzLVreNp5pmCdOxXUzwsMDBkR21cyQ=",
        version = "v1.28.0",
    )
    go_repository(
        name = "com_github_scaleway_scaleway_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/scaleway/scaleway-sdk-go",
        sum = "h1:wJrcTdddKOI8TFxs8cemnhKP2EmKy3yfUKHj3ZdfzYo=",
        version = "v1.0.0-beta.22",
    )
    go_repository(
        name = "com_github_schollz_closestmatch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/schollz/closestmatch",
        sum = "h1:Uel2GXEpJqOWBrlyI+oY9LTiyyjYS17cCYRqP13/SHk=",
        version = "v2.1.0+incompatible",
    )
    go_repository(
        name = "com_github_securego_gosec_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/securego/gosec/v2",
        sum = "h1:Le8MSj0PDmOnHJgUATjD96PaXRvCpKC+DGJvwyy0Mlk=",
        version = "v2.21.4",
    )
    go_repository(
        name = "com_github_segmentio_asm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/segmentio/asm",
        sum = "h1:9BQrFxC+YOHJlTlHGkTrFWf59nbL3XnCoFLTwDCI7ys=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_segmentio_fasthash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/segmentio/fasthash",
        sum = "h1:EI9+KE1EwvMLBWwjpRDc+fEM+prwxDYbslddQGtrmhM=",
        version = "v1.0.3",
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
        sum = "h1:WnNuhiq+FOY3jNj6JXFT+eLN3CQ/oPIsDPRanvwsmbI=",
        version = "v0.0.0-20190829150210-3e036491d500",
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
        sum = "h1:i0t8kL+kQTvpAYToeuiVk3TgDeKOFioZO3Ztz/iZ9pI=",
        version = "v3.24.5",
    )
    go_repository(
        name = "com_github_shirou_gopsutil_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shirou/gopsutil/v4",
        sum = "h1:qvePBOk20e0IKA1QXrIIU+jmk+zEiYVVx06WjBRlZo4=",
        version = "v4.24.12",
    )
    go_repository(
        name = "com_github_shoenig_go_m1cpu",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shoenig/go-m1cpu",
        sum = "h1:nxdKQNcEB6vzgA2E2bvzKIYRuNj7XNJ4S/aRSwKzFtM=",
        version = "v0.1.6",
    )
    go_repository(
        name = "com_github_shoenig_test",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shoenig/test",
        sum = "h1:kVTaSd7WLz5WZ2IaoM0RSzRsUD+m8wRR+5qvntpn4LU=",
        version = "v0.6.4",
    )
    go_repository(
        name = "com_github_shopify_goreferrer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Shopify/goreferrer",
        sum = "h1:KkH3I3sJuOLP3TjA/dfr4NAY8bghDwnXiU7cTKxQqo0=",
        version = "v0.0.0-20220729165902-8cddb4f5de06",
    )
    go_repository(
        name = "com_github_shopspring_decimal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shopspring/decimal",
        sum = "h1:abSATXmQEYyShuxI4/vyW3tV1MrKAJzCZ/0zLUXYbsQ=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_shurcool_httpfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/httpfs",
        sum = "h1:aqg5Vm5dwtvL+YgDpBcK1ITf3o96N/K7/wsRXQnUTEs=",
        version = "v0.0.0-20230704072500-f1e31cf0ba5c",
    )
    go_repository(
        name = "com_github_shurcool_httpgzip",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/httpgzip",
        sum = "h1:mj/nMDAwTBiaCqMEs4cYCqF7pO6Np7vhy1D1wcQGz+E=",
        version = "v0.0.0-20190720172056-320755c1c1b0",
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
        sum = "h1:dueUQJ1C2q9oE3F7wvmSGAaVtTmUizReu6fjN8uqzbQ=",
        version = "v1.9.3",
    )
    go_repository(
        name = "com_github_sivchari_containedctx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sivchari/containedctx",
        sum = "h1:x+etemjbsh2fB5ewm5FeLNi5bUjK0V8n0RB+Wwfd0XE=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_sivchari_tenv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sivchari/tenv",
        sum = "h1:+E0QzjktdnExv/wwsnnyk4oqZBUfuh89YMQT1cyuvSY=",
        version = "v1.12.1",
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
        sum = "h1:JjqOc2WN16ISWAjAk8M5ej0RfExEXtkEyExl2hLW+OM=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_sourcegraph_go_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sourcegraph/go-diff",
        sum = "h1:9uLlrd5T46OXs5qpp8L/MTltk0zikUGi0sNNyCpA8G0=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_github_spf13_afero",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/afero",
        sum = "h1:WJQKhtpdm3v2IzqG8VMqrr6Rf3UYpEF239Jy9wNepM8=",
        version = "v1.11.0",
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
        sum = "h1:e5/vxKd/rZsfSJMUX1agtjeTDf+qv1/JdBF8gg5k9ZM=",
        version = "v1.8.1",
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
        sum = "h1:i8pxvGrt1+4G0czLr/WnmyH7zbZ8Bg8etvARQ1rpyl4=",
        version = "v0.2.0",
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
        sum = "h1:Xv5erBjTwe/5IxqUQTdXv5kgmIvbHo3QQyRwhJsOfJA=",
        version = "v1.10.0",
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
        sum = "h1:LqDGgZdholxZMaJgpM6b0U9CFIjDCbFdUF00bDnBKOQ=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_tdewolff_minify_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tdewolff/minify/v2",
        sum = "h1:kejsHQMM17n6/gwdw53qsi6lg0TGddZADVyQOz1KMdE=",
        version = "v2.12.4",
    )
    go_repository(
        name = "com_github_tdewolff_parse_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tdewolff/parse/v2",
        sum = "h1:KCkDvNUMof10e3QExio9OPZJT8SbdKojLBumw8YZycQ=",
        version = "v2.6.4",
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
        sum = "h1:z/p8Ek55UdNvzt4TFn2zx2KscpW4rWqcnUrdmvWJj7E=",
        version = "v1.4.20",
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
        sum = "h1:J/YdBZ46WKpXsxsW93SG+q0F8KI+yFrcIDT4c/RNoc4=",
        version = "v0.0.0-20221230034425-4025bc8a4d4a",
    )
    go_repository(
        name = "com_github_tikv_client_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tikv/client-go/v2",
        sum = "h1:87RbBpXCsDIecDTIyXLF1kQR+WKMS6lL5lFCqyEaWEs=",
        version = "v2.0.8-0.20250117034919-61e09c6539bd",
    )
    go_repository(
        name = "com_github_tikv_pd_client",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tikv/pd/client",
        sum = "h1:fefo6U56UMca1CfwY/FVhfVcinyDgXgffQQp9lfMeLg=",
        version = "v0.0.0-20250107032658-5c4ab57d68de",
    )
    go_repository(
        name = "com_github_timakin_bodyclose",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/timakin/bodyclose",
        sum = "h1:y4mJRFlM6fUyPhoXuFg/Yu02fg/nIPFMOY8tOqppoFg=",
        version = "v0.0.0-20241017074812-ed6a65f985e3",
    )
    go_repository(
        name = "com_github_timonwong_loggercheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/timonwong/loggercheck",
        sum = "h1:uVZYClxQFpw55eh+PIoqM7uAOHMrhVcDoWDery9R8Lg=",
        version = "v0.10.1",
    )
    go_repository(
        name = "com_github_tklauser_go_sysconf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tklauser/go-sysconf",
        sum = "h1:0QaGUFOdQaIVdPgfITYzaTegZvdCjmYO52cSFAEVmqU=",
        version = "v0.3.12",
    )
    go_repository(
        name = "com_github_tklauser_numcpus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tklauser/numcpus",
        sum = "h1:ng9scYS7az0Bk4OZLvrNXNSAO2Pxr1XXRAPyjhIx+Fk=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_tmc_grpc_websocket_proxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tmc/grpc-websocket-proxy",
        sum = "h1:6fotK7otjonDflCTK0BCfls4SPy3NcCVb5dqqmbRknE=",
        version = "v0.0.0-20220101234140-673ab2c3ae75",
    )
    go_repository(
        name = "com_github_tomarrell_wrapcheck_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tomarrell/wrapcheck/v2",
        sum = "h1:SzRCryzy4IrAH7bVGG4cK40tNUhmVmMDuJujy4XwYDg=",
        version = "v2.10.0",
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
        sum = "h1:mqrRot1BRxm+Yct+vavLMou2/iJt0tNVTTC0QoIjaZg=",
        version = "v1.1.6",
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
        name = "com_github_ugorji_go_codec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ugorji/go/codec",
        sum = "h1:YPXUKf7fYbp/y8xloBqZOw2qaVggbfwMlI8WM3wZUJ0=",
        version = "v1.2.7",
    )
    go_repository(
        name = "com_github_ultraware_funlen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ultraware/funlen",
        sum = "h1:gCHmCn+d2/1SemTdYMiKLAHFYxTYz7z9VIDRaTGyLkI=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_ultraware_whitespace",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ultraware/whitespace",
        sum = "h1:TYowo2m9Nfj1baEQBjuHzvMRbp19i+RCcRYrSWoFa+g=",
        version = "v0.2.0",
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
        sum = "h1:3BU9aMr1xbhPlvJLSydKwdLN3tEUUrzPSSM8S4hDYRA=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_uudashr_iface",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uudashr/iface",
        sum = "h1:zwPch0fs9tdh9BmL5kcgSpvnObV+yHjO4JjVBl8IA10=",
        version = "v1.3.0",
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
        sum = "h1:CRq/00MfruPGFLTQKY8b+8SfdK60TxNztjRMnH0t1Yc=",
        version = "v1.40.0",
    )
    go_repository(
        name = "com_github_valyala_fasttemplate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/fasttemplate",
        sum = "h1:lxLXG0uE3Qnshl9QyaK6XJxMXlQZELvChBOCmQD0Loo=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_valyala_quicktemplate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/quicktemplate",
        sum = "h1:zU0tjbIqTRgKQzFY1L42zq0qR3eh4WoQQdIdqCysW5k=",
        version = "v1.8.0",
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
        sum = "h1:5gO0H1iULLWGhs2H5tbAHIZTV8/cYafcFOr9znI5mJU=",
        version = "v5.3.5",
    )
    go_repository(
        name = "com_github_vmihailenco_tagparser_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vmihailenco/tagparser/v2",
        sum = "h1:y09buUbR+b5aycVFQs/g70pqKVZNBmxwAhO7/IwNM9g=",
        version = "v2.0.0",
    )
    go_repository(
        name = "com_github_vultr_govultr_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vultr/govultr/v2",
        sum = "h1:gej/rwr91Puc/tgh+j33p/BLR16UrIPnSr+AIwYWZQs=",
        version = "v2.17.2",
    )
    go_repository(
        name = "com_github_wangjohn_quickselect",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/wangjohn/quickselect",
        sum = "h1:9DDCDwOyEy/gId+IEMrFHLuQ5R/WV0KNxWLler8X2OY=",
        version = "v0.0.0-20161129230411-ed8402a42d5f",
    )
    go_repository(
        name = "com_github_xen0n_gosmopolitan",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xen0n/gosmopolitan",
        sum = "h1:/p2KTnMzwRexIW8GlKawsTWOxn7UHA+jCMF/V8HHtvU=",
        version = "v1.2.2",
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
        sum = "h1:S2dVYn90KE98chqDkyE9Z4N61UnQd+KOfgp5Iu53llk=",
        version = "v0.0.0-20221125231312-a49e3df8f510",
    )
    go_repository(
        name = "com_github_xitongsys_parquet_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xitongsys/parquet-go",
        sum = "h1:mr+7gGPUasLmH3/5Iv1zwQwiY0WgGO21Ym7Q4FVw+xs=",
        version = "v1.6.3-0.20240520233950-75e935fc3e17",
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
        name = "com_github_yangkeao_go_mysql_driver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/YangKeao/go-mysql-driver",
        sum = "h1:mx7rQczQb38AWgiFJsuwvygvOnktsz/mknqUJNByB1Q=",
        version = "v0.0.0-20240627104025-dd5589458cfa",
    )
    go_repository(
        name = "com_github_yeya24_promlinter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yeya24/promlinter",
        sum = "h1:JVDbMp08lVCP7Y6NP3qHroGAO6z2yGKQtS5JsjqtoFs=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_ykadowak_zerologlint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ykadowak/zerologlint",
        sum = "h1:Gy/fMz1dFQN9JZTPjv1hxEk+sRWm05row04Yoolgdiw=",
        version = "v0.1.5",
    )
    go_repository(
        name = "com_github_yosssi_ace",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yosssi/ace",
        sum = "h1:tUkIP/BLdKqrlrPwcmH0shwEEhTRHoGnc1wFIWmaBUA=",
        version = "v0.0.5",
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
        sum = "h1:zFUKzehAFReQwLys1b/iSMl+JQGSCSjtVqQn9bBrPo0=",
        version = "v1.2.4",
    )
    go_repository(
        name = "com_github_zeebo_assert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/zeebo/assert",
        sum = "h1:g7C04CbJuIDKNPFHmsk4hwZDO5O+kntRxzaUoNXj+IQ=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_zeebo_xxh3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/zeebo/xxh3",
        sum = "h1:xZmwmqxHZA8AI603jOQ0tMqmBr9lPeFwGg6d+xy9DC0=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_zyedidia_generic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/zyedidia/generic",
        sum = "h1:Zv5KS/N2m0XZZiuLS82qheRG4X1o5gsWreGb0hR7XDc=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_gitlab_bosi_decorder",
        build_file_proto_mode = "disable_global",
        importpath = "gitlab.com/bosi/decorder",
        sum = "h1:qbQaV3zgwnBZ4zPMhGLW4KZe7A7NwxEhJx39R3shffo=",
        version = "v0.4.2",
    )
    go_repository(
        name = "com_google_cloud_go",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go",
        sum = "h1:uJSeirPke5UNZHIb4SxfZklVSiWWVqW4oXlETwZziwM=",
        version = "v0.112.1",
    )
    go_repository(
        name = "com_google_cloud_go_accessapproval",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/accessapproval",
        sum = "h1:uzmAMSgYcnlHa9X9YSQZ4Q1wlfl4NNkZyQgho1Z6p04=",
        version = "v1.7.5",
    )
    go_repository(
        name = "com_google_cloud_go_accesscontextmanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/accesscontextmanager",
        sum = "h1:2GLNaNu9KRJhJBFTIVRoPwk6xE5mUDgD47abBq4Zp/I=",
        version = "v1.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_aiplatform",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/aiplatform",
        sum = "h1:0cSrii1ZeLr16MbBoocyy5KVnrSdiQ3KN/vtrTe7RqE=",
        version = "v1.60.0",
    )
    go_repository(
        name = "com_google_cloud_go_analytics",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/analytics",
        sum = "h1:Q+y94XH84jM8SK8O7qiY/PJRexb6n7dRbQ6PiUa4YGM=",
        version = "v0.23.0",
    )
    go_repository(
        name = "com_google_cloud_go_apigateway",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigateway",
        sum = "h1:sPXnpk+6TneKIrjCjcpX5YGsAKy3PTdpIchoj8/74OE=",
        version = "v1.6.5",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeconnect",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigeeconnect",
        sum = "h1:CrfIKv9Go3fh/QfQgisU3MeP90Ww7l/sVGmr3TpECo8=",
        version = "v1.6.5",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeregistry",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/apigeeregistry",
        sum = "h1:C+QU2K+DzDjk4g074ouwHQGkoff1h5OMQp6sblCVreQ=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_appengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/appengine",
        sum = "h1:l2SviT44zWQiOv8bPoMBzW0vOcMO22iO0s+nVtVhdts=",
        version = "v1.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_area120",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/area120",
        sum = "h1:vTs08KPLN/iMzTbxpu5ciL06KcsrVPMjz4IwcQyZ4uY=",
        version = "v0.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_artifactregistry",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/artifactregistry",
        sum = "h1:W9sVlyb1VRcUf83w7aM3yMsnp4HS4PoyGqYQNG0O5lI=",
        version = "v1.14.7",
    )
    go_repository(
        name = "com_google_cloud_go_asset",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/asset",
        sum = "h1:xgFnBP3luSbUcC9RWJvb3Zkt+y/wW6PKwPHr3ssnIP8=",
        version = "v1.17.2",
    )
    go_repository(
        name = "com_google_cloud_go_assuredworkloads",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/assuredworkloads",
        sum = "h1:gCrN3IyvqY3cP0wh2h43d99CgH3G+WYs9CeuFVKChR8=",
        version = "v1.11.5",
    )
    go_repository(
        name = "com_google_cloud_go_automl",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/automl",
        sum = "h1:ijiJy9sYWh75WrqImXsfWc1e3HR3iO+ef9fvW03Ig/4=",
        version = "v1.13.5",
    )
    go_repository(
        name = "com_google_cloud_go_baremetalsolution",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/baremetalsolution",
        sum = "h1:LFydisRmS7hQk9P/YhekwuZGqb45TW4QavcrMToWo5A=",
        version = "v1.2.4",
    )
    go_repository(
        name = "com_google_cloud_go_batch",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/batch",
        sum = "h1:2HK4JerwVaIcCh/lJiHwh6+uswPthiMMWhiSWLELayk=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_beyondcorp",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/beyondcorp",
        sum = "h1:qs0J0O9Ol2h1yA0AU+r7l3hOCPzs2MjE1d6d/kaHIKo=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/bigquery",
        sum = "h1:CpT+/njKuKT3CEmswm6IbhNu9u35zt5dO4yPDLW+nG4=",
        version = "v1.59.1",
    )
    go_repository(
        name = "com_google_cloud_go_billing",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/billing",
        sum = "h1:oWUEQvuC4JvtnqLZ35zgzdbuHt4Itbftvzbe6aEyFdE=",
        version = "v1.18.2",
    )
    go_repository(
        name = "com_google_cloud_go_binaryauthorization",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/binaryauthorization",
        sum = "h1:1jcyh2uIUwSZkJ/JmL8kd5SUkL/Krbv8zmYLEbAz6kY=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_certificatemanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/certificatemanager",
        sum = "h1:UMBr/twXvH3jcT5J5/YjRxf2tvwTYIfrpemTebe0txc=",
        version = "v1.7.5",
    )
    go_repository(
        name = "com_google_cloud_go_channel",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/channel",
        sum = "h1:/omiBnyFjm4S1ETHoOmJbL7LH7Ljcei4rYG6Sj3hc80=",
        version = "v1.17.5",
    )
    go_repository(
        name = "com_google_cloud_go_cloudbuild",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/cloudbuild",
        sum = "h1:ZB6oOmJo+MTov9n629fiCrO9YZPOg25FZvQ7gIHu5ng=",
        version = "v1.15.1",
    )
    go_repository(
        name = "com_google_cloud_go_clouddms",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/clouddms",
        sum = "h1:Sr0Zo5EAcPQiCBgHWICg3VGkcdS/LLP1d9SR7qQBM/s=",
        version = "v1.7.4",
    )
    go_repository(
        name = "com_google_cloud_go_cloudtasks",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/cloudtasks",
        sum = "h1:EUt1hIZ9bLv8Iz9yWaCrqgMnIU+Tdh0yXM1MMVGhjfE=",
        version = "v1.12.6",
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/compute",
        sum = "h1:phWcR2eWzRJaL/kOiJwfFsPs4BaKq1j6vnpZrc1YlVg=",
        version = "v1.24.0",
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
        sum = "h1:6Vs/YnDG5STGjlWMEjN/xtmft7MrOTOnOZYUZtGTx0w=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_container",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/container",
        sum = "h1:MAaNH7VRNPWEhvqOypq2j+7ONJKrKzon4v9nS3nLZe0=",
        version = "v1.31.0",
    )
    go_repository(
        name = "com_google_cloud_go_containeranalysis",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/containeranalysis",
        sum = "h1:doJ0M1ljS4hS0D2UbHywlHGwB7sQLNrt9vFk9Zyi7vY=",
        version = "v0.11.4",
    )
    go_repository(
        name = "com_google_cloud_go_datacatalog",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datacatalog",
        sum = "h1:A0vKYCQdxQuV4Pi0LL9p39Vwvg4jH5yYveMv50gU5Tw=",
        version = "v1.19.3",
    )
    go_repository(
        name = "com_google_cloud_go_dataflow",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataflow",
        sum = "h1:RYHtcPhmE664+F0Je46p+NvFbG8z//KCXp+uEqB4jZU=",
        version = "v0.9.5",
    )
    go_repository(
        name = "com_google_cloud_go_dataform",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataform",
        sum = "h1:5e4eqGrd0iDTCg4Q+VlAao5j2naKAA7xRurNtwmUknU=",
        version = "v0.9.2",
    )
    go_repository(
        name = "com_google_cloud_go_datafusion",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datafusion",
        sum = "h1:HQ/BUOP8OIGJxuztpYvNvlb+/U+/Bfs9SO8tQbh61fk=",
        version = "v1.7.5",
    )
    go_repository(
        name = "com_google_cloud_go_datalabeling",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datalabeling",
        sum = "h1:GpIFRdm0qIZNsxqURFJwHt0ZBJZ0nF/mUVEigR7PH/8=",
        version = "v0.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_dataplex",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataplex",
        sum = "h1:fxIfdU8fxzR3clhOoNI7XFppvAmndxDu1AMH+qX9WKQ=",
        version = "v1.14.2",
    )
    go_repository(
        name = "com_google_cloud_go_dataproc_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataproc/v2",
        sum = "h1:/u81Fd+BvCLp+xjctI1DiWVJn6cn9/s3Akc8xPH02yk=",
        version = "v2.4.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataqna",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dataqna",
        sum = "h1:9ybXs3nr9BzxSGC04SsvtuXaHY0qmJSLIpIAbZo9GqQ=",
        version = "v0.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datastore",
        sum = "h1:0P9WcsQeTWjuD1H14JIY7XQscIPQ4Laje8ti96IC5vg=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_datastream",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datastream",
        sum = "h1:o1QDKMo/hk0FN7vhoUQURREuA0rgKmnYapB+1M+7Qz4=",
        version = "v1.10.4",
    )
    go_repository(
        name = "com_google_cloud_go_deploy",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/deploy",
        sum = "h1:m27Ojwj03gvpJqCbodLYiVmE9x4/LrHGGMjzc0LBfM4=",
        version = "v1.17.1",
    )
    go_repository(
        name = "com_google_cloud_go_dialogflow",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dialogflow",
        sum = "h1:KqG0oxGE71qo0lRVyAoeBozefCvsMfcDzDjoLYSY0F4=",
        version = "v1.49.0",
    )
    go_repository(
        name = "com_google_cloud_go_dlp",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/dlp",
        sum = "h1:lTipOuJaSjlYnnotPMbEhKURLC6GzCMDDzVbJAEbmYM=",
        version = "v1.11.2",
    )
    go_repository(
        name = "com_google_cloud_go_documentai",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/documentai",
        sum = "h1:lI62GMEEPO6vXJI9hj+G9WjOvnR0hEjvjokrnex4cxA=",
        version = "v1.25.0",
    )
    go_repository(
        name = "com_google_cloud_go_domains",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/domains",
        sum = "h1:Mml/R6s3vQQvFPpi/9oX3O5dRirgjyJ8cksK8N19Y7g=",
        version = "v0.9.5",
    )
    go_repository(
        name = "com_google_cloud_go_edgecontainer",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/edgecontainer",
        sum = "h1:tBY32km78ScpK2aOP84JoW/+wtpx5WluyPUSEE3270U=",
        version = "v1.1.5",
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
        sum = "h1:13eHn5qBnsawxI7mIrv4jRIEmQ1xg0Ztqw5ZGqtUNfA=",
        version = "v1.6.6",
    )
    go_repository(
        name = "com_google_cloud_go_eventarc",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/eventarc",
        sum = "h1:ORkd6/UV5FIdA8KZQDLNZYKS7BBOrj0p01DXPmT4tE4=",
        version = "v1.13.4",
    )
    go_repository(
        name = "com_google_cloud_go_filestore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/filestore",
        sum = "h1:X5G4y/vrUo1B8Nsz93qSWTMAcM8LXbGUldq33OdcdCw=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/firestore",
        sum = "h1:8aLcKnMPoldYU3YHgu4t2exrKhLQkqaXAGqT0ljrFVw=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_functions",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/functions",
        sum = "h1:IWVylmK5F6hJ3R5zaRW7jI5PrWhCvtBVU4axQLmXSo4=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkebackup",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkebackup",
        sum = "h1:iuE8KNtTsPOc79qeWoNS8zOWoXPD9SAdOmwgxtlCmh8=",
        version = "v1.3.5",
    )
    go_repository(
        name = "com_google_cloud_go_gkeconnect",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkeconnect",
        sum = "h1:17d+ZSSXKqG/RwZCq3oFMIWLPI8Zw3b8+a9/BEVlwH0=",
        version = "v0.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_gkehub",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkehub",
        sum = "h1:RboLNFzf9wEMSo7DrKVBlf+YhK/A/jrLN454L5Tz99Q=",
        version = "v0.14.5",
    )
    go_repository(
        name = "com_google_cloud_go_gkemulticloud",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gkemulticloud",
        sum = "h1:rsSZAGLhyjyE/bE2ToT5fqo1qSW7S+Ubsc9jFOcbhSI=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_google_cloud_go_gsuiteaddons",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/gsuiteaddons",
        sum = "h1:CZEbaBwmbYdhFw21Fwbo+C35HMe36fTE0FBSR4KSfWg=",
        version = "v1.6.5",
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iam",
        sum = "h1:bEa06k05IO4f4uJonbB5iAgKTPpABy1ayxaIZV/GHVc=",
        version = "v1.1.6",
    )
    go_repository(
        name = "com_google_cloud_go_iap",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iap",
        sum = "h1:94zirc2r4t6KzhAMW0R6Dme005eTP6yf7g6vN4IhRrA=",
        version = "v1.9.4",
    )
    go_repository(
        name = "com_google_cloud_go_ids",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/ids",
        sum = "h1:xd4U7pgl3GHV+MABnv1BF4/Vy/zBF7CYC8XngkOLzag=",
        version = "v1.4.5",
    )
    go_repository(
        name = "com_google_cloud_go_iot",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iot",
        sum = "h1:munTeBlbqI33iuTYgXy7S8lW2TCgi5l1hA4roSIY+EE=",
        version = "v1.7.5",
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/kms",
        sum = "h1:7caV9K3yIxvlQPAcaFffhlT7d1qpxjB1wHBtjWa13SM=",
        version = "v1.15.7",
    )
    go_repository(
        name = "com_google_cloud_go_language",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/language",
        sum = "h1:iaJZg6K4j/2PvZZVcjeO/btcWWIllVRBhuTFjGO4LXs=",
        version = "v1.12.3",
    )
    go_repository(
        name = "com_google_cloud_go_lifesciences",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/lifesciences",
        sum = "h1:gXvN70m2p+4zgJFzaz6gMKaxTuF9WJ0USYoMLWAOm8g=",
        version = "v0.9.5",
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/logging",
        sum = "h1:iEIOXFO9EmSiTjDmfpbRjOxECO7R8C7b8IXUGOj7xZw=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_longrunning",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/longrunning",
        sum = "h1:GOE6pZFdSrTb4KAiKnXsJBtlE6mEyaW44oKyMILWnOg=",
        version = "v0.5.5",
    )
    go_repository(
        name = "com_google_cloud_go_managedidentities",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/managedidentities",
        sum = "h1:+bpih1piZVLxla/XBqeSUzJBp8gv9plGHIMAI7DLpDM=",
        version = "v1.6.5",
    )
    go_repository(
        name = "com_google_cloud_go_maps",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/maps",
        sum = "h1:EVCZAiDvog9So46460BGbCasPhi613exoaQbpilMVlk=",
        version = "v1.6.4",
    )
    go_repository(
        name = "com_google_cloud_go_mediatranslation",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/mediatranslation",
        sum = "h1:c76KdIXljQHSCb/Cy47S8H4s05A4zbK3pAFGzwcczZo=",
        version = "v0.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_memcache",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/memcache",
        sum = "h1:yeDv5qxRedFosvpMSEswrqUsJM5OdWvssPHFliNFTc4=",
        version = "v1.10.5",
    )
    go_repository(
        name = "com_google_cloud_go_metastore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/metastore",
        sum = "h1:dR7vqWXlK6IYR8Wbu9mdFfwlVjodIBhd1JRrpZftTEg=",
        version = "v1.13.4",
    )
    go_repository(
        name = "com_google_cloud_go_monitoring",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/monitoring",
        sum = "h1:NfkDLQDG2UR3WYZVQE8kwSbUIEyIqJUPl+aOQdFH1T4=",
        version = "v1.18.0",
    )
    go_repository(
        name = "com_google_cloud_go_networkconnectivity",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networkconnectivity",
        sum = "h1:GBfXFhLyPspnaBE3nI/BRjdhW8vcbpT9QjE/4kDCDdc=",
        version = "v1.14.4",
    )
    go_repository(
        name = "com_google_cloud_go_networkmanagement",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networkmanagement",
        sum = "h1:aLV5GcosBNmd6M8+a0ekB0XlLRexv4fvnJJrYnqeBcg=",
        version = "v1.9.4",
    )
    go_repository(
        name = "com_google_cloud_go_networksecurity",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/networksecurity",
        sum = "h1:+caSxBTj0E8OYVh/5wElFdjEMO1S/rZtE1152Cepchc=",
        version = "v0.9.5",
    )
    go_repository(
        name = "com_google_cloud_go_notebooks",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/notebooks",
        sum = "h1:FH48boYmrWVQ6k0Mx/WrnNafXncT5iSYxA8CNyWTgy0=",
        version = "v1.11.3",
    )
    go_repository(
        name = "com_google_cloud_go_optimization",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/optimization",
        sum = "h1:63NZaWyN+5rZEKHPX4ACpw3BjgyeuY8+rCehiCMaGPY=",
        version = "v1.6.3",
    )
    go_repository(
        name = "com_google_cloud_go_orchestration",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/orchestration",
        sum = "h1:YHgWMlrPttIVGItgGfuvO2KM7x+y9ivN/Yk92pMm1a4=",
        version = "v1.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_orgpolicy",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/orgpolicy",
        sum = "h1:2JbXigqBJVp8Dx5dONUttFqewu4fP0p3pgOdIZAhpYU=",
        version = "v1.12.1",
    )
    go_repository(
        name = "com_google_cloud_go_osconfig",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/osconfig",
        sum = "h1:Mo5jGAxOMKH/PmDY7fgY19yFcVbvwREb5D5zMPQjFfo=",
        version = "v1.12.5",
    )
    go_repository(
        name = "com_google_cloud_go_oslogin",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/oslogin",
        sum = "h1:1K4nOT5VEZNt7XkhaTXupBYos5HjzvJMfhvyD2wWdFs=",
        version = "v1.13.1",
    )
    go_repository(
        name = "com_google_cloud_go_phishingprotection",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/phishingprotection",
        sum = "h1:DH3WFLzEoJdW/6xgsmoDqOwT1xddFi7gKu0QGZQhpGU=",
        version = "v0.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_policytroubleshooter",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/policytroubleshooter",
        sum = "h1:c0WOzC6hz964QWNBkyKfna8A2jOIx1zzZa43Gx/P09o=",
        version = "v1.10.3",
    )
    go_repository(
        name = "com_google_cloud_go_privatecatalog",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/privatecatalog",
        sum = "h1:UZ0assTnATXSggoxUIh61RjTQ4P9zCMk/kEMbn0nMYA=",
        version = "v0.9.5",
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/pubsub",
        sum = "h1:dfEPuGCHGbWUhaMCTHUFjfroILEkx55iUmKBZTP5f+Y=",
        version = "v1.36.1",
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
        sum = "h1:U3Wfq12X9cVMuTpsWDSURnXF0Z9hSPTHj+xsnXDRLsw=",
        version = "v2.9.2",
    )
    go_repository(
        name = "com_google_cloud_go_recommendationengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recommendationengine",
        sum = "h1:ineqLswaCSBY0csYv5/wuXJMBlxATK6Xc5jJkpiTEdM=",
        version = "v0.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_recommender",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/recommender",
        sum = "h1:LVLYS3r3u0MSCxQSDUtLSkporEGi9OAE6hGvayrZNPs=",
        version = "v1.12.1",
    )
    go_repository(
        name = "com_google_cloud_go_redis",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/redis",
        sum = "h1:QF0maEdVv0Fj/2roU8sX3NpiDBzP9ICYTO+5F32gQNo=",
        version = "v1.14.2",
    )
    go_repository(
        name = "com_google_cloud_go_resourcemanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/resourcemanager",
        sum = "h1:AZWr1vWVDKGwfLsVhcN+vcwOz3xqqYxtmMa0aABCMms=",
        version = "v1.9.5",
    )
    go_repository(
        name = "com_google_cloud_go_resourcesettings",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/resourcesettings",
        sum = "h1:BTr5MVykJwClASci/7Og4Qfx70aQ4n3epsNLj94ZYgw=",
        version = "v1.6.5",
    )
    go_repository(
        name = "com_google_cloud_go_retail",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/retail",
        sum = "h1:Fn1GuAua1c6crCGqfJ1qMxG1Xh10Tg/x5EUODEHMqkw=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_google_cloud_go_run",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/run",
        sum = "h1:m9WDA7DzTpczhZggwYlZcBWgCRb+kgSIisWn1sbw2rQ=",
        version = "v1.3.4",
    )
    go_repository(
        name = "com_google_cloud_go_scheduler",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/scheduler",
        sum = "h1:5U8iXLoQ03qOB+ZXlAecU7fiE33+u3QiM9nh4cd0eTE=",
        version = "v1.10.6",
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/secretmanager",
        sum = "h1:82fpF5vBBvu9XW4qj0FU2C6qVMtj1RM/XHwKXUEAfYY=",
        version = "v1.11.5",
    )
    go_repository(
        name = "com_google_cloud_go_security",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/security",
        sum = "h1:wTKJQ10j8EYgvE8Y+KhovxDRVDk2iv/OsxZ6GrLP3kE=",
        version = "v1.15.5",
    )
    go_repository(
        name = "com_google_cloud_go_securitycenter",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/securitycenter",
        sum = "h1:/5jjkZ+uGe8hZ7pvd7pO30VW/a+pT2MrrdgOqjyucKQ=",
        version = "v1.24.4",
    )
    go_repository(
        name = "com_google_cloud_go_servicedirectory",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/servicedirectory",
        sum = "h1:da7HFI1229kyzIyuVEzHXip0cw0d+E0s8mjQby0WN+k=",
        version = "v1.11.4",
    )
    go_repository(
        name = "com_google_cloud_go_shell",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/shell",
        sum = "h1:3Fq2hzO0ZSyaqBboJrFkwwf/qMufDtqwwA6ep8EZxEI=",
        version = "v1.7.5",
    )
    go_repository(
        name = "com_google_cloud_go_spanner",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/spanner",
        sum = "h1:fJq+ZfQUDHE+cy1li0bJA8+sy2oiSGhuGqN5nqVaZdU=",
        version = "v1.57.0",
    )
    go_repository(
        name = "com_google_cloud_go_speech",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/speech",
        sum = "h1:nuFc+Kj5B8de75nN4FdPyUbI2SiBoHZG6BLurXL56Q0=",
        version = "v1.21.1",
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/storage",
        sum = "h1:Az68ZRGlnNTpIBbLjSMIV2BDcwwXYlRlQzis0llkpJg=",
        version = "v1.38.0",
    )
    go_repository(
        name = "com_google_cloud_go_storagetransfer",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/storagetransfer",
        sum = "h1:dy4fL3wO0VABvzM05ycMUPFHxTPbJz9Em8ikAJVqSbI=",
        version = "v1.10.4",
    )
    go_repository(
        name = "com_google_cloud_go_talent",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/talent",
        sum = "h1:JssV0CE3FNujuSWn7SkosOzg7qrMxVnt6txOfGcMSa4=",
        version = "v1.6.6",
    )
    go_repository(
        name = "com_google_cloud_go_texttospeech",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/texttospeech",
        sum = "h1:dxY2Q5mHCbrGa3oPR2O3PCicdnvKa1JmwGQK36EFLOw=",
        version = "v1.7.5",
    )
    go_repository(
        name = "com_google_cloud_go_tpu",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/tpu",
        sum = "h1:C8YyYda8WtNdBoCgFwwBzZd+S6+EScHOxM/z1h0NNp8=",
        version = "v1.6.5",
    )
    go_repository(
        name = "com_google_cloud_go_trace",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/trace",
        sum = "h1:0pr4lIKJ5XZFYD9GtxXEWr0KkVeigc3wlGpZco0X1oA=",
        version = "v1.10.5",
    )
    go_repository(
        name = "com_google_cloud_go_translate",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/translate",
        sum = "h1:upovZ0wRMdzZvXnu+RPam41B0mRJ+coRXFP2cYFJ7ew=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_video",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/video",
        sum = "h1:TXwotxkShP1OqgKsbd+b8N5hrIHavSyLGvYnLGCZ7xc=",
        version = "v1.20.4",
    )
    go_repository(
        name = "com_google_cloud_go_videointelligence",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/videointelligence",
        sum = "h1:mYaWH8uhUCXLJCN3gdXswKzRa2+lK0zN6/KsIubm6pE=",
        version = "v1.11.5",
    )
    go_repository(
        name = "com_google_cloud_go_vision_v2",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vision/v2",
        sum = "h1:W52z1b6LdGI66MVhE70g/NFty9zCYYcjdKuycqmlhtg=",
        version = "v2.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_vmmigration",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vmmigration",
        sum = "h1:5v9RT2vWyuw3pK2ox0HQpkoftO7Q7/8591dTxxQc79g=",
        version = "v1.7.5",
    )
    go_repository(
        name = "com_google_cloud_go_vmwareengine",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vmwareengine",
        sum = "h1:EGdDi9QbqThfZq3ILcDK5g+m9jTevc34AY5tACx5v7k=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_google_cloud_go_vpcaccess",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/vpcaccess",
        sum = "h1:XyL6hTLtEM/eE4F1GEge8xUN9ZCkiVWn44K/YA7z1rQ=",
        version = "v1.7.5",
    )
    go_repository(
        name = "com_google_cloud_go_webrisk",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/webrisk",
        sum = "h1:251MvGuC8wisNN7+jqu9DDDZAi38KiMXxOpA/EWy4dE=",
        version = "v1.9.5",
    )
    go_repository(
        name = "com_google_cloud_go_websecurityscanner",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/websecurityscanner",
        sum = "h1:YqWZrZYabG88TZt7364XWRJGhxmxhony2ZUyZEYMF2k=",
        version = "v1.6.5",
    )
    go_repository(
        name = "com_google_cloud_go_workflows",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/workflows",
        sum = "h1:uHNmUiatTbPQ4H1pabwfzpfEYD4BBnqDHqMm2IesOh4=",
        version = "v1.12.4",
    )
    go_repository(
        name = "com_lukechampine_uint128",
        build_file_proto_mode = "disable_global",
        importpath = "lukechampine.com/uint128",
        sum = "h1:mBi/5l91vocEN8otkC5bDLhi2KdCticRiwbdB0O+rjI=",
        version = "v1.2.0",
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
        name = "ht_sr_git_sbinet_gg",
        build_file_proto_mode = "disable",
        importpath = "git.sr.ht/~sbinet/gg",
        sum = "h1:LNhjNn8DerC8f9DHLz6lS0YYul/b602DUxDgGkd/Aik=",
        version = "v0.3.1",
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
        sum = "h1:bBRl1b0OH9s/DuPhuXpNl+VtCaJXFZ5/uEFST95x9zc=",
        version = "v2.2.1",
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
        sum = "h1:W4sw5ZoU2Juc9gBWuLk5U6fHfNVyY1WC5g9uiXZio/c=",
        version = "v3.5.12",
    )
    go_repository(
        name = "io_etcd_go_etcd_client_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/pkg/v3",
        sum = "h1:EYDL6pWwyOsylrQyLp2w+HkQ46ATiOvoEdMarindU2A=",
        version = "v3.5.12",
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v2",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/v2",
        sum = "h1:0m4ovXYo1CHaA/Mp3X/Fak5sRNIWf01wk/X1/G3sGKI=",
        version = "v2.305.12",
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/v3",
        sum = "h1:v5lCPXn1pf1Uu3M4laUE2hp/geOTc5uPcYYsNe1lDxg=",
        version = "v3.5.12",
    )
    go_repository(
        name = "io_etcd_go_etcd_etcdutl_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/etcdutl/v3",
        sum = "h1:R3HLloeRcIOAvNtOTcMV9fshCbz9aZP2Xh4AP2+KnFU=",
        version = "v3.5.12",
    )
    go_repository(
        name = "io_etcd_go_etcd_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/pkg/v3",
        sum = "h1:OK2fZKI5hX/+BTK76gXSTyZMrbnARyX9S643GenNGb8=",
        version = "v3.5.12",
    )
    go_repository(
        name = "io_etcd_go_etcd_raft_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/raft/v3",
        sum = "h1:7r22RufdDsq2z3STjoR7Msz6fYH8tmbkdheGfwJNRmU=",
        version = "v3.5.12",
    )
    go_repository(
        name = "io_etcd_go_etcd_server_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/server/v3",
        sum = "h1:EtMjsbfyfkwZuA2JlKOiBfuGkFCekv5H178qjXypbG8=",
        version = "v3.5.12",
    )
    go_repository(
        name = "io_etcd_go_etcd_tests_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/tests/v3",
        sum = "h1:k1fG7+F87Z7zKp57EcjXu9XgOsW0sfp5USqfzmMTIwM=",
        version = "v3.5.12",
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
        sum = "h1:FNf4tywRC1HmFuKW5xopWpigGjJKiJSV0Cqo0cJWDaA=",
        version = "v1.1.0",
    )
    go_repository(
        name = "io_gorm_driver_mysql",
        build_file_proto_mode = "disable_global",
        importpath = "gorm.io/driver/mysql",
        sum = "h1:MndhOPYOfEp2rHKgkZIhJ16eVUIRf2HmzgoPmh7FCWo=",
        version = "v1.5.7",
    )
    go_repository(
        name = "io_gorm_gorm",
        build_file_proto_mode = "disable_global",
        importpath = "gorm.io/gorm",
        sum = "h1:/Wfyg1B/je1hnDx3sMkX+gAlxrlZpn6X0BXRlwXlvHg=",
        version = "v1.25.11",
    )
    go_repository(
        name = "io_k8s_api",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/api",
        sum = "h1:6FwDo33f1WX5Yu0RQTX9YAd3wth8Ik0B4SXQKsoQfbk=",
        version = "v0.29.11",
    )
    go_repository(
        name = "io_k8s_apimachinery",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/apimachinery",
        sum = "h1:55+6ue9advpA7T0sX2ZJDHCLKuiFfrAAR/39VQN9KEQ=",
        version = "v0.29.11",
    )
    go_repository(
        name = "io_k8s_client_go",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/client-go",
        sum = "h1:Gge6ziyIdafRchfoBKcpaARuz7jfrK1R1azuwORIsQI=",
        version = "v0.28.6",
    )
    go_repository(
        name = "io_k8s_klog",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog",
        sum = "h1:Pt+yjF5aB1xDSVbau4VsWe+dQNzA0qv1LlXdC2dF6Q8=",
        version = "v1.0.0",
    )
    go_repository(
        name = "io_k8s_klog_v2",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog/v2",
        sum = "h1:QXU6cPEOIslTGvZaXvFWiP9VKyeet3sawzTOvdXb4Vw=",
        version = "v2.120.1",
    )
    go_repository(
        name = "io_k8s_kube_openapi",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/kube-openapi",
        sum = "h1:aVUu9fTY98ivBPKR9Y5w/AuzbMm96cd3YHRTU83I780=",
        version = "v0.0.0-20231010175941-2dd684a91f00",
    )
    go_repository(
        name = "io_k8s_sigs_json",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/json",
        sum = "h1:EDPBXCAspyGV4jQlpZSudPeMmr1bNJefnuqLsRAsHZo=",
        version = "v0.0.0-20221116044647-bc3834ca7abd",
    )
    go_repository(
        name = "io_k8s_sigs_structured_merge_diff_v4",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/structured-merge-diff/v4",
        sum = "h1:150L+0vs/8DA78h1u02ooW1/fFq/Lwr+sGiqlzvrtq4=",
        version = "v4.4.1",
    )
    go_repository(
        name = "io_k8s_sigs_yaml",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/yaml",
        sum = "h1:Mk1wCc2gy/F0THH0TAp1QYyJNzRm2KCLy3o5ASXVI5E=",
        version = "v1.4.0",
    )
    go_repository(
        name = "io_k8s_utils",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/utils",
        sum = "h1:sgn3ZU783SCgtaSJjpcVVlRqd6GSnlTLKgpAAttJvpI=",
        version = "v0.0.0-20230726121419-3b25d923346b",
    )
    go_repository(
        name = "io_opencensus_go",
        build_file_proto_mode = "disable_global",
        importpath = "go.opencensus.io",
        sum = "h1:y73uSU6J157QMP2kn2r30vwW1A2W2WFwSCGnAVxeaD0=",
        version = "v0.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_collector_featuregate",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/collector/featuregate",
        sum = "h1:ok//hLSXttBbyu4sSV1pTx1nKdr5udSmrWy5sFMIIbM=",
        version = "v1.0.1",
    )
    go_repository(
        name = "io_opentelemetry_go_collector_pdata",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/collector/pdata",
        sum = "h1:dGX2h7maA6zHbl5D3AsMnF1c3Nn+3EUftbVCLzeyNvA=",
        version = "v1.0.1",
    )
    go_repository(
        name = "io_opentelemetry_go_collector_semconv",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/collector/semconv",
        sum = "h1:eBlMcVNTwYYsVdAsCVDs4wvVYs75K1xcIDpqj16PG4c=",
        version = "v0.93.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
        sum = "h1:4Pp6oUg3+e/6M4C0A/3kJ2VYa++dsWVTtGgLVj5xtHg=",
        version = "v0.49.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
        sum = "h1:jq9TW8u3so/bN+JPT166wjOI6/vQPF6Xe7nMNIltagk=",
        version = "v0.49.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel",
        sum = "h1:0LAOdjNmQeSTzGBzduGe/rU4tZhMwL5rWgtp9Ku5Jfo=",
        version = "v1.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace",
        sum = "h1:9M3+rhx7kZCIQQhQRYaZCdNu1V73tm4TvXs2ntl98C4=",
        version = "v1.22.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc",
        sum = "h1:H2JFgRcGiyHg7H7bwcwaQJYrNFqCqrbTQ8K4p1OvDu8=",
        version = "v1.22.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp",
        sum = "h1:FyjCyI9jVEfqhUh2MoSkmolPjfh5fp2hnV0b0irxH4Q=",
        version = "v1.22.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_metric",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/metric",
        sum = "h1:6EhoGWWK28x1fbpA4tYTOWBkPefTDQnb8WSGXlc88kI=",
        version = "v1.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/sdk",
        sum = "h1:YMPPDNymmQN3ZgczicBY3B6sf9n62Dlj9pWD3ucgoDw=",
        version = "v1.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/trace",
        sum = "h1:CsKnnL4dUAr/0llH9FKuc698G04IrpWV0MQA/Y1YELI=",
        version = "v1.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_proto_otlp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/proto/otlp",
        sum = "h1:2Di21piLrCqJ3U3eXGCTPHE9R8Nh+0uglSnOyxikMeI=",
        version = "v1.1.0",
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
        sum = "h1:xwwDQW5We85NaTk2APgoN9202w/l0DVGp+GZMfsrh7s=",
        version = "v0.0.0-20210223155950-e043a3d3c984",
    )
    go_repository(
        name = "org_gioui",
        build_file_proto_mode = "disable_global",
        importpath = "gioui.org",
        sum = "h1:K72hopUosKG3ntOPNG4OzzbuhxGuVf06fa2la1/H/Ho=",
        version = "v0.0.0-20210308172011-57750fc8a0a6",
    )
    go_repository(
        name = "org_go_simpler_musttag",
        build_file_proto_mode = "disable_global",
        importpath = "go-simpler.org/musttag",
        sum = "h1:Q/YAW0AHvaoaIbsPj3bvEI5/QFP7w696IMUpnKXQfCE=",
        version = "v0.13.0",
    )
    go_repository(
        name = "org_go_simpler_sloglint",
        build_file_proto_mode = "disable_global",
        importpath = "go-simpler.org/sloglint",
        sum = "h1:Wc9Em/Zeuu7JYpl+oKoYOsQSy2X560aVueCW/m6IijY=",
        version = "v0.7.2",
    )
    go_repository(
        name = "org_golang_google_api",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/api",
        sum = "h1:QwWPy71FgMWqJN/l6jVlFHUa29a7dcUy02I8o799nPY=",
        version = "v0.169.0",
    )
    go_repository(
        name = "org_golang_google_appengine",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/appengine",
        sum = "h1:IhEN5q69dyKagZPYMSdIjS2HqprW324FRQZJcGqPAsM=",
        version = "v1.6.8",
    )
    go_repository(
        name = "org_golang_google_genproto",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto",
        sum = "h1:F6qOa9AZTYJXOUEr4jDysRDLrm4PHePlge4v4TGAlxY=",
        version = "v0.0.0-20240227224415-6ceb2ff114de",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_api",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto/googleapis/api",
        sum = "h1:RFiFrvy37/mpSpdySBDrUdipW/dHwsRwh3J3+A9VgT4=",
        version = "v0.0.0-20240318140521-94a12d6c2237",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_bytestream",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto/googleapis/bytestream",
        sum = "h1:YqFWYZXim8bG9v68xU8WjTZmYKb5M5dMeSOWIp6jogI=",
        version = "v0.0.0-20240304161311-37d4d3c04a78",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_rpc",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto/googleapis/rpc",
        sum = "h1:AgADTJarZTBqgjiUzRgfaBchgYB3/WFTC80GPwsMcRI=",
        version = "v0.0.0-20240515191416-fc5f0ca64291",
    )
    go_repository(
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/grpc",
        sum = "h1:MUeiw1B2maTVZthpU5xvASfTh3LDbxHd6IJ6QQVU+xM=",
        version = "v1.63.2",
    )
    go_repository(
        name = "org_golang_google_grpc_examples",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/grpc/examples",
        sum = "h1:ATnmU8nL2NfIyTSiBvJVDIDIr3qBmeW+c7z7XU21eWs=",
        version = "v0.0.0-20231221225426-4f03f3ff32c9",
    )
    go_repository(
        name = "org_golang_google_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/protobuf",
        sum = "h1:yBPeRvTftaleIgM3PZ/WBIZ7XM/eEYAaEyCwvyjq/gk=",
        version = "v1.36.1",
    )
    go_repository(
        name = "org_golang_x_crypto",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/crypto",
        sum = "h1:euUpcYgM8WcP71gNpTqQCn6rC2t6ULUPiOzfWaXVVfc=",
        version = "v0.32.0",
    )
    go_repository(
        name = "org_golang_x_exp",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/exp",
        sum = "h1:e66Fs6Z+fZTbFBAxKfP3PALWBtpfqks2bwGcexMxgtk=",
        version = "v0.0.0-20240909161429-701f63a606c0",
    )
    go_repository(
        name = "org_golang_x_exp_typeparams",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/exp/typeparams",
        sum = "h1:WTyX8eCCyfdqiPYkRGm0MqElSfYFH3yR1+rl/mct9sA=",
        version = "v0.0.0-20241108190413-2d47ceb2692f",
    )
    go_repository(
        name = "org_golang_x_image",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/image",
        sum = "h1:TcHcE0vrmgzNH1v3ppjcMGbhG5+9fMuvOmUYwNEF4q4=",
        version = "v0.0.0-20220302094943-723b81ca9867",
    )
    go_repository(
        name = "org_golang_x_lint",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/lint",
        sum = "h1:2M3HP5CCK1Si9FQhwnzYhXdG6DXeebvUHFpre8QvbyI=",
        version = "v0.0.0-20201208152925-83fdc39ff7b5",
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
        sum = "h1:D4nJWe9zXqHOmWqj4VMOJhvzj7bEZg4wEYa759z1pH4=",
        version = "v0.22.0",
    )
    go_repository(
        name = "org_golang_x_net",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/net",
        sum = "h1:Mb7Mrk043xzHgnRM88suvJFwzVrRfHEHJEl5/71CKw0=",
        version = "v0.34.0",
    )
    go_repository(
        name = "org_golang_x_oauth2",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/oauth2",
        sum = "h1:KTBBxWqUa0ykRPLtV69rRto9TLXcqYkeswu48x/gvNE=",
        version = "v0.24.0",
    )
    go_repository(
        name = "org_golang_x_perf",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/perf",
        sum = "h1:ObuXPmIgI4ZMyQLIz48cJYgSyWdjUXc2SZAdyJMwEAU=",
        version = "v0.0.0-20230113213139-801c7ef9e5c5",
    )
    go_repository(
        name = "org_golang_x_sync",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sync",
        sum = "h1:3NQrjDixjgGwUOCaF8w2+VYHv0Ve/vGYSbdkTa98gmQ=",
        version = "v0.10.0",
    )
    go_repository(
        name = "org_golang_x_sys",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sys",
        sum = "h1:TPYlXGxvx1MGTn2GiZDhnjPA9wZzZeGKHHmKhHYvgaU=",
        version = "v0.29.0",
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
        sum = "h1:/Ts8HFuMR2E6IP/jlo7QVLZHggjKQbhu/7H0LJFr3Gg=",
        version = "v0.28.0",
    )
    go_repository(
        name = "org_golang_x_text",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/text",
        sum = "h1:zyQAAkrwaneQ066sspRyJaG9VNi/YJ1NfzcGB3hZ/qo=",
        version = "v0.21.0",
    )
    go_repository(
        name = "org_golang_x_time",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/time",
        sum = "h1:ntUhktv3OPE6TgYxXWv9vKvUSJyIFJlyohwbkEwPrKQ=",
        version = "v0.7.0",
    )
    go_repository(
        name = "org_golang_x_tools",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/tools",
        sum = "h1:Xx0h3TtM9rzQpQuR4dKLrdglAmCEN5Oi+P74JdhdzXE=",
        version = "v0.29.0",
    )
    go_repository(
        name = "org_golang_x_xerrors",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/xerrors",
        sum = "h1:+cNy6SZtPcJQH3LJVLOSmiC7MMxXNOb3PU/VUEz+EhU=",
        version = "v0.0.0-20231012003039-104605ab7028",
    )
    go_repository(
        name = "org_gonum_v1_gonum",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/gonum",
        sum = "h1:f1IJhK4Km5tBJmaiJXtk/PkL4cdVX6J+tGiM187uT5E=",
        version = "v0.11.0",
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
        sum = "h1:dnifSs43YJuNMDzB7v8wV64O4ABBHReuAVAoBxqBqS4=",
        version = "v0.10.1",
    )
    go_repository(
        name = "org_modernc_cc_v3",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/cc/v3",
        sum = "h1:P3g79IUS/93SYhtoeaHW+kRCIrYaxJ27MFPv+7kaTOw=",
        version = "v3.40.0",
    )
    go_repository(
        name = "org_modernc_ccgo_v3",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/ccgo/v3",
        sum = "h1:Mkgdzl46i5F/CNR/Kj80Ri59hC8TKAhZrYSaqvkwzUw=",
        version = "v3.16.13",
    )
    go_repository(
        name = "org_modernc_ccorpus",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/ccorpus",
        sum = "h1:J16RXiiqiCgua6+ZvQot4yUuUy8zxgqbqEEUuGPlISk=",
        version = "v1.11.6",
    )
    go_repository(
        name = "org_modernc_golex",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/golex",
        sum = "h1:dmSaksHMd+y6NkBsRsCShNPRaSNCNH+abrVm5/gZic8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "org_modernc_httpfs",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/httpfs",
        sum = "h1:AAgIpFZRXuYnkjftxTAZwMIiwEqAfk8aVB2/oA6nAeM=",
        version = "v1.0.6",
    )
    go_repository(
        name = "org_modernc_libc",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/libc",
        sum = "h1:4U7v51GyhlWqQmwCHj28Rdq2Yzwk55ovjFrdPjs8Hb0=",
        version = "v1.22.2",
    )
    go_repository(
        name = "org_modernc_mathutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/mathutil",
        sum = "h1:fRe9+AmYlaej+64JsEEhoWuAYBkOtQiMEU7n/XgfYi4=",
        version = "v1.6.0",
    )
    go_repository(
        name = "org_modernc_memory",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/memory",
        sum = "h1:N+/8c5rE6EqugZwHii4IFsaJ7MUhoWX07J5tC/iI5Ds=",
        version = "v1.5.0",
    )
    go_repository(
        name = "org_modernc_opt",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/opt",
        sum = "h1:3XOZf2yznlhC+ibLltsDGzABUGVx8J6pnFMS3E4dcq4=",
        version = "v0.1.3",
    )
    go_repository(
        name = "org_modernc_parser",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/parser",
        sum = "h1:XoClYpoz2xHEDIteSQ7tICOTFcNwBI7XRCeghUS6SNI=",
        version = "v1.1.0",
    )
    go_repository(
        name = "org_modernc_sortutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/sortutil",
        sum = "h1:jQiD3PfS2REGJNzNCMMaLSp/wdMNieTbKX920Cqdgqc=",
        version = "v1.2.0",
    )
    go_repository(
        name = "org_modernc_sqlite",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/sqlite",
        sum = "h1:S2uFiaNPd/vTAP/4EmyY8Qe2Quzu26A2L1e25xRNTio=",
        version = "v1.18.2",
    )
    go_repository(
        name = "org_modernc_strutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/strutil",
        sum = "h1:agBi9dp1I+eOnxXeiZawM8F4LawKv4NzGWSaLfyeNZA=",
        version = "v1.2.0",
    )
    go_repository(
        name = "org_modernc_tcl",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/tcl",
        sum = "h1:5PQgL/29XkQ9wsEmmNPjzKs+7iPCaYqUJAhzPvQbjDA=",
        version = "v1.13.2",
    )
    go_repository(
        name = "org_modernc_token",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/token",
        sum = "h1:Xl7Ap9dKaEs5kLoOQeQmPWevfnk/DM5qcLcYlA8ys6Y=",
        version = "v1.1.0",
    )
    go_repository(
        name = "org_modernc_y",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/y",
        sum = "h1:JdIvLry+rKeSsVNRCdr6YWYimwwNm0GXtzxid77VfWc=",
        version = "v1.1.0",
    )
    go_repository(
        name = "org_modernc_z",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/z",
        sum = "h1:RTNHdsrOpeoSeOF4FbzTo8gBYByaJ5xT7NgZ9ZqRiJM=",
        version = "v1.5.1",
    )
    go_repository(
        name = "org_mongodb_go_mongo_driver",
        build_file_proto_mode = "disable_global",
        importpath = "go.mongodb.org/mongo-driver",
        sum = "h1:YIc7HTYsKndGK4RFzJ3covLz1byri52x0IoMB0Pt/vk=",
        version = "v1.13.1",
    )
    go_repository(
        name = "org_uber_go_atomic",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/atomic",
        sum = "h1:ZvwS0R+56ePWxUNi+Atn9dWONBPp/AUETXlHW0DxSjE=",
        version = "v1.11.0",
    )
    go_repository(
        name = "org_uber_go_automaxprocs",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/automaxprocs",
        sum = "h1:O3y2/QNTOdbF+e/dpXNNW7Rx2hZ4sTIPyybbxyNqTUs=",
        version = "v1.6.0",
    )
    go_repository(
        name = "org_uber_go_goleak",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/goleak",
        sum = "h1:2K3zAYmnTNqV73imy9J1T3WC+gmCePx2hEGkimedGto=",
        version = "v1.3.0",
    )
    go_repository(
        name = "org_uber_go_mock",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/mock",
        sum = "h1:VcM4ZOtdbR4f6VXfiOpwpVJDL6lCReaZ6mw31wqh7KU=",
        version = "v0.4.0",
    )
    go_repository(
        name = "org_uber_go_multierr",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/multierr",
        sum = "h1:blXXJkSxSSfBVBlC76pxqeO+LN3aDfLQo+309xJstO0=",
        version = "v1.11.0",
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
        sum = "h1:aJMhYGrd5QSmlpLMr2MftRKl7t8J8PTZPA732ud/XR8=",
        version = "v1.27.0",
    )
    go_repository(
        name = "tech_einride_go_aip",
        build_file_proto_mode = "disable_global",
        importpath = "go.einride.tech/aip",
        sum = "h1:XfV+NQX6L7EOYK11yoHHFtndeaWh3KbD9/cN/6iWEt8=",
        version = "v0.66.0",
    )
    go_repository(
        name = "tools_gotest_v3",
        build_file_proto_mode = "disable_global",
        importpath = "gotest.tools/v3",
        sum = "h1:4AuOwCGf4lLR9u3YOe2awrHygurzhO/HeQ6laiA6Sx0=",
        version = "v3.0.3",
    )
