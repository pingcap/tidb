load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "56d8c5a5c91e1af73eca71a6fab2ced959b67c86d12ba37feedb0a2dfea441a6",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.37.0/rules_go-v0.37.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.37.0/rules_go-v0.37.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "501deb3d5695ab658e82f6f6f549ba681ea3ca2a5fb7911154b5aa45596183fa",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.26.0/bazel-gazelle-v0.26.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.26.0/bazel-gazelle-v0.26.0.tar.gz",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_download_sdk", "go_register_toolchains", "go_rules_dependencies")
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("//:DEPS.bzl", "go_deps")

# gazelle:repository_macro DEPS.bzl%go_deps
go_deps()

go_rules_dependencies()

go_download_sdk(
    name = "go_sdk",
    sdks = {
        "darwin_amd64": ("go1.19.4.darwin-amd64.tar.gz", "6fa1e9087b36fba65625869c885ca9c6f1db734306d8e74836b212248c20d55d"),
        "darwin_arm64": ("go1.19.4.darwin-arm64.tar.gz", "bb3bc5d7655b9637cfe2b5e90055dee93b0ead50e2ffd091df320d1af1ca853f"),
        "freebsd_amd64": ("go1.19.4.freebsd-amd64.tar.gz", "84489ebb63f1757b79574d7345c647bd40bc6414cecb868c93e24476c2d2b9b6"),
        "linux_amd64": ("go1.19.4.linux-amd64.tar.gz", "e52774e4d6a0bb5bcc5a0f1d11e337929de826b40c99c408283b8854336d9dc4"),
        "linux_arm64": ("go1.19.4.linux-arm64.tar.gz", "8bb193126fea46dca70658b7916b458a22fddb8e37d6deb463f14e10d6f06552"),
        "windows_amd64": ("go1.19.4.windows-amd64.tar.gz", "ced538537d190c03e6e4bffb3b60049794d70f09af7900bd8419b44245b2b5dc"),
    },
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/go/20221219-000617/{}"],
    version = "1.19.4",
)

go_register_toolchains(
    nogo = "@//build:tidb_nogo",
)

gazelle_dependencies()

http_archive(
    name = "com_google_protobuf",
    sha256 = "bc3dbf1f09dba1b2eb3f2f70352ee97b9049066c9040ce0c9b67fb3294e91e4b",
    strip_prefix = "protobuf-3.15.5",
    # latest, as of 2021-03-08
    urls = [
        "https://github.com/protocolbuffers/protobuf/archive/v3.15.5.tar.gz",
        "https://mirror.bazel.build/github.com/protocolbuffers/protobuf/archive/v3.15.5.tar.gz",
    ],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "remote_java_tools",
    sha256 = "5cd59ea6bf938a1efc1e11ea562d37b39c82f76781211b7cd941a2346ea8484d",
    urls = [
            "http://ats.apps.svc/bazel_java_tools/releases/java/v11.9/java_tools-v11.9.zip",
            "https://mirror.bazel.build/bazel_java_tools/releases/java/v11.9/java_tools-v11.9.zip",
            "https://github.com/bazelbuild/java_tools/releases/download/java_v11.9/java_tools-v11.9.zip",
    ],
)

http_archive(
    name = "remote_java_tools_linux",
    sha256 = "512582cac5b7ea7974a77b0da4581b21f546c9478f206eedf54687eeac035989",
    urls = [
            "http://ats.apps.svc/bazel_java_tools/releases/java/v11.9/java_tools_linux-v11.9.zip",
            "https://mirror.bazel.build/bazel_java_tools/releases/java/v11.9/java_tools_linux-v11.9.zip",
            "https://github.com/bazelbuild/java_tools/releases/download/java_v11.9/java_tools_linux-v11.9.zip",
    ],
)
