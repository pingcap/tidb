load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "6dc2da7ab4cf5d7bfc7c949776b1b7c733f05e56edc4bcd9022bb249d2e2a996",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.39.1/rules_go-v0.39.1.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.39.1/rules_go-v0.39.1.zip",
        "https://ats.apps.svc/bazelbuild/rules_go/releases/download/v0.39.1/rules_go-v0.39.1.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "727f3e4edd96ea20c29e8c2ca9e8d2af724d8c7778e7923a854b2c80952bc405",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.30.0/bazel-gazelle-v0.30.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.30.0/bazel-gazelle-v0.30.0.tar.gz",
        "http://ats.apps.svc/bazelbuild/bazel-gazelle/releases/download/v0.30.0/bazel-gazelle-v0.30.0.tar.gz",
    ],
)

http_archive(
    name = "rules_cc",
    urls = [
        "https://github.com/bazelbuild/rules_cc/releases/download/0.0.6/rules_cc-0.0.6.tar.gz",
        "http://ats.apps.svc/bazelbuild/rules_cc/releases/download/0.0.6/rules_cc-0.0.6.tar.gz",
    ],
    sha256 = "3d9e271e2876ba42e114c9b9bc51454e379cbf0ec9ef9d40e2ae4cec61a31b40",
    strip_prefix = "rules_cc-0.0.6",
)

load("@io_bazel_rules_go//go:deps.bzl", "go_download_sdk", "go_register_toolchains", "go_rules_dependencies")
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("//:DEPS.bzl", "go_deps")

# gazelle:repository_macro DEPS.bzl%go_deps
go_deps()

go_rules_dependencies()

go_download_sdk(
    name = "go_sdk",
    urls = [
        "http://ats.apps.svc/golang/{}",
        "http://bazel-cache.pingcap.net:8080/golang/{}",
        "https://mirrors.aliyun.com/golang/{}",
        "https://dl.google.com/go/{}",
    ],
    version = "1.20.4",
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
