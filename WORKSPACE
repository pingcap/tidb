load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "bazel_skylib",
    sha256 = "66ffd9315665bfaafc96b52278f57c7e2dd09f5ede279ea6d39b2be471e7e3aa",
    urls = [
        "http://bazel-cache.pingcap.net:8080/bazelbuild/bazel-skylib/releases/download/1.4.2/bazel-skylib-1.4.2.tar.gz",
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.4.2/bazel-skylib-1.4.2.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.4.2/bazel-skylib-1.4.2.tar.gz",
        "http://ats.apps.svc/bazelbuild/bazel-skylib/releases/download/1.4.2/bazel-skylib-1.4.2.tar.gz",
    ],
)

load("@bazel_skylib//lib:versions.bzl", "versions")
versions.check(minimum_bazel_version = "6.0.0")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "278b7ff5a826f3dc10f04feaf0b70d48b68748ccd512d7f98bf442077f043fe3",
    urls = [
        "http://bazel-cache.pingcap.net:8080/bazelbuild/rules_go/releases/download/v0.41.0/rules_go-v0.41.0.zip",
        "http://ats.apps.svc/bazelbuild/rules_go/releases/download/v0.41.0/rules_go-v0.41.0.zip",
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.41.0/rules_go-v0.41.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.41.0/rules_go-v0.41.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "b8b6d75de6e4bf7c41b7737b183523085f56283f6db929b86c5e7e1f09cf59c9",
    urls = [
        "http://bazel-cache.pingcap.net:8080/bazelbuild/bazel-gazelle/releases/download/v0.31.1/bazel-gazelle-v0.31.1.tar.gz",
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.31.1/bazel-gazelle-v0.31.1.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.31.1/bazel-gazelle-v0.31.1.tar.gz",
        "http://ats.apps.svc/bazelbuild/bazel-gazelle/releases/download/v0.31.1/bazel-gazelle-v0.31.1.tar.gz",
    ],
)

http_archive(
    name = "rules_cc",
    urls = [
        "http://bazel-cache.pingcap.net:8080/bazelbuild/rules_cc/releases/download/0.0.6/rules_cc-0.0.6.tar.gz",
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
    version = "1.21.0",
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
        "http://bazel-cache.pingcap.net:8080/protocolbuffers/protobuf/archive/v3.15.5.tar.gz",
        "https://github.com/protocolbuffers/protobuf/archive/v3.15.5.tar.gz",
        "https://mirror.bazel.build/github.com/protocolbuffers/protobuf/archive/v3.15.5.tar.gz",
    ],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "remote_java_tools",
    sha256 = "f58a358ca694a41416a9b6a92b852935ad301d8882e5d22f4f11134f035317d5",
    urls = [
            "http://bazel-cache.pingcap.net:8080/bazelbuild/java_tools/releases/download/java_v12.6/java_tools-v12.6.zip",
            "http://ats.apps.svc/bazel_java_tools/releases/java/v12.6/java_tools-v12.6.zip",
            "https://mirror.bazel.build/bazel_java_tools/releases/java/v12.6/java_tools-v12.6.zip",
            "https://github.com/bazelbuild/java_tools/releases/download/java_v12.6/java_tools-v12.6.zip",
    ],
)

http_archive(
    name = "remote_java_tools_linux",
    sha256 = "64294e91fe940c77e6d35818b4c3a1f07d78e33add01e330188d907032687066",
    urls = [
            "http://bazel-cache.pingcap.net:8080/bazelbuild/java_tools/releases/download/java_v12.6/java_tools_linux-v12.6.zip",
            "http://ats.apps.svc/bazel_java_tools/releases/java/v12.6/java_tools_linux-v12.6.zip",
            "https://mirror.bazel.build/bazel_java_tools/releases/java/v12.6/java_tools_linux-v12.6.zip",
            "https://github.com/bazelbuild/java_tools/releases/download/java_v12.6/java_tools_linux-v12.6.zip",
    ],
)
