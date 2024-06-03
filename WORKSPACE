load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Required by toolchains_protoc.
http_archive(
    name = "platforms",
    sha256 = "218efe8ee736d26a3572663b374a253c012b716d8af0c07e842e82f238a0a7ee",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.10/platforms-0.0.10.tar.gz",
        "https://github.com/bazelbuild/platforms/releases/download/0.0.10/platforms-0.0.10.tar.gz",
    ],
)

http_archive(
    name = "bazel_features",
    sha256 = "d7787da289a7fb497352211ad200ec9f698822a9e0757a4976fd9f713ff372b3",
    strip_prefix = "bazel_features-1.9.1",
    url = "https://github.com/bazel-contrib/bazel_features/releases/download/v1.9.1/bazel_features-v1.9.1.tar.gz",
)

load("@bazel_features//:deps.bzl", "bazel_features_deps")

bazel_features_deps()

http_archive(
    name = "bazel_skylib",
    sha256 = "66ffd9315665bfaafc96b52278f57c7e2dd09f5ede279ea6d39b2be471e7e3aa",
    urls = [
        "http://bazel-cache.pingcap.net:8080/gomod/rules/bazel-skylib-1.4.2.tar.gz",
        "http://ats.apps.svc/gomod/rules/bazel-skylib-1.4.2.tar.gz",
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.4.2/bazel-skylib-1.4.2.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.4.2/bazel-skylib-1.4.2.tar.gz",
    ],
)

load("@bazel_skylib//lib:versions.bzl", "versions")
versions.check(minimum_bazel_version = "6.0.0")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "33acc4ae0f70502db4b893c9fc1dd7a9bf998c23e7ff2c4517741d4049a976f8",
    urls = [
        "http://bazel-cache.pingcap.net:8080/bazelbuild/rules_go/releases/download/v0.48.0/rules_go-v0.48.0.zip",
        "http://ats.apps.svc/bazelbuild/rules_go/releases/download/v0.48.0/rules_go-v0.48.0.zip",
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.48.0/rules_go-v0.48.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.48.0/rules_go-v0.48.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "d76bf7a60fd8b050444090dfa2837a4eaf9829e1165618ee35dceca5cbdf58d5",
    urls = [
        "http://bazel-cache.pingcap.net:8080/bazelbuild/bazel-gazelle/releases/download/v0.37.0/bazel-gazelle-v0.37.0.tar.gz",
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.37.0/bazel-gazelle-v0.37.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.37.0/bazel-gazelle-v0.37.0.tar.gz",
        "http://ats.apps.svc/bazelbuild/bazel-gazelle/releases/download/v0.37.0/bazel-gazelle-v0.37.0.tar.gz",
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
    version = "1.21.10",
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
        "http://bazel-cache.pingcap.net:8080/gomod/rules/protobuf-3.15.5.tar.gz ",
        "https://mirror.bazel.build/github.com/protocolbuffers/protobuf/archive/v3.15.5.tar.gz",
        "https://github.com/protocolbuffers/protobuf/archive/v3.15.5.tar.gz",
    ],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "remote_java_tools",
    sha256 = "f58a358ca694a41416a9b6a92b852935ad301d8882e5d22f4f11134f035317d5",
    urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/rules/java_tools-v12.6.zip",
            "http://ats.apps.svc/gomod/rules/java_tools-v12.6.zip",
            "https://mirror.bazel.build/bazel_java_tools/releases/java/v12.6/java_tools-v12.6.zip",
            "https://github.com/bazelbuild/java_tools/releases/download/java_v12.6/java_tools-v12.6.zip",
    ],
)

http_archive(
    name = "remote_java_tools_linux",
    sha256 = "64294e91fe940c77e6d35818b4c3a1f07d78e33add01e330188d907032687066",
    urls = [
            "http://bazel-cache.pingcap.net:8080/gomod/rules/java_tools_linux-v12.6.zip",
            "http://ats.apps.svc/gomod/rules/java_tools_linux-v12.6.zip",
            "https://mirror.bazel.build/bazel_java_tools/releases/java/v12.6/java_tools_linux-v12.6.zip",
            "https://github.com/bazelbuild/java_tools/releases/download/java_v12.6/java_tools_linux-v12.6.zip",
    ],
)

http_archive(
    name = "rules_proto",
    sha256 = "303e86e722a520f6f326a50b41cfc16b98fe6d1955ce46642a5b7a67c11c0f5d",
    strip_prefix = "rules_proto-6.0.0",
    urls = [
        "https://github.com/bazelbuild/rules_proto/releases/download/6.0.0/rules_proto-6.0.0.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies")

rules_proto_dependencies()

load("@rules_proto//proto:toolchains.bzl", "rules_proto_toolchains")

rules_proto_toolchains()

http_archive(
    name = "rules_java",
    sha256 = "f5a3e477e579231fca27bf202bb0e8fbe4fc6339d63b38ccb87c2760b533d1c3",
    strip_prefix = "rules_java-981f06c3d2bd10225e85209904090eb7b5fb26bd",
    urls = [
        "http://bazel-cache.pingcap.net:8080/gomod/rules/rules_java/rules_java-981f06c3d2bd10225e85209904090eb7b5fb26bd.tar.gz",
        "http://ats.apps.svc/bazelbuild/gomod/rules/rules_java/rules_java-981f06c3d2bd10225e85209904090eb7b5fb26bd.tar.gz",
        "https://mirror.bazel.build/github.com/bazelbuild/rules_java/archive/981f06c3d2bd10225e85209904090eb7b5fb26bd.tar.gz",
        "https://github.com/bazelbuild/rules_java/archive/981f06c3d2bd10225e85209904090eb7b5fb26bd.tar.gz",
    ],
)

http_archive(
    name = "toolchains_protoc",
    sha256 = "117af61ee2f1b9b014dcac7c9146f374875551abb8a30e51d1b3c5946d25b142",
    strip_prefix = "toolchains_protoc-0.3.0",
    url = "https://github.com/aspect-build/toolchains_protoc/releases/download/v0.3.0/toolchains_protoc-v0.3.0.tar.gz",
)
