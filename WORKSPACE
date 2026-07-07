load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "platforms",
    sha256 = "dbad4a23abcca6171e47b79edc53bd6a41067a3b75f9e8b104656b459ff25046",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/1.1.0/platforms-1.1.0.tar.gz",
        "https://github.com/bazelbuild/platforms/releases/download/1.1.0/platforms-1.1.0.tar.gz",
    ],
)

# To use the new Starlark host platform in @platforms, also include the following snippet:
load("@platforms//host:extension.bzl", "host_platform_repo")

host_platform_repo(name = "host_platform")

http_archive(
    name = "bazel_features",
    sha256 = "9390b391a68d3b24aef7966bce8556d28003fe3f022a5008efc7807e8acaaf1a",
    strip_prefix = "bazel_features-1.36.0",
    url = "https://github.com/bazel-contrib/bazel_features/releases/download/v1.36.0/bazel_features-v1.36.0.tar.gz",
)

load("@bazel_features//:deps.bzl", "bazel_features_deps")

bazel_features_deps()

http_archive(
    name = "bazel_skylib",
    sha256 = "51b5105a760b353773f904d2bbc5e664d0987fbaf22265164de65d43e910d8ac",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.8.1/bazel-skylib-1.8.1.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.8.1/bazel-skylib-1.8.1.tar.gz",
    ],
)

load("@bazel_skylib//lib:versions.bzl", "versions")

versions.check(minimum_bazel_version = "6.0.0")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "763f4a3f6b03469fdb00a77a333dd0b5546d3ee1fa29db373128c08fee73e0e8",
    urls = [
        "https://cache.hawkingrei.com/bazel-contrib/rules_go/releases/download/v0.61.1/rules_go-v0.61.1.zip",
        "https://mirror.bazel.build/github.com/bazel-contrib/rules_go/releases/download/v0.61.1/rules_go-v0.61.1.zip",
        "https://github.com/bazel-contrib/rules_go/releases/download/v0.61.1/rules_go-v0.61.1.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "49d9eba309b0b695824ff417d734242824ad9ab5edb56063b9d3400df1a61a56",
    urls = [
        "https://github.com/bazel-contrib/bazel-gazelle/releases/download/v0.51.3/bazel-gazelle-v0.51.3.tar.gz",
        "https://cache.hawkingrei.com/bazel-contrib/bazel-gazelle/releases/download/v0.51.3/bazel-gazelle-v0.51.3.tar.gz",
    ],
)

http_archive(
    name = "rules_cc",
    patch_cmds = [
        "printf '\\ncc_toolchain_alias(name = \"optional_current_cc_toolchain\")\\n' >> cc/BUILD",
    ],
    sha256 = "65b67b81c6da378f136cc7e7e14ee08d5b9375973427eceb8c773a4f69fa7e49",
    strip_prefix = "rules_cc-0.0.10",
    url = "https://github.com/bazelbuild/rules_cc/releases/download/0.0.10/rules_cc-0.0.10.tar.gz",
)

http_archive(
    name = "rules_python",
    sha256 = "9f9f3b300a9264e4c77999312ce663be5dee9a56e361a1f6fe7ec60e1beef9a3",
    strip_prefix = "rules_python-1.4.1",
    urls = [
        "https://github.com/bazel-contrib/rules_python/releases/download/1.4.1/rules_python-1.4.1.tar.gz",
        "https://cache.hawkingrei.com/bazel-contrib/rules_python/releases/download/1.4.1/rules_python-1.4.1.tar.gz",
    ],
)

load("@rules_python//python:repositories.bzl", "py_repositories")

py_repositories()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("@io_bazel_rules_go//go:deps.bzl", "go_download_sdk", "go_register_toolchains", "go_rules_dependencies")
load("//:DEPS.bzl", "go_deps")

# gazelle:repository_macro DEPS.bzl%go_deps
go_deps()

go_rules_dependencies()

go_download_sdk(
    name = "go_sdk",
    urls = [
        "https://cache.hawkingrei.com/golang/{}",
        "https://mirrors.aliyun.com/golang/{}",
        "https://dl.google.com/go/{}",
    ],
    version = "1.25.10",
)

gazelle_dependencies(go_sdk = "go_sdk")

go_register_toolchains(
    nogo = "@//build:tidb_nogo",
)

http_archive(
    name = "com_google_protobuf",
    integrity = "sha256-zl0At4RQoMpAC/NgrADA1ZnMIl8EnZhqJ+mk45bFqEo=",
    strip_prefix = "protobuf-29.0-rc2",
    # latest, as of 2021-03-08
    urls = [
        "https://github.com/protocolbuffers/protobuf/archive/v29.0-rc2.tar.gz",
        "https://mirror.bazel.build/github.com/protocolbuffers/protobuf/archive/v29.0-rc2.tar.gz",
    ],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "remote_java_tools",
    sha256 = "f58a358ca694a41416a9b6a92b852935ad301d8882e5d22f4f11134f035317d5",
    urls = [
        "https://mirror.bazel.build/bazel_java_tools/releases/java/v12.6/java_tools-v12.6.zip",
        "https://github.com/bazelbuild/java_tools/releases/download/java_v12.6/java_tools-v12.6.zip",
    ],
)

http_archive(
    name = "remote_java_tools_linux",
    sha256 = "64294e91fe940c77e6d35818b4c3a1f07d78e33add01e330188d907032687066",
    urls = [
        "https://mirror.bazel.build/bazel_java_tools/releases/java/v12.6/java_tools_linux-v12.6.zip",
        "https://github.com/bazelbuild/java_tools/releases/download/java_v12.6/java_tools_linux-v12.6.zip",
    ],
)

http_archive(
    name = "rules_proto",
    sha256 = "0e5c64a2599a6e26c6a03d6162242d231ecc0de219534c38cb4402171def21e8",
    strip_prefix = "rules_proto-7.0.2",
    urls = [
        "https://github.com/bazelbuild/rules_proto/releases/download/7.0.2/rules_proto-7.0.2.tar.gz",
        "https://cache.hawkingrei.com/bazelbuild/rules_proto/releases/download/7.0.2/rules_proto-7.0.2.tar.gz",
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
