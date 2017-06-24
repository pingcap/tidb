load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "composition.go",
        "forminfo.go",
        "input.go",
        "iter.go",
        "normalize.go",
        "readwriter.go",
        "tables.go",
        "transform.go",
        "trie.go",
    ],
    visibility = ["//visibility:public"],
    deps = ["//vendor/golang.org/x/text/transform:go_default_library"],
)
