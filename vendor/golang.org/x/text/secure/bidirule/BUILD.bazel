load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["bidirule.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/golang.org/x/text/transform:go_default_library",
        "//vendor/golang.org/x/text/unicode/bidi:go_default_library",
    ],
)
