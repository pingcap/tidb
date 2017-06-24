load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "all.go",
        "gbk.go",
        "hzgb2312.go",
        "tables.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//vendor/golang.org/x/text/encoding:go_default_library",
        "//vendor/golang.org/x/text/encoding/internal:go_default_library",
        "//vendor/golang.org/x/text/encoding/internal/identifier:go_default_library",
        "//vendor/golang.org/x/text/transform:go_default_library",
    ],
)
