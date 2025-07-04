#! /bin/bash
GOBIN=/go/bin go install github.com/hawkingrei/bazel_collect@latest 
GOBIN=/go/bin go install github.com/bazelbuild/bazel-gazelle/cmd/gazelle@latest
