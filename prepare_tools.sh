#! /bin/bash
set -euo pipefail

# Keep Gazelle aligned with the version in WORKSPACE.
GOBIN=/go/bin go install github.com/hawkingrei/bazel_collect@v0.0.0-20251226032103-89393073f416
GOBIN=/go/bin go install github.com/bazelbuild/bazel-gazelle/cmd/gazelle@v0.47.0
