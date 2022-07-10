// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !bazel
// +build !bazel

package util

// This file contains stub implementations for non-bazel builds.
// See bazel.go for full documentation on the contracts of these functions.

// BuiltWithBazel returns true iff this library was built with Bazel.
func BuiltWithBazel() bool {
	return false
}

// SetGoEnv is get go env from bazel
func SetGoEnv() {
	panic("not built with Bazel")
}
