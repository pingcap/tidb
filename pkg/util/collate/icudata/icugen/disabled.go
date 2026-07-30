// Copyright 2026 PingCAP, Inc.
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

//go:build !icugen

// This stub keeps the icugen package buildable in normal builds (where the real, cgo + ICU
// extractor in main.go is excluded by the `icugen` build tag). Run the real tool with:
//
//	go run -tags icugen ./pkg/util/collate/icudata/icugen
package main

func main() {}
