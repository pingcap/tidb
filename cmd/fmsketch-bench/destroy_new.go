// Copyright 2025 PingCAP, Inc.
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

//go:build !fmsketch_pool

// destroy_new.go implements destroySketch for the PR branch where FMSketch
// cleanup is just nil assignment (no pool, no Clear). This is the default
// build (no special tags needed).
package main

import "github.com/pingcap/tidb/pkg/statistics"

func destroySketch(s **statistics.FMSketch) {
	*s = nil
}
