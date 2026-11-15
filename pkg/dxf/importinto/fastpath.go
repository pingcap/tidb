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

package importinto

import (
	"github.com/pingcap/tidb/pkg/executor/importer"
)

// useMemoryFastPath reports whether a task is small enough to encode and ingest
// in one step without a round trip through the object store.
//
// The size question was already answered before the task was submitted, by the
// bounded probe in importer.ProbeSmallSource. TotalFileSize is checked as well,
// since it is exact once prepare has run.
//
// Conflicts are the reason for the OnDupKey restriction: the fast path skips the
// two conflict steps, so it can only be used in the mode that fails on the first
// conflict, which both the KV writer and the ingest path already detect.
func useMemoryFastPath(plan *importer.Plan) bool {
	return plan != nil && plan.QuerySource == nil && !plan.ForceMergeStep &&
		plan.IsGlobalSort() &&
		plan.SmallSource &&
		plan.TotalFileSize <= importer.SmallImportMaxSize &&
		plan.GetOnDupKeyMode() == importer.OnDupKeyModeError
}
