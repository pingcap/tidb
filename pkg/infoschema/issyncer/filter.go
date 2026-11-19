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

package issyncer

import (
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// Filter allows caller to customize which schema objects should be loaded
// when syncing the information schema.
//
// The semantics of the return values are:
//   - true:  skip the corresponding operation
//   - false: continue the default loading logic
type Filter interface {
	// SkipLoadDiff returns true when the given schema diff should be ignored.
	// latestIS is the newest InfoSchema cached by the loader. It can be nil when
	// the loader hasn't loaded any schema yet.
	SkipLoadDiff(diff *model.SchemaDiff, latestIS infoschema.InfoSchema) bool

	// SkipLoadSchema returns true when the given DB should not be loaded during
	// a full schema load.
	SkipLoadSchema(dbInfo *model.DBInfo) bool
}
