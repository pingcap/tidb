// Copyright 2024 PingCAP, Inc.
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

package rule

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/util"
)

// ExportedNewBinder is an exported version of NewBinder for testing purposes.
func ExportedNewBinder(p *pattern.Pattern, gE *memo.GroupExpression) *Binder {
	return NewBinder(p, gE)
}

// ExportedSetBinderBSW sets the bsw field for testing purposes.
func ExportedSetBinderBSW(b *Binder, bsw util.StrBufferWriter) {
	b.bsw = bsw
}

// ExportedGetBinderHolder returns the holder field for testing purposes.
func ExportedGetBinderHolder(b *Binder) any {
	return b.holder
}
