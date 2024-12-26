// Copyright 2019 PingCAP, Inc.
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

package bindinfo

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pkg/errors"
)

const (
	// Enabled is the bind info's in enabled status.
	// It is the same as the previous 'Using' status.
	// Only use 'Enabled' status in the future, not the 'Using' status.
	// The 'Using' status is preserved for compatibility.
	Enabled = "enabled"
	// Disabled is the bind info's in disabled status.
	Disabled = "disabled"
	// Using is the bind info's in use status.
	// The 'Using' status is preserved for compatibility.
	Using = "using"
	// deleted is the bind info's deleted status.
	deleted = "deleted"
	// Manual indicates the binding is created by SQL like "create binding for ...".
	Manual = "manual"
	// Builtin indicates the binding is a builtin record for internal locking purpose. It is also the status for the builtin binding.
	Builtin = "builtin"
	// History indicate the binding is created from statement summary by plan digest
	History = "history"
)

// Binding stores the basic bind hint info.
type Binding struct {
	OriginalSQL string
	Db          string
	BindSQL     string
	// Status represents the status of the binding. It can only be one of the following values:
	// 1. deleted: Bindings is deleted, can not be used anymore.
	// 2. enabled, using: Binding is in the normal active mode.
	Status     string
	CreateTime types.Time
	UpdateTime types.Time
	Source     string
	Charset    string
	Collation  string
	// Hint is the parsed hints, it is used to bind hints to stmt node.
	Hint *hint.HintsSet `json:"-"`
	// ID is the string form of Hint. It would be non-empty only when the status is `Using` or `PendingVerify`.
	ID         string `json:"-"`
	SQLDigest  string
	PlanDigest string

	// TableNames records all schema and table names in this binding statement, which are used for cross-db matching.
	TableNames []*ast.TableName `json:"-"`
}

// IsBindingEnabled returns whether the binding is enabled.
func (b *Binding) IsBindingEnabled() bool {
	return b.Status == Enabled || b.Status == Using
}

// size calculates the memory size of a bind info.
func (b *Binding) size() float64 {
	res := len(b.OriginalSQL) + len(b.Db) + len(b.BindSQL) + len(b.Status) + 2*int(unsafe.Sizeof(b.CreateTime)) + len(b.Charset) + len(b.Collation) + len(b.ID)
	return float64(res)
}

// prepareHints builds ID and Hint for Bindings. If sctx is not nil, we check if
// the BindSQL is still valid.
func prepareHints(sctx sessionctx.Context, binding *Binding) (rerr error) {
	defer func() {
		if r := recover(); r != nil {
			rerr = errors.Errorf("panic when preparing hints for binding %v, panic: %v", binding.BindSQL, r)
		}
	}()

	p := parser.New()
	if (binding.Hint != nil && binding.ID != "") || binding.Status == deleted {
		return nil
	}
	dbName := binding.Db
	bindingStmt, err := p.ParseOneStmt(binding.BindSQL, binding.Charset, binding.Collation)
	if err != nil {
		return err
	}
	tableNames := CollectTableNames(bindingStmt)
	isFuzzy := isCrossDBBinding(bindingStmt)
	if isFuzzy {
		dbName = "*" // ues '*' for universal bindings
	}

	hintsSet, stmt, warns, err := hint.ParseHintsSet(p, binding.BindSQL, binding.Charset, binding.Collation, dbName)
	if err != nil {
		return err
	}
	if sctx != nil && !isFuzzy {
		paramChecker := &paramMarkerChecker{}
		stmt.Accept(paramChecker)
		if !paramChecker.hasParamMarker {
			_, err = getHintsForSQL(sctx, binding.BindSQL)
			if err != nil {
				return err
			}
		}
	}
	hintsStr, err := hintsSet.Restore()
	if err != nil {
		return err
	}
	// For `create global binding for select * from t using select * from t`, we allow it though hintsStr is empty.
	// For `create global binding for select * from t using select /*+ non_exist_hint() */ * from t`,
	// the hint is totally invalid, we escalate warning to error.
	if hintsStr == "" && len(warns) > 0 {
		return warns[0]
	}
	binding.Hint = hintsSet
	binding.ID = hintsStr
	binding.TableNames = tableNames
	return nil
}

// pickCachedBinding picks the best binding to cache.
func pickCachedBinding(cachedBinding *Binding, bindingsFromStorage ...*Binding) *Binding {
	bindings := make([]*Binding, 0, len(bindingsFromStorage)+1)
	bindings = append(bindings, cachedBinding)
	bindings = append(bindings, bindingsFromStorage...)

	// filter nil
	n := 0
	for _, binding := range bindings {
		if binding != nil {
			bindings[n] = binding
			n++
		}
	}
	if len(bindings) == 0 {
		return nil
	}

	// filter bindings whose update time is not equal to maxUpdateTime
	maxUpdateTime := bindings[0].UpdateTime
	for _, binding := range bindings {
		if binding.UpdateTime.Compare(maxUpdateTime) > 0 {
			maxUpdateTime = binding.UpdateTime
		}
	}
	n = 0
	for _, binding := range bindings {
		if binding.UpdateTime.Compare(maxUpdateTime) == 0 {
			bindings[n] = binding
			n++
		}
	}
	bindings = bindings[:n]

	// filter deleted bindings
	n = 0
	for _, binding := range bindings {
		if binding.Status != deleted {
			bindings[n] = binding
			n++
		}
	}
	bindings = bindings[:n]

	if len(bindings) == 0 {
		return nil
	}
	// should only have one binding.
	return bindings[0]
}
