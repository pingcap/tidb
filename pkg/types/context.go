// Copyright 2023 PingCAP, Inc.
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

package types

import (
	"github.com/pingcap/tidb/pkg/types/context"
)

// TODO: move a contents in `types/context/context.go` to this file after refactor finished.
// Because package `types` has a dependency on `sessionctx/stmtctx`, we need a separate package `type/context` to define
// context objects during refactor works.

// Context is an alias of `context.Context`
type Context = context.Context

// Flags is an alias of `Flags`
type Flags = context.Flags

// StrictFlags is a flags with a fields unset and has the most strict behavior.
const StrictFlags = context.StrictFlags

// NewContext creates a new `Context`
var NewContext = context.NewContext

// DefaultStmtFlags is the default flags for statement context with the flag `FlagAllowNegativeToUnsigned` set.
// TODO: make DefaultStmtFlags to be equal with StrictFlags, and setting flag `FlagAllowNegativeToUnsigned`
// is only for make the code to be equivalent with the old implement during refactoring.
const DefaultStmtFlags = context.DefaultStmtFlags

// DefaultStmtNoWarningContext is an alias of `DefaultStmtNoWarningContext`
var DefaultStmtNoWarningContext = context.DefaultStmtNoWarningContext
