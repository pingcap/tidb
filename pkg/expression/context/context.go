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

package context

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
)

// EvalContext is used to evaluate an expression
type EvalContext interface {
	// CtxID indicates the id of the context.
	CtxID() uint64
	// SQLMode returns the sql mode
	SQLMode() mysql.SQLMode
	// TypeCtx returns the types.Context
	TypeCtx() types.Context
	// ErrCtx returns the errctx.Context
	ErrCtx() errctx.Context
	// Location returns the timezone info
	Location() *time.Location
	// AppendWarning append warnings to the context.
	AppendWarning(err error)
	// WarningCount gets warning count.
	WarningCount() int
	// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
	TruncateWarnings(start int) []stmtctx.SQLWarn
	// CurrentDB return the current database name
	CurrentDB() string
	// GetMaxAllowedPacket returns the value of the 'max_allowed_packet' system variable.
	GetMaxAllowedPacket() uint64
	// GetDefaultWeekFormatMode returns the value of the 'default_week_format' system variable.
	GetDefaultWeekFormatMode() string
	// GetDivPrecisionIncrement returns the specified value of DivPrecisionIncrement.
	GetDivPrecisionIncrement() int
	// GetSessionVars gets the session variables.
	GetSessionVars() *variable.SessionVars
	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) any
	// GetStore returns the store of session.
	GetStore() kv.Storage
	// GetInfoSchema returns the current infoschema
	GetInfoSchema() infoschema.InfoSchemaMetaVersion
	// GetDomainInfoSchema returns the latest information schema in domain
	GetDomainInfoSchema() infoschema.InfoSchemaMetaVersion
	// GetOptionalPropProvider gets the optional property provider by key
	GetOptionalPropProvider(OptionalEvalPropKey) (OptionalEvalPropProvider, bool)
}

// BuildContext is used to build an expression
type BuildContext interface {
	EvalContext
	// GetSessionVars gets the session variables.
	GetSessionVars() *variable.SessionVars
	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) any
	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value any)
}
