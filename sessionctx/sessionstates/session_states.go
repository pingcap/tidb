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

package sessionstates

import (
	"time"

	ptypes "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

// SessionStateType is the type of session states.
type SessionStateType int

// These enums represents the types of session state handlers.
const (
	// StatePrepareStmt represents prepared statements.
	StatePrepareStmt SessionStateType = iota
	// StateBinding represents session SQL bindings.
	StateBinding
)

// PreparedStmtInfo contains the information about prepared statements, both text and binary protocols.
type PreparedStmtInfo struct {
	Name       string `json:"name,omitempty"`
	StmtText   string `json:"text"`
	StmtDB     string `json:"db,omitempty"`
	ParamTypes []byte `json:"types,omitempty"`
}

// QueryInfo represents the information of last executed query. It's used to expose information for test purpose.
type QueryInfo struct {
	TxnScope    string `json:"txn_scope"`
	StartTS     uint64 `json:"start_ts"`
	ForUpdateTS uint64 `json:"for_update_ts"`
	ErrMsg      string `json:"error,omitempty"`
}

// LastDDLInfo represents the information of last DDL. It's used to expose information for test purpose.
type LastDDLInfo struct {
	Query  string `json:"query"`
	SeqNum uint64 `json:"seq_num"`
}

// BindRecordState represents the information of a SQL binding record.
type BindRecordState struct {
	OriginalSQL string         `json:"original_sql"`
	Db          string         `json:"db,omitempty"`
	Bindings    []BindingState `json:"bindings"`
}

// BindingState represents the information of a binding.
type BindingState struct {
	BindSQL string `json:"bind_sql"`
	// Status represents the status of the binding. It can only be one of the following values:
	// 1. deleted: BindRecord is deleted, can not be used anymore.
	// 2. enabled, using: Binding is in the normal active mode.
	Status     string     `json:"status"`
	CreateTime types.Time `json:"create_time"`
	UpdateTime types.Time `json:"update_time"`
	Source     string     `json:"source"`
	Charset    string     `json:"charset"`
	Collation  string     `json:"collation"`
}

// SessionStates contains all the states in the session that should be migrated when the session
// is migrated to another server. It is shown by `show session_states` and recovered by `set session_states`.
type SessionStates struct {
	UserVars             map[string]*types.Datum      `json:"user-var-values,omitempty"`
	UserVarTypes         map[string]*ptypes.FieldType `json:"user-var-types,omitempty"`
	SystemVars           map[string]string            `json:"sys-vars,omitempty"`
	PreparedStmts        map[uint32]*PreparedStmtInfo `json:"prepared-stmts,omitempty"`
	PreparedStmtID       uint32                       `json:"prepared-stmt-id,omitempty"`
	Status               uint16                       `json:"status,omitempty"`
	CurrentDB            string                       `json:"current-db,omitempty"`
	LastTxnInfo          string                       `json:"txn-info,omitempty"`
	LastQueryInfo        *QueryInfo                   `json:"query-info,omitempty"`
	LastDDLInfo          *LastDDLInfo                 `json:"ddl-info,omitempty"`
	LastFoundRows        uint64                       `json:"found-rows,omitempty"`
	FoundInPlanCache     bool                         `json:"in-plan-cache,omitempty"`
	FoundInBinding       bool                         `json:"in-binding,omitempty"`
	SequenceLatestValues map[int64]int64              `json:"seq-values,omitempty"`
	MPPStoreLastFailTime map[string]time.Time         `json:"store-fail-time,omitempty"`
	LastAffectedRows     int64                        `json:"affected-rows,omitempty"`
	LastInsertID         uint64                       `json:"last-insert-id,omitempty"`
	Warnings             []stmtctx.SQLWarn            `json:"warnings,omitempty"`
	Bindings             []*BindRecordState           `json:"bindings,omitempty"`
}
