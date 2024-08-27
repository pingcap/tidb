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
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/model"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// SessionStateType is the type of session states.
type SessionStateType int

var (
	// ErrCannotMigrateSession indicates the session cannot be migrated.
	ErrCannotMigrateSession = dbterror.ClassSession.NewStd(errno.ErrCannotMigrateSession)
)

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
	TxnScope      string  `json:"txn_scope"`
	StartTS       uint64  `json:"start_ts"`
	ForUpdateTS   uint64  `json:"for_update_ts"`
	RUConsumption float64 `json:"ru_consumption"`
	ErrMsg        string  `json:"error,omitempty"`
}

// LastDDLInfo represents the information of last DDL. It's used to expose information for test purpose.
type LastDDLInfo struct {
	Query  string `json:"query"`
	SeqNum uint64 `json:"seq_num"`
}

// SessionStates contains all the states in the session that should be migrated when the session
// is migrated to another server. It is shown by `show session_states` and recovered by `set session_states`.
type SessionStates struct {
	UserVars             map[string]*types.Datum      `json:"user-var-values,omitempty"`
	UserVarTypes         map[string]*ptypes.FieldType `json:"user-var-types,omitempty"`
	SystemVars           map[string]string            `json:"sys-vars,omitempty"`
	PreparedStmts        map[uint32]*PreparedStmtInfo `json:"prepared-stmts,omitempty"`
	PreparedStmtID       uint32                       `json:"prepared-stmt-id,omitempty"`
	Status               uint32                       `json:"status,omitempty"`
	CurrentDB            string                       `json:"current-db,omitempty"`
	LastTxnInfo          string                       `json:"txn-info,omitempty"`
	LastQueryInfo        *QueryInfo                   `json:"query-info,omitempty"`
	LastDDLInfo          *LastDDLInfo                 `json:"ddl-info,omitempty"`
	LastFoundRows        uint64                       `json:"found-rows,omitempty"`
	FoundInPlanCache     bool                         `json:"in-plan-cache,omitempty"`
	FoundInBinding       bool                         `json:"in-binding,omitempty"`
	SequenceLatestValues map[int64]int64              `json:"seq-values,omitempty"`
	LastAffectedRows     int64                        `json:"affected-rows,omitempty"`
	LastInsertID         uint64                       `json:"last-insert-id,omitempty"`
	Warnings             []contextutil.SQLWarn        `json:"warnings,omitempty"`
	// Define it as string to avoid cycle import.
	Bindings            string                                            `json:"bindings,omitempty"`
	ResourceGroupName   string                                            `json:"rs-group,omitempty"`
	HypoIndexes         map[string]map[string]map[string]*model.IndexInfo `json:"hypo-indexes,omitempty"`
	HypoTiFlashReplicas map[string]map[string]struct{}                    `json:"hypo-tiflash-replicas,omitempty"`
}
