// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"github.com/pingcap/tidb/context"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/stmt"
)

// SessionVars is to handle user-defined or global varaibles in current session
type SessionVars struct {
	// user-defined variables
	Users map[string]string
	// system variables
	Systems map[string]string
	// prepared statement
	PreparedStmts map[string]stmt.Statement
	// prepared statement auto increament id
	preparedStmtID uint32

	// following variables are specail for current session
	Status       uint16
	LastInsertID uint64
	AffectedRows uint64

	// Client Capability
	ClientCapability uint32 // Client capability

	// Found rows
	FoundRows uint64
}

// sessionVarsKeyType is a dummy type to avoid naming collision in context.
type sessionVarsKeyType int

// define a Stringer function for debugging and pretty printting
func (k sessionVarsKeyType) String() string {
	return "session_vars"
}

const sessionVarsKey sessionVarsKeyType = 0

// BindSessionVars creates a session vars object and bind it to context
func BindSessionVars(ctx context.Context) {
	v := &SessionVars{
		Users:         make(map[string]string),
		Systems:       make(map[string]string),
		PreparedStmts: make(map[string]stmt.Statement),
	}

	ctx.SetValue(sessionVarsKey, v)
}

// GetSessionVars gets the session vars from context
func GetSessionVars(ctx context.Context) *SessionVars {
	v, ok := ctx.Value(sessionVarsKey).(*SessionVars)
	if !ok {
		return nil
	}
	return v
}

// SetLastInsertID saves the last insert id to the session context
func (s *SessionVars) SetLastInsertID(insertID uint64) {
	s.LastInsertID = insertID

	// TODO: we may store the result for last_insert_id sys var later.
}

// SetAffectedRows saves the affected rows to the session context
func (s *SessionVars) SetAffectedRows(affectedRows uint64) {
	s.AffectedRows = affectedRows
}

// AddAffectedRows adds affected rows with the argument rows
func (s *SessionVars) AddAffectedRows(rows uint64) {
	s.AffectedRows += rows
}

// AddFoundRows adds found rows with the argument rows
func (s *SessionVars) AddFoundRows(rows uint64) {
	s.FoundRows += rows
}

// SetStatus sets the session server status variable
func (s *SessionVars) SetStatus(status uint16) {
	s.Status = status
}

// SetStatusInTrans sets the status flags about ServerStatusInTrans.
func (s *SessionVars) SetStatusInTrans(isInTrans bool) {
	if isInTrans {
		s.Status |= mysql.ServerStatusInTrans
		return
	}
	s.Status &= (^mysql.ServerStatusInTrans)
}

// GetNextPreparedStmtID generates and return the next session scope prepared statement id
func (s *SessionVars) GetNextPreparedStmtID() uint32 {
	s.preparedStmtID++
	return s.preparedStmtID
}

// ShouldAutocommit checks if it is in autocommit enviroment
func ShouldAutocommit(ctx context.Context) bool {
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK.
	if GetSessionVars(ctx).Status&mysql.ServerStatusAutocommit > 0 &&
		GetSessionVars(ctx).Status&mysql.ServerStatusInTrans > 0 {
		return true
	}
	return false
}
