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
)

// SessionVars is to handle user-defined or global variables in current session.
type SessionVars struct {
	// user-defined variables
	Users map[string]string
	// system variables
	Systems map[string]string
	// prepared statement
	PreparedStmts map[string]interface{}
	// prepared statement auto increment id
	preparedStmtID uint32

	// following variables are special for current session
	Status       uint16
	LastInsertID uint64
	AffectedRows uint64

	// Client capability
	ClientCapability uint32

	// Found rows
	FoundRows uint64

	// Current user
	User string
}

// sessionVarsKeyType is a dummy type to avoid naming collision in context.
type sessionVarsKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k sessionVarsKeyType) String() string {
	return "session_vars"
}

const sessionVarsKey sessionVarsKeyType = 0

// BindSessionVars creates a session vars object and binds it to context.
func BindSessionVars(ctx context.Context) {
	v := &SessionVars{
		Users:         make(map[string]string),
		Systems:       make(map[string]string),
		PreparedStmts: make(map[string]interface{}),
	}

	ctx.SetValue(sessionVarsKey, v)
}

// GetSessionVars gets the session vars from context.
func GetSessionVars(ctx context.Context) *SessionVars {
	v, ok := ctx.Value(sessionVarsKey).(*SessionVars)
	if !ok {
		return nil
	}
	return v
}

// SetLastInsertID saves the last insert id to the session context.
// TODO: we may store the result for last_insert_id sys var later.
func (s *SessionVars) SetLastInsertID(insertID uint64) {
	s.LastInsertID = insertID
}

// SetAffectedRows saves the affected rows to the session context.
func (s *SessionVars) SetAffectedRows(affectedRows uint64) {
	s.AffectedRows = affectedRows
}

// AddAffectedRows adds affected rows with the argument rows.
func (s *SessionVars) AddAffectedRows(rows uint64) {
	s.AffectedRows += rows
}

// AddFoundRows adds found rows with the argument rows.
func (s *SessionVars) AddFoundRows(rows uint64) {
	s.FoundRows += rows
}

// SetStatusFlag sets the session server status variable.
// If on is ture sets the flag in session status,
// otherwise removes the flag.
func (s *SessionVars) SetStatusFlag(flag uint16, on bool) {
	if on {
		s.Status |= flag
		return
	}
	s.Status &= (^flag)
}

// GetNextPreparedStmtID generates and returns the next session scope prepared statement id.
func (s *SessionVars) GetNextPreparedStmtID() uint32 {
	s.preparedStmtID++
	return s.preparedStmtID
}

// SetCurrentUser saves the current user to the session context.
func (s *SessionVars) SetCurrentUser(user string) {
	s.User = user
}
