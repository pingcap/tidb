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
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

const (
	codeCantGetValidID terror.ErrCode = 1
	codeCantSetToNull  terror.ErrCode = 2
)

var (
	errCantGetValidID = terror.ClassVariable.New(codeCantGetValidID, "cannot get valid auto-increment id in retry")
	errCantSetToNull  = terror.ClassVariable.New(codeCantSetToNull, "cannot set variable to null")
)

// RetryInfo saves retry information.
type RetryInfo struct {
	Retrying         bool
	currRetryOff     int
	autoIncrementIDs []int64
	// Attempts is the current number of retry attempts.
	Attempts int
}

// Clean does some clean work.
func (r *RetryInfo) Clean() {
	r.currRetryOff = 0
	if len(r.autoIncrementIDs) > 0 {
		r.autoIncrementIDs = r.autoIncrementIDs[:0]
	}
	r.Attempts = 0
}

// AddAutoIncrementID adds id to AutoIncrementIDs.
func (r *RetryInfo) AddAutoIncrementID(id int64) {
	r.autoIncrementIDs = append(r.autoIncrementIDs, id)
}

// ResetOffset resets the current retry offset.
func (r *RetryInfo) ResetOffset() {
	r.currRetryOff = 0
}

// GetCurrAutoIncrementID gets current AutoIncrementID.
func (r *RetryInfo) GetCurrAutoIncrementID() (int64, error) {
	if r.currRetryOff >= len(r.autoIncrementIDs) {
		return 0, errCantGetValidID
	}
	id := r.autoIncrementIDs[r.currRetryOff]
	r.currRetryOff++

	return id, nil
}

// SessionVars is to handle user-defined or global variables in current session.
type SessionVars struct {
	// user-defined variables
	Users map[string]string
	// system variables
	systems map[string]string
	// prepared statement
	PreparedStmts        map[uint32]interface{}
	PreparedStmtNameToID map[string]uint32
	// prepared statement auto increment id
	preparedStmtID uint32

	// retry information
	RetryInfo *RetryInfo

	// following variables are special for current session
	Status       uint16
	LastInsertID uint64
	AffectedRows uint64

	// Client capability
	ClientCapability uint32

	// Connection ID
	ConnectionID uint64

	// Found rows
	FoundRows uint64

	// Current user
	User string

	// Strict SQL mode
	StrictSQLMode bool

	// CommonGlobalLoaded indicates if common global variable has been loaded for this session.
	CommonGlobalLoaded bool

	// InUpdateStmt indicates if the session is handling update stmt.
	InUpdateStmt bool

	// InRestrictedSQL indicates if the session is handling restricted SQL execution.
	InRestrictedSQL bool

	// SnapshotTS is used for reading history data. For simplicity, SnapshotTS only supports distsql request.
	SnapshotTS uint64

	// SnapshotInfoschema is used with SnapshotTS, when the schema version at snapshotTS less than current schema
	// version, we load an old version schema for query.
	SnapshotInfoschema interface{}

	// SkipConstraintCheck is true when importing data.
	SkipConstraintCheck bool
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
		Users:                make(map[string]string),
		systems:              make(map[string]string),
		PreparedStmts:        make(map[uint32]interface{}),
		PreparedStmtNameToID: make(map[string]uint32),
		RetryInfo:            &RetryInfo{},
		StrictSQLMode:        true,
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

const (
	characterSetConnection = "character_set_connection"
	collationConnection    = "collation_connection"
)

// GetCharsetInfo gets charset and collation for current context.
// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and collation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// collation_connection is important for comparisons of literal strings.
// For comparisons of strings with column values, collation_connection does not matter because columns
// have their own collation, which has a higher collation precedence.
// See https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
func GetCharsetInfo(ctx context.Context) (charset, collation string) {
	sessionVars := GetSessionVars(ctx)
	charset = sessionVars.systems[characterSetConnection]
	collation = sessionVars.systems[collationConnection]
	return
}

// GetSnapshotTS gets snapshot timestamp that has been set by set variable statement.
func GetSnapshotTS(ctx context.Context) uint64 {
	sessionVars := GetSessionVars(ctx)
	return sessionVars.SnapshotTS
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

// GetStatusFlag gets the session server status variable, returns true if it is on.
func (s *SessionVars) GetStatusFlag(flag uint16) bool {
	return s.Status&flag > 0
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

// special session variables.
const (
	SQLModeVar          = "sql_mode"
	AutocommitVar       = "autocommit"
	characterSetResults = "character_set_results"
)

// SetSystemVar sets a system variable.
func (s *SessionVars) SetSystemVar(key string, value types.Datum) error {
	key = strings.ToLower(key)
	if value.IsNull() {
		if key != characterSetResults {
			return errCantSetToNull
		}
		delete(s.systems, key)
		return nil
	}
	sVal, err := value.ToString()
	if err != nil {
		return errors.Trace(err)
	}
	switch key {
	case SQLModeVar:
		sVal = strings.ToUpper(sVal)
		if strings.Contains(sVal, "STRICT_TRANS_TABLES") || strings.Contains(sVal, "STRICT_ALL_TABLES") {
			s.StrictSQLMode = true
		} else {
			s.StrictSQLMode = false
		}
	case TiDBSnapshot:
		err = s.setSnapshotTS(sVal)
		if err != nil {
			return errors.Trace(err)
		}
	case AutocommitVar:
		isAutocommit := strings.EqualFold(sVal, "ON") || sVal == "1"
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
	case TiDBSkipConstraintCheck:
		s.setSkipConstraintCheck(sVal)
	}
	s.systems[key] = sVal
	return nil
}

// epochShiftBits is used to reserve logical part of the timestamp.
const epochShiftBits = 18

func (s *SessionVars) setSnapshotTS(sVal string) error {
	if sVal == "" {
		s.SnapshotTS = 0
		return nil
	}
	t, err := mysql.ParseTime(sVal, mysql.TypeTimestamp, mysql.MaxFsp)
	if err != nil {
		return errors.Trace(err)
	}
	ts := (t.UnixNano() / int64(time.Millisecond)) << epochShiftBits
	s.SnapshotTS = uint64(ts)
	return nil
}

func (s *SessionVars) setSkipConstraintCheck(sVal string) {
	if sVal == "1" {
		s.SkipConstraintCheck = true
	} else {
		s.SkipConstraintCheck = false
	}
}

// GetSystemVar gets a system variable.
func (s *SessionVars) GetSystemVar(key string) types.Datum {
	var d types.Datum
	key = strings.ToLower(key)
	sVal, ok := s.systems[key]
	if ok {
		d.SetString(sVal)
	} else {
		// TiDBSkipConstraintCheck is a session scope vars. We do not store it in the global table.
		if key == TiDBSkipConstraintCheck {
			d.SetString(SysVars[TiDBSkipConstraintCheck].Value)
		}
	}
	return d
}

// GetTiDBSystemVar gets variable value for name.
// The variable should be a TiDB specific system variable (The vars in tidbSysVars map).
// We load the variable from session first, if not found, use local defined default variable.
func (s *SessionVars) GetTiDBSystemVar(ctx context.Context, name string) (string, error) {
	key := strings.ToLower(name)
	_, ok := tidbSysVars[key]
	if !ok {
		return "", errors.Errorf("%s is not a TiDB specific system variable.", name)
	}

	sVal, ok := s.systems[key]
	if ok {
		return sVal, nil
	}
	return SysVars[key].Value, nil
}
