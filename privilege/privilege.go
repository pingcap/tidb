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

package privilege

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

type keyType int

func (k keyType) String() string {
	return "privilege-key"
}

// Manager is the interface for providing privilege related operations.
type Manager interface {
	// Show granted privileges for user.
	ShowGrants(ctx context.Context, user string) ([]string, error)

	// RequestVerification verifies user privilege for the request.
	// If table is "", only check global/db scope privileges.
	// If table is not "", check global/db/table scope privileges.
	RequestVerification(db, table, column string, priv mysql.PrivilegeType) bool
	// ConnectionVerification verifies user privilege for connection.
	ConnectionVerification(host, user string, auth, salt []byte) bool

	// DBIsVisible returns true is the database is visible to current user.
	DBIsVisible(db string) bool

	// UserPrivilegesTable provide data for INFORMATION_SCHEMA.USERS_PRIVILEGE table.
	UserPrivilegesTable() [][]types.Datum
}

const key keyType = 0

// BindPrivilegeManager binds Manager to context.
func BindPrivilegeManager(ctx context.Context, pc Manager) {
	ctx.SetValue(key, pc)
}

// GetPrivilegeManager gets Checker from context.
func GetPrivilegeManager(ctx context.Context) Manager {
	if v, ok := ctx.Value(key).(Manager); ok {
		return v
	}
	return nil
}
