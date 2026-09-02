// Copyright 2026 PingCAP, Inc.
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

package privileges

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

func TestUserAttrFilterNilManagerAllowsAllRows(t *testing.T) {
	filter := NewUserAttrFilter(nil, "viewer", "%", nil)
	if !filter.Visible("alice", "%") || !filter.Visible("root", "localhost") {
		t.Fatal("a nil privilege manager must preserve unrestricted user-attribute visibility")
	}
}

func TestUserAttrFilterRestrictsSelfAndNonSystemRows(t *testing.T) {
	priv := newMySQLPrivilege()
	newRecord := func(user string, privileges mysql.PrivilegeType) UserRecord {
		record := UserRecord{
			baseRecord: baseRecord{Host: "localhost", User: user},
			Privileges: privileges,
		}
		record.patChars, record.patTypes = stringutil.CompilePatternBinary(record.Host, '\\')
		return record
	}
	priv.SetUser([]UserRecord{
		newRecord("viewer", 0),
		newRecord("creator", mysql.CreateUserPriv),
		newRecord("operator", mysql.SelectPriv),
		newRecord("ordinary", 0),
		newRecord("system", 0),
	})
	priv.dynamicPriv.ReplaceOrInsert(itemDynamicPriv{
		username: "system",
		data: []dynamicPrivRecord{{
			baseRecord:    newRecord("system", 0).baseRecord,
			PrivilegeName: "SYSTEM_USER",
		}},
	})
	handle := &Handle{}
	manager := func(user string) *UserPrivileges {
		handle.priv.Store(priv)
		return &UserPrivileges{Handle: handle, user: user, host: "localhost"}
	}

	selfOnly := NewUserAttrFilter(nil, "viewer", "localhost", manager("viewer"))
	if !selfOnly.Visible("viewer", "localhost") || selfOnly.Visible("ordinary", "localhost") {
		t.Fatal("users without elevated privileges must see only their own row")
	}
	nonSystem := NewUserAttrFilter(nil, "creator", "localhost", manager("creator"))
	if !nonSystem.Visible("ordinary", "localhost") || nonSystem.Visible("system", "localhost") {
		t.Fatal("CREATE USER without SYSTEM_USER must hide system-user rows")
	}
	allRows := NewUserAttrFilter(nil, "operator", "localhost", manager("operator"))
	if !allRows.Visible("ordinary", "localhost") || !allRows.Visible("system", "localhost") {
		t.Fatal("SELECT on mysql.user must expose all user-attribute rows")
	}
}

func TestOperateViewMakesDatabaseVisible(t *testing.T) {
	record := NewUserRecord("localhost", "operator")
	record.Privileges = mysql.OperateViewPriv
	record.patChars, record.patTypes = stringutil.CompilePatternBinary(record.Host, '\\')
	priv := newMySQLPrivilege()
	priv.SetUser([]UserRecord{record})
	if !priv.DBIsVisible("operator", "localhost", "app") {
		t.Fatal("OPERATE VIEW must make a database visible")
	}
}
