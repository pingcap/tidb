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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
)

type keyType int

func (k keyType) String() string {
	return "privilege-key"
}

type PrivilegeChecker interface {
	SetUser(user string)
	CheckPrivilege(ctx context.Context, db *model.DBInfo, privilege mysql.PrivilegeType) (bool, error)
	CheckPrivilege(ctx context.Context, db *model.DBInfo, tbl *model.TableInfo, privilege mysql.PrivilegeType) (bool, error)
}

// BindPrivilegeChecker binds domain to context.
func BindPrivilegeChecker(ctx context.Context, pc PrivilegeChecker) {
	ctx.SetValue(keyType, pc)
}

// GetPrivilegeChecker gets domain from context.
func GetPrivilegeChecker(ctx context.Context) Privilege {
	v, ok := ctx.Value(keyType).(PrivilegeChecker)
	if !ok {
		return nil
	}
	return v
}
