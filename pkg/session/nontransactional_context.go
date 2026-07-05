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

package session

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/privilege"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

var nonTransactionalDMLCapturedSysVars = []string{
	variable.SQLModeVar,
	variable.TimeZone,
	variable.CharacterSetClient,
	variable.CharacterSetConnection,
	variable.CharacterSetResults,
	variable.CollationConnection,
	variable.ForeignKeyChecks,
	variable.TiDBConstraintCheckInPlace,
	variable.TiDBConstraintCheckInPlacePessimistic,
	variable.TiDBRedactLog,
}

type nonTransactionalDMLSessionContext struct {
	CurrentDB         string
	SysVars           map[string]string
	User              *auth.UserIdentity
	ActiveRoles       []*auth.RoleIdentity
	ResourceGroup     string
	StmtResourceGroup string
}

func captureNonTransactionalDMLSessionContext(se sessiontypes.Session) (nonTransactionalDMLSessionContext, error) {
	vars := se.GetSessionVars()
	captured := nonTransactionalDMLSessionContext{
		CurrentDB:         vars.CurrentDB,
		SysVars:           make(map[string]string, len(nonTransactionalDMLCapturedSysVars)),
		User:              cloneNonTransactionalDMLUser(vars.User),
		ActiveRoles:       cloneNonTransactionalDMLRoles(vars.ActiveRoles),
		ResourceGroup:     vars.ResourceGroupName,
		StmtResourceGroup: vars.StmtCtx.ResourceGroupName,
	}

	for _, name := range nonTransactionalDMLCapturedSysVars {
		value, err := vars.GetSessionOrGlobalSystemVar(context.Background(), name)
		if err != nil {
			return captured, errors.Trace(err)
		}
		captured.SysVars[name] = value
	}
	return captured, nil
}

func applyNonTransactionalDMLSessionContext(se sessiontypes.Session, captured nonTransactionalDMLSessionContext) error {
	vars := se.GetSessionVars()
	vars.CurrentDB = captured.CurrentDB
	vars.CurrentDBChanged = captured.CurrentDB != ""

	for _, name := range nonTransactionalDMLCapturedSysVars {
		value, ok := captured.SysVars[name]
		if !ok {
			continue
		}
		if err := vars.SetSystemVar(name, value); err != nil {
			return errors.Trace(err)
		}
	}

	pm := privilege.GetPrivilegeManager(se)
	userAuthenticated := false
	if captured.User != nil {
		user := cloneNonTransactionalDMLUser(captured.User)
		if pm != nil {
			userAuthenticated = se.AuthWithoutVerification(user)
		}
		if !userAuthenticated {
			vars.User = user
		}
	}

	roles := cloneNonTransactionalDMLRoles(captured.ActiveRoles)
	if userAuthenticated {
		if pm != nil {
			if ok, role := pm.ActiveRoles(se, roles); !ok {
				return errors.Errorf("failed to activate non-transactional DML worker role %s", role)
			}
		} else {
			vars.ActiveRoles = roles
		}
	} else {
		vars.ActiveRoles = roles
	}

	vars.SetResourceGroupName(captured.ResourceGroup)
	vars.StmtCtx.ResourceGroupName = captured.StmtResourceGroup
	return nil
}

func cloneNonTransactionalDMLUser(user *auth.UserIdentity) *auth.UserIdentity {
	if user == nil {
		return nil
	}
	cloned := *user
	return &cloned
}

func cloneNonTransactionalDMLRoles(roles []*auth.RoleIdentity) []*auth.RoleIdentity {
	if len(roles) == 0 {
		return nil
	}
	clonedRoles := make([]*auth.RoleIdentity, 0, len(roles))
	for _, role := range roles {
		if role == nil {
			clonedRoles = append(clonedRoles, nil)
			continue
		}
		clonedRole := *role
		clonedRoles = append(clonedRoles, &clonedRole)
	}
	return clonedRoles
}
