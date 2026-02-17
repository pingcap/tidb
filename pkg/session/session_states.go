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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"go.uber.org/zap"
)
func (s *session) EncodeStates(ctx context.Context,
	sessionStates *sessionstates.SessionStates) error {
	// Transaction status is hard to encode, so we do not support it.
	s.txn.mu.Lock()
	valid := s.txn.Valid()
	s.txn.mu.Unlock()
	if valid {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has an active transaction")
	}
	// Data in local temporary tables is hard to encode, so we do not support it.
	// Check temporary tables here to avoid circle dependency.
	if s.sessionVars.LocalTemporaryTables != nil {
		localTempTables := s.sessionVars.LocalTemporaryTables.(*infoschema.SessionTables)
		if localTempTables.Count() > 0 {
			return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has local temporary tables")
		}
	}
	// The advisory locks will be released when the session is closed.
	if len(s.advisoryLocks) > 0 {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has advisory locks")
	}
	// The TableInfo stores session ID and server ID, so the session cannot be migrated.
	if len(s.lockedTables) > 0 {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has locked tables")
	}
	// It's insecure to migrate sandBoxMode because users can fake it.
	if s.InSandBoxMode() {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session is in sandbox mode")
	}

	if err := s.sessionVars.EncodeSessionStates(ctx, sessionStates); err != nil {
		return err
	}
	sessionStates.ResourceGroupName = s.sessionVars.ResourceGroupName

	hasRestrictVarPriv := false
	checker := privilege.GetPrivilegeManager(s)
	if checker == nil || checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESTRICTED_VARIABLES_ADMIN", false) {
		hasRestrictVarPriv = true
	}
	// Encode session variables. We put it here instead of SessionVars to avoid cycle import.
	sessionStates.SystemVars = make(map[string]string)
	for _, sv := range variable.GetSysVars() {
		switch {
		case sv.HasNoneScope(), !sv.HasSessionScope():
			// Hidden attribute is deprecated.
			// None-scoped variables cannot be modified.
			// Noop variables should also be migrated even if they are noop.
			continue
		case sv.ReadOnly:
			// Skip read-only variables here. We encode them into SessionStates manually.
			continue
		}
		// Get all session variables because the default values may change between versions.
		val, keep, err := s.sessionVars.GetSessionStatesSystemVar(sv.Name)
		switch {
		case err != nil:
			return err
		case !keep:
			continue
		case !hasRestrictVarPriv && sem.IsEnabled() && sem.IsInvisibleSysVar(sv.Name):
			// If the variable has a global scope, it should be the same with the global one.
			// Otherwise, it should be the same with the default value.
			defaultVal := sv.Value
			if sv.HasGlobalScope() {
				// If the session value is the same with the global one, skip it.
				if defaultVal, err = sv.GetGlobalFromHook(ctx, s.sessionVars); err != nil {
					return err
				}
			}
			if val != defaultVal {
				// Case 1: the RESTRICTED_VARIABLES_ADMIN is revoked after setting the session variable.
				// Case 2: the global variable is updated after the session is created.
				// In any case, the variable can't be set in the new session, so give up.
				return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs(fmt.Sprintf("session has set invisible variable '%s'", sv.Name))
			}
		default:
			sessionStates.SystemVars[sv.Name] = val
		}
	}

	// Encode prepared statements and sql bindings.
	for _, handler := range s.sessionStatesHandlers {
		if err := handler.EncodeSessionStates(ctx, s, sessionStates); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) DecodeStates(ctx context.Context,
	sessionStates *sessionstates.SessionStates) error {
	// Decode prepared statements and sql bindings.
	for _, handler := range s.sessionStatesHandlers {
		if err := handler.DecodeSessionStates(ctx, s, sessionStates); err != nil {
			return err
		}
	}

	// Decode session variables.
	names := variable.OrderByDependency(sessionStates.SystemVars)
	// Some variables must be set before others, e.g. tidb_enable_noop_functions should be before noop variables.
	for _, name := range names {
		val := sessionStates.SystemVars[name]
		// Experimental system variables may change scope, data types, or even be removed.
		// We just ignore the errors and continue.
		if err := s.sessionVars.SetSystemVar(name, val); err != nil {
			logutil.Logger(ctx).Warn("set session variable during decoding session states error",
				zap.String("name", name), zap.String("value", val), zap.Error(err))
		}
	}

	// Put resource group privilege check from sessionVars to session to avoid circular dependency.
	if sessionStates.ResourceGroupName != s.sessionVars.ResourceGroupName {
		hasPriv := true
		if vardef.EnableResourceControlStrictMode.Load() {
			checker := privilege.GetPrivilegeManager(s)
			if checker != nil {
				hasRgAdminPriv := checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESOURCE_GROUP_ADMIN", false)
				hasRgUserPriv := checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESOURCE_GROUP_USER", false)
				hasPriv = hasRgAdminPriv || hasRgUserPriv
			}
		}
		if hasPriv {
			s.sessionVars.SetResourceGroupName(sessionStates.ResourceGroupName)
		} else {
			logutil.Logger(ctx).Warn("set session states error, no privilege to set resource group, skip changing resource group",
				zap.String("source_resource_group", s.sessionVars.ResourceGroupName), zap.String("target_resource_group", sessionStates.ResourceGroupName))
		}
	}

	// Decoding session vars / prepared statements may override stmt ctx, such as warnings,
	// so we decode stmt ctx at last.
	return s.sessionVars.DecodeSessionStates(ctx, sessionStates)
}

func (s *session) setRequestSource(ctx context.Context, stmtLabel string, stmtNode ast.StmtNode) {
	if !s.isInternal() {
		if txn, _ := s.Txn(false); txn != nil && txn.Valid() {
			if txn.IsPipelined() {
				stmtLabel = "pdml"
			}
			txn.SetOption(kv.RequestSourceType, stmtLabel)
		}
		s.sessionVars.RequestSourceType = stmtLabel
		return
	}
	if source := ctx.Value(kv.RequestSourceKey); source != nil {
		requestSource := source.(kv.RequestSource)
		if requestSource.RequestSourceType != "" {
			s.sessionVars.RequestSourceType = requestSource.RequestSourceType
			return
		}
	}
	// panic in test mode in case there are requests without source in the future.
	// log warnings in production mode.
	if intest.EnableInternalCheck {
		panic("unexpected no source type context, if you see this error, " +
			"the `RequestSourceTypeKey` is missing in your context")
	}
	logutil.Logger(ctx).Warn("unexpected no source type context, if you see this warning, "+
		"the `RequestSourceTypeKey` is missing in the context",
		zap.Bool("internal", s.isInternal()),
		zap.String("sql", stmtNode.Text()))
}

// NewStmtIndexUsageCollector creates a new `*indexusage.StmtIndexUsageCollector` based on the internal session index
// usage collector
func (s *session) NewStmtIndexUsageCollector() *indexusage.StmtIndexUsageCollector {
	if s.idxUsageCollector == nil {
		return nil
	}

	return indexusage.NewStmtIndexUsageCollector(s.idxUsageCollector)
}
