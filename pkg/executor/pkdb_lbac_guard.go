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

package executor

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/lbac"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

type lbacDMLGuard struct {
	policyName     string
	labelColID     int64
	labelColOffset int
	validator      *lbac.SecurityLabelValidator
	accessCheck    *lbac.SecurityLabelAccessChecker
}

func newLBACDMLGuard(ctx sessionctx.Context, tblInfo *model.TableInfo) (*lbacDMLGuard, error) {
	if tblInfo.SecurityPolicy == nil || tblInfo.SecurityPolicy.L == "" {
		return nil, nil
	}
	labelColID := int64(0)
	labelColOffset := -1
	for _, col := range tblInfo.Columns {
		if col != nil && col.FieldType.IsSecurityLabel() {
			labelColID = col.ID
			labelColOffset = col.Offset
			break
		}
	}
	if labelColOffset == -1 {
		return nil, nil
	}

	cache, err := privilege.GetSecurityLabelCache(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	validator, err := lbac.NewSecurityLabelValidator(cache)
	if err != nil {
		return nil, err
	}
	guard := &lbacDMLGuard{
		policyName:     tblInfo.SecurityPolicy.L,
		labelColID:     labelColID,
		labelColOffset: labelColOffset,
		validator:      validator,
	}
	privChecker := privilege.GetPrivilegeManager(ctx)
	currentUser := ctx.GetSessionVars().User
	if currentUser != nil && (privChecker == nil || !privChecker.RequestVerification(ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)) {
		userName, hostName := auth.GetUserAndHostName(currentUser)
		accessChecker, err := lbac.NewSecurityLabelAccessChecker(cache, userName, hostName, ast.SecurityLabelAccessTypeWrite)
		if err != nil {
			return nil, err
		}
		guard.accessCheck = accessChecker
	}
	return guard, nil
}

func (g *lbacDMLGuard) CheckRowLabelAccess(labelDatum types.Datum) error {
	if g.accessCheck == nil {
		return nil
	}
	if labelDatum.IsNull() {
		return exeerrors.ErrRowLabelUnAccessible.GenWithStackByArgs("NULL", g.policyName)
	}
	labelValue, labelString, err := g.validator.NormalizeLabelValue(g.policyName, labelDatum.GetBytes())
	if err != nil {
		return err
	}
	allowed, err := g.accessCheck.CheckLabelValue(g.policyName, labelValue)
	if err != nil {
		return err
	}
	if !allowed {
		return exeerrors.ErrRowLabelUnAccessible.GenWithStackByArgs(labelString, g.policyName)
	}
	return nil
}

func (g *lbacDMLGuard) EnforceWriteLabel(labelDatum types.Datum) (types.Datum, error) {
	if labelDatum.IsNull() {
		return labelDatum, nil
	}
	labelValue, labelString, err := g.validator.NormalizeLabelValue(g.policyName, labelDatum.GetBytes())
	if err != nil {
		return types.Datum{}, err
	}
	encodedDatum := types.NewBytesDatum(labelValue)
	if g.accessCheck == nil {
		return encodedDatum, nil
	}
	allowed, err := g.accessCheck.CheckLabelValue(g.policyName, labelValue)
	if err != nil {
		return types.Datum{}, err
	}
	if allowed {
		return encodedDatum, nil
	}
	writeControlOverride, err := g.accessCheck.IsWriteControlOverride(g.policyName)
	if err != nil {
		return types.Datum{}, err
	}
	if !writeControlOverride {
		return types.Datum{}, exeerrors.ErrRowLabelUnAccessible.GenWithStackByArgs(labelString, g.policyName)
	}
	overrideLabel, _, ok, err := g.accessCheck.PreferredWriteLabel(g.policyName)
	if err != nil {
		return types.Datum{}, err
	}
	if !ok || len(overrideLabel) == 0 {
		return types.Datum{}, exeerrors.ErrRowLabelUnAccessible.GenWithStackByArgs(labelString, g.policyName)
	}
	return types.NewBytesDatum(overrideLabel), nil
}
