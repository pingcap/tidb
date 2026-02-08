// Copyright 2024 PingCAP, Inc.
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

package expropt

import (
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// PrivilegeChecker provides privilege check for expressions.
type PrivilegeChecker interface {
	// RequestVerification verifies user privilege
	RequestVerification(db, table, column string, priv mysql.PrivilegeType) bool
	// RequestDynamicVerification verifies user privilege for a DYNAMIC privilege.
	RequestDynamicVerification(privName string, grantable bool) bool
}

var _ exprctx.OptionalEvalPropProvider = PrivilegeCheckerProvider(nil)

// PrivilegeCheckerProvider is used to provide PrivilegeChecker.
type PrivilegeCheckerProvider func() PrivilegeChecker

// Desc returns the description for the property key.
func (PrivilegeCheckerProvider) Desc() *exprctx.OptionalEvalPropDesc {
	return exprctx.OptPropPrivilegeChecker.Desc()
}

// PrivilegeCheckerPropReader is used by expression to get PrivilegeChecker.
type PrivilegeCheckerPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (PrivilegeCheckerPropReader) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	return exprctx.OptPropPrivilegeChecker.AsPropKeySet()
}

// GetPrivilegeChecker returns a PrivilegeChecker.
func (PrivilegeCheckerPropReader) GetPrivilegeChecker(ctx exprctx.EvalContext) (PrivilegeChecker, error) {
	p, err := getPropProvider[PrivilegeCheckerProvider](ctx, exprctx.OptPropPrivilegeChecker)
	if err != nil {
		return nil, err
	}
	return p(), nil
}
