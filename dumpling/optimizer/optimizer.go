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

package optimizer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/optimizer/plan"
)

// Optimize do optimization and create a Plan.
// InfoSchema has to be passed in as parameter because
// it can not be changed after binding.
func Optimize(is infoschema.InfoSchema, node ast.Node) (plan.Plan, error) {
	var va validator
	node.Accept(&va)
	if va.err != nil {
		return nil, errors.Trace(va.err)
	}
	binder := &InfoBinder{Info: is}
	node.Accept(binder)
	if binder.Err != nil {
		return nil, errors.Trace(binder.Err)
	}
	tc := &typeComputer{}
	node.Accept(tc)
	if tc.err != nil {
		return nil, errors.Trace(tc.err)
	}
	cn := &conditionNormalizer{}
	node.Accept(cn)
	var builder planBuilder
	p, err := builder.build(node)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var picker indexPicker
	p, _ = p.Accept(&picker)
	return p, nil
}

// Supported checks if the node is supported to use new plan.
func Supported(node ast.Node) bool {
	return false
}
