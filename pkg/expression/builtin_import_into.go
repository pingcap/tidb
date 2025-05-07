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

// Copyright 2015 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package expression

import (
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	_ functionClass = &fileRouteFunctionClass{}
)

type fileRouteFunctionClass struct {
	baseFunctionClass
}

func (c *fileRouteFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, types.ETString)
	if err != nil {
		return nil, err
	}

	sig := &builtinFileRouteSig{bf}
	return sig, nil
}

type builtinFileRouteSig struct {
	baseBuiltinFunc
}

func (b *builtinFileRouteSig) Clone() builtinFunc {
	newSig := &builtinFileRouteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinFileRouteSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	// get the path from the args[0]
	dir, isNULL, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNULL {
		return res, true, err
	}

	// find table and files from dir
	if len(dir) == 0 {
		return res, true, nil
	}

	// create binary json
	tableRoutePath := make(map[string]string, 0)
	bj, err := types.CreateBinaryJSONWithCheck(tableRoutePath)
	if err != nil {
		return res, true, err
	}
	return bj, false, nil
}
