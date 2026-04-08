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

package exprctx

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
)

// ErrParamIndexExceedParamCounts describes the error that the index exceed the count of all params
var ErrParamIndexExceedParamCounts = errors.New("Param index exceed param counts")

// ParamValues is a readonly interface to return param
type ParamValues interface {
	// GetParamValue returns the value of the parameter by index.
	GetParamValue(idx int) (types.Datum, error)
}

// EmptyParamValues is the `ParamValues` which contains nothing
var EmptyParamValues ParamValues = &emptyParamValues{}

type emptyParamValues struct{}

// GetParamValue always returns the `ErrParamIndexExceedParamCounts` for any index
func (e *emptyParamValues) GetParamValue(_ int) (types.Datum, error) {
	return types.Datum{}, ErrParamIndexExceedParamCounts
}
