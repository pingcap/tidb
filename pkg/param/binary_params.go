// Copyright 2023 PingCAP, Inc.
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

package param

import (
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

var ErrUnknownFieldType = dbterror.ClassServer.NewStd(errno.ErrUnknownFieldType)

// BinaryParam stores the information decoded from the binary protocol
// It can be further parsed into `expression.Expression` through the expression.ExecBinaryParam function in expression package
type BinaryParam struct {
	Tp         byte
	IsUnsigned bool
	IsNull     bool
	Val        []byte
}
