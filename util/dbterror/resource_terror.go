// Copyright 2022 PingCAP, Inc.
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

package dbterror

import (
	mysql "github.com/pingcap/tidb/errno"
)

var (
	// ErrUnsupportedBackendOpType means the backend operation type is not currently supported.
	ErrUnsupportedBackendOpType = ClassResource.NewStd(mysql.ErrUnsupportedBackendOpType)
	// ErrDupPauseOperation means the pause operation for specific type has been issued already.
	ErrDupPauseOperation = ClassResource.NewStd(mysql.ErrDuplicatePauseOperation)
	// ErrNoPauseForResume mean there is no count-part pause for resume operation.
	ErrNoPauseForResume = ClassResource.NewStd(mysql.ErrNoCounterPauseForResume)
	// ErrOnIssuePauseOperation means there is error occurs during pause|resume operation execution.
	ErrOnIssuePauseOperation = ClassResource.NewStd(mysql.ErrOnExecOperation)
)
