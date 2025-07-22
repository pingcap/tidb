// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ingest

import (
	"github.com/pingcap/errors"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// TryConvertToKeyExistsErr checks if the input error is ErrFoundDuplicateKeys. If
// so, convert it to a KeyExists error.
func TryConvertToKeyExistsErr(originErr error, idxInfo *model.IndexInfo, tblInfo *model.TableInfo) error {
	tErr, ok := errors.Cause(originErr).(*terror.Error)
	if !ok {
		return originErr
	}
	if tErr.ID() != common.ErrFoundDuplicateKeys.ID() {
		return originErr
	}
	if len(tErr.Args()) != 2 {
		return originErr
	}
	key, keyIsByte := tErr.Args()[0].([]byte)
	value, valIsByte := tErr.Args()[1].([]byte)
	if !keyIsByte || !valIsByte {
		return originErr
	}
	return ddlutil.GenKeyExistsErr(key, value, idxInfo, tblInfo)
}
