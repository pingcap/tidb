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

package ddl

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
)

func needIndexReorg(oldCol, changingCol *model.ColumnInfo) bool {
	if isIntegerChange(oldCol, changingCol) {
		return mysql.HasUnsignedFlag(oldCol.GetFlag()) != mysql.HasUnsignedFlag(changingCol.GetFlag())
	}

	intest.Assert(isCharChange(oldCol, changingCol))

	// Check index key part, ref tablecodec.GenIndexKey
	if !collate.CompatibleCollate(oldCol.GetCollate(), changingCol.GetCollate()) {
		return true
	}

	// Check index value part, ref tablecodec.GenIndexValuePortal
	// TODO(joechenrh): It's better to check each index here, because not all indexes need
	// reorg even if the below condition is true.
	return types.NeedRestoredData(&oldCol.FieldType) != types.NeedRestoredData(&changingCol.FieldType)
}

func needRowReorg(oldCol, changingCol *model.ColumnInfo) bool {
	// Integer changes can skip reorg
	if isIntegerChange(oldCol, changingCol) {
		return false
	}

	// Other changes except char changes need row reorg.
	if !isCharChange(oldCol, changingCol) {
		return true
	}

	// We have checked charset before, only need to check binary string, which needs padding.
	return types.IsBinaryStr(&oldCol.FieldType) || types.IsBinaryStr(&changingCol.FieldType)
}

// checkModifyColumnData checks the values of the old column data
func checkModifyColumnData(
	ctx context.Context,
	w *worker,
	dbName, tblName ast.CIStr,
	oldCol, changingCol *model.ColumnInfo,
	checkValueRange bool,
) (checked bool, err error) {
	// Get sessionctx from context resource pool.
	var sctx sessionctx.Context
	sctx, err = w.sessPool.Get()
	if err != nil {
		return false, errors.Trace(err)
	}
	defer w.sessPool.Put(sctx)

	sql := buildCheckSQLFromModifyColumn(dbName, tblName, oldCol, changingCol, checkValueRange)
	if sql == "" {
		return false, nil
	}

	delayForAsyncCommit()
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(rows) != 0 {
		if rows[0].IsNull(0) {
			return true, dbterror.ErrInvalidUseOfNull
		}

		datum := rows[0].GetDatum(0, &oldCol.FieldType)
		dStr := datumToStringNoErr(datum)
		return true, types.ErrTruncated.GenWithStack("Data truncated for column '%s', value is '%s'", oldCol.Name.L, dStr)
	}

	return true, nil
}

// buildCheckSQLFromModifyColumn builds the SQL to check whether the data
// is valid after modifying to new type.
func buildCheckSQLFromModifyColumn(
	dbName, tblName ast.CIStr,
	oldCol, changingCol *model.ColumnInfo,
	checkValueRange bool,
) string {
	oldTp := oldCol.GetType()
	changingTp := changingCol.GetType()

	var conditions []string
	template := "SELECT %s FROM %s WHERE %s LIMIT 1"
	checkColName := fmt.Sprintf("`%s`", oldCol.Name.O)
	tableName := fmt.Sprintf("`%s`.`%s`", dbName.O, tblName.O)

	if checkValueRange {
		if mysql.IsIntegerType(oldTp) && mysql.IsIntegerType(changingTp) {
			// Integer conversion
			conditions = append(conditions, buildCheckRangeForIntegerTypes(oldCol, changingCol))
		} else {
			conditions = append(conditions, fmt.Sprintf("LENGTH(%s) > %d", checkColName, changingCol.FieldType.GetFlen()))
			if oldTp == mysql.TypeVarchar && changingTp == mysql.TypeString {
				conditions = append(conditions, fmt.Sprintf("%s LIKE '%% '", checkColName))
			}
		}
	}

	if isNullToNotNullChange(oldCol, changingCol) {
		if !(oldTp != mysql.TypeTimestamp && changingTp == mysql.TypeTimestamp) {
			conditions = append(conditions, fmt.Sprintf("`%s` IS NULL", oldCol.Name.O))
		}
	}

	if len(conditions) == 0 {
		return ""
	}

	return fmt.Sprintf(template, checkColName, tableName, strings.Join(conditions, " OR "))
}

// buildCheckRangeForIntegerTypes builds the range check condition for integer type conversion.
func buildCheckRangeForIntegerTypes(oldCol, changingCol *model.ColumnInfo) string {
	changingTp := changingCol.GetType()
	changingUnsigned := mysql.HasUnsignedFlag(changingCol.GetFlag())

	columnName := fmt.Sprintf("`%s`", oldCol.Name.O)

	if changingUnsigned {
		upperBound := types.IntegerUnsignedUpperBound(changingTp)
		return fmt.Sprintf("(%s < 0 OR %s > %d)", columnName, columnName, upperBound)
	}

	lowerBound := types.IntegerSignedLowerBound(changingTp)
	upperBound := types.IntegerSignedUpperBound(changingTp)
	return fmt.Sprintf("(%s < %d OR %s > %d)", columnName, lowerBound, columnName, upperBound)
}

// reorderChangingIdx reorders the changing index infos to match the order of old index infos.
func reorderChangingIdx(oldIdxInfos []*model.IndexInfo, changingIdxInfos []*model.IndexInfo) {
	nameToChanging := make(map[string]*model.IndexInfo, len(changingIdxInfos))
	for _, cIdx := range changingIdxInfos {
		origName := cIdx.Name.O
		if cIdx.State != model.StatePublic {
			origName = cIdx.GetChangingOriginName()
		}
		nameToChanging[origName] = cIdx
	}

	for i, oldIdx := range oldIdxInfos {
		changingIdxInfos[i] = nameToChanging[oldIdx.GetRemovingOriginName()]
	}
}
