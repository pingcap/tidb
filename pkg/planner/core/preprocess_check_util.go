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

package core

import (
	"math"
	"strings"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// checkDuplicateColumnName checks if index exists duplicated columns.
func checkDuplicateColumnName(indexPartSpecifications []*ast.IndexPartSpecification) error {
	colNames := make(map[string]struct{}, len(indexPartSpecifications))
	for _, IndexColNameWithExpr := range indexPartSpecifications {
		if IndexColNameWithExpr.Column != nil {
			name := IndexColNameWithExpr.Column.Name
			if _, ok := colNames[name.L]; ok {
				return infoschema.ErrColumnExists.GenWithStackByArgs(name)
			}
			colNames[name.L] = struct{}{}
		}
	}
	return nil
}

// checkIndexInfo checks index name, index column names and prefix lengths.
func checkIndexInfo(indexName string, indexPartSpecifications []*ast.IndexPartSpecification) error {
	if strings.EqualFold(indexName, mysql.PrimaryKeyName) {
		return dbterror.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
	}
	if len(indexPartSpecifications) > mysql.MaxKeyParts {
		return infoschema.ErrTooManyKeyParts.GenWithStackByArgs(mysql.MaxKeyParts)
	}
	for _, idxSpec := range indexPartSpecifications {
		// -1 => unspecified/full, > 0 OK, 0 => error
		if idxSpec.Expr == nil && idxSpec.Length == 0 {
			return plannererrors.ErrKeyPart0.GenWithStackByArgs(idxSpec.Column.Name.O)
		}
	}
	return checkDuplicateColumnName(indexPartSpecifications)
}

// checkUnsupportedTableOptions checks if there exists unsupported table options
func checkUnsupportedTableOptions(options []*ast.TableOption) error {
	var err error
	for _, option := range options {
		switch option.Tp {
		case ast.TableOptionUnion:
			err = dbterror.ErrTableOptionUnionUnsupported
		case ast.TableOptionInsertMethod:
			err = dbterror.ErrTableOptionInsertMethodUnsupported
		case ast.TableOptionEngine:
			err = checkTableEngine(option.StrValue)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

var mysqlValidTableEngineNames = map[string]struct{}{
	"archive":    {},
	"blackhole":  {},
	"csv":        {},
	"example":    {},
	"federated":  {},
	"innodb":     {},
	"memory":     {},
	"merge":      {},
	"mgr_myisam": {},
	"myisam":     {},
	"ndb":        {},
	"heap":       {},
	"aria":       {}, // https://mariadb.com/docs/server/server-usage/storage-engines/aria/aria-storage-engine
	"myrocks":    {}, // https://mariadb.com/docs/server/server-usage/storage-engines/myrocks
	"tokudb":     {}, // https://mariadb.com/docs/server/server-usage/storage-engines/legacy-storage-engines/tokudb
}

func checkTableEngine(engineName string) error {
	if _, have := mysqlValidTableEngineNames[strings.ToLower(engineName)]; !have {
		return dbterror.ErrUnknownEngine.GenWithStackByArgs(engineName)
	}
	return nil
}

func checkReferInfoForTemporaryTable(tableMetaInfo *model.TableInfo) error {
	if tableMetaInfo.AutoRandomBits != 0 {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random")
	}
	if tableMetaInfo.PreSplitRegions != 0 {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions")
	}
	if tableMetaInfo.Partition != nil {
		return plannererrors.ErrPartitionNoTemporary
	}
	if tableMetaInfo.ShardRowIDBits != 0 {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits")
	}
	if tableMetaInfo.PlacementPolicyRef != nil {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("placement")
	}

	return nil
}

// checkColumn checks if the column definition is valid.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func checkColumn(colDef *ast.ColumnDef) error {
	// Check column name.
	cName := colDef.Name.Name.String()
	if util.IsInCorrectIdentifierName(cName) {
		return dbterror.ErrWrongColumnName.GenWithStackByArgs(cName)
	}

	if isInvalidDefaultValue(colDef) {
		return types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
	}

	// Check column type.
	tp := colDef.Tp
	if tp == nil {
		return nil
	}
	if tp.GetFlen() > math.MaxUint32 {
		return types.ErrTooBigDisplayWidth.GenWithStack("Display width out of range for column '%s' (max = %d)", colDef.Name.Name.O, math.MaxUint32)
	}

	switch tp.GetType() {
	case mysql.TypeString:
		if tp.GetFlen() != types.UnspecifiedLength && tp.GetFlen() > mysql.MaxFieldCharLength {
			return types.ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, mysql.MaxFieldCharLength)
		}
	case mysql.TypeVarchar:
		if len(tp.GetCharset()) == 0 {
			// It's not easy to get the schema charset and table charset here.
			// The charset is determined by the order ColumnDefaultCharset --> TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
			// return nil, to make the check in the ddl.CreateTable.
			return nil
		}
		err := types.IsVarcharTooBigFieldLength(colDef.Tp.GetFlen(), colDef.Name.Name.O, tp.GetCharset())
		if err != nil {
			return err
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		// For FLOAT, the SQL standard permits an optional specification of the precision.
		// https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
		if tp.GetDecimal() == -1 {
			switch tp.GetType() {
			case mysql.TypeDouble:
				// For Double type flen and decimal check is moved to parser component
			default:
				if tp.GetFlen() > mysql.MaxDoublePrecisionLength {
					return types.ErrWrongFieldSpec.GenWithStackByArgs(colDef.Name.Name.O)
				}
			}
		} else {
			if tp.GetDecimal() > mysql.MaxFloatingTypeScale {
				return types.ErrTooBigScale.GenWithStackByArgs(tp.GetDecimal(), colDef.Name.Name.O, mysql.MaxFloatingTypeScale)
			}
			if tp.GetFlen() > mysql.MaxFloatingTypeWidth || tp.GetFlen() == 0 {
				return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, mysql.MaxFloatingTypeWidth)
			}
			if tp.GetFlen() < tp.GetDecimal() {
				return types.ErrMBiggerThanD.GenWithStackByArgs(colDef.Name.Name.O)
			}
		}
	case mysql.TypeSet:
		if len(tp.GetElems()) > mysql.MaxTypeSetMembers {
			return types.ErrTooBigSet.GenWithStack("Too many strings for column %s and SET", colDef.Name.Name.O)
		}
		// Check set elements. See https://dev.mysql.com/doc/refman/5.7/en/set.html.
		for _, str := range colDef.Tp.GetElems() {
			if strings.Contains(str, ",") {
				return types.ErrIllegalValueForType.GenWithStackByArgs(types.TypeStr(tp.GetType()), str)
			}
		}
	case mysql.TypeNewDecimal:
		tpFlen := tp.GetFlen()
		tpDecimal := tp.GetDecimal()
		if tpDecimal > mysql.MaxDecimalScale {
			return types.ErrTooBigScale.GenWithStackByArgs(tpDecimal, colDef.Name.Name.O, mysql.MaxDecimalScale)
		}
		if tpFlen > mysql.MaxDecimalWidth {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tpFlen, colDef.Name.Name.O, mysql.MaxDecimalWidth)
		}
		if tpFlen < tpDecimal {
			return types.ErrMBiggerThanD.GenWithStackByArgs(colDef.Name.Name.O)
		}
		// If decimal and flen all equals 0, just set flen to default value.
		if tpFlen == 0 && (tpDecimal == 0 || tpDecimal == types.UnspecifiedLength) {
			defaultFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNewDecimal)
			tp.SetFlen(defaultFlen)
			tp.SetDecimal(0)
		}
	case mysql.TypeBit:
		if tp.GetFlen() <= 0 {
			return types.ErrInvalidFieldSize.GenWithStackByArgs(colDef.Name.Name.O)
		}
		if tp.GetFlen() > mysql.MaxBitDisplayWidth {
			return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, mysql.MaxBitDisplayWidth)
		}
	case mysql.TypeTiDBVectorFloat32:
		if tp.GetFlen() != types.UnspecifiedLength {
			if err := types.CheckVectorDimValid(tp.GetFlen()); err != nil {
				return err
			}
		}
	default:
		// TODO: Add more types.
	}
	return nil
}

// isDefaultValNowSymFunc checks whether default value is a NOW() builtin function.
func isDefaultValNowSymFunc(expr ast.ExprNode) bool {
	if funcCall, ok := expr.(*ast.FuncCallExpr); ok {
		// Default value NOW() is transformed to CURRENT_TIMESTAMP() in parser.
		if funcCall.FnName.L == ast.CurrentTimestamp {
			return true
		}
	}
	return false
}

func isInvalidDefaultValue(colDef *ast.ColumnDef) bool {
	tp := colDef.Tp
	// Check the last default value.
	for i := len(colDef.Options) - 1; i >= 0; i-- {
		columnOpt := colDef.Options[i]
		if columnOpt.Tp == ast.ColumnOptionDefaultValue {
			if !(tp.GetType() == mysql.TypeTimestamp || tp.GetType() == mysql.TypeDatetime) && isDefaultValNowSymFunc(columnOpt.Expr) {
				return true
			}
			break
		}
	}

	return false
}

