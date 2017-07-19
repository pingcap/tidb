// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2016 PingCAP, Inc.
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

package ddl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/types"
)

func (d *ddl) CreateSchema(ctx context.Context, schema model.CIStr, charsetInfo *ast.CharsetOpt) (err error) {
	is := d.GetInformationSchema()
	_, ok := is.SchemaByName(schema)
	if ok {
		return infoschema.ErrDatabaseExists.GenByArgs(schema)
	}

	if err = checkTooLongSchema(schema); err != nil {
		return errors.Trace(err)
	}

	schemaID, err := d.genGlobalID()
	if err != nil {
		return errors.Trace(err)
	}
	dbInfo := &model.DBInfo{
		Name: schema,
	}
	if charsetInfo != nil {
		dbInfo.Charset = charsetInfo.Chs
		dbInfo.Collate = charsetInfo.Col
	} else {
		dbInfo.Charset, dbInfo.Collate = getDefaultCharsetAndCollate()
	}

	job := &model.Job{
		SchemaID:   schemaID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropSchema(ctx context.Context, schema model.CIStr) (err error) {
	is := d.GetInformationSchema()
	old, ok := is.SchemaByName(schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}

	job := &model.Job{
		SchemaID:   old.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkTooLongSchema(schema model.CIStr) error {
	if len(schema.L) > mysql.MaxDatabaseNameLength {
		return ErrTooLongIdent.Gen("too long schema %s", schema)
	}
	return nil
}

func checkTooLongTable(table model.CIStr) error {
	if len(table.L) > mysql.MaxTableNameLength {
		return ErrTooLongIdent.Gen("too long table %s", table)
	}
	return nil
}

func getDefaultCharsetAndCollate() (string, string) {
	// TODO: TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
	// TODO: Change TableOption parser to parse collate.
	// This is a tmp solution.
	return "utf8", "utf8_bin"
}

func setColumnFlagWithConstraint(colMap map[string]*table.Column, v *ast.Constraint) {
	switch v.Tp {
	case ast.ConstraintPrimaryKey:
		for _, key := range v.Keys {
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			c.Flag |= mysql.PriKeyFlag
			// Primary key can not be NULL.
			c.Flag |= mysql.NotNullFlag
		}
	case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		for i, key := range v.Keys {
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set
				// if unique index has multi columns,
				// the flag should be MultipleKeyFlag.
				// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
				if len(v.Keys) > 1 {
					c.Flag |= mysql.MultipleKeyFlag
				} else {
					c.Flag |= mysql.UniqueKeyFlag
				}
			}
		}
	case ast.ConstraintKey, ast.ConstraintIndex:
		for i, key := range v.Keys {
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set.
				c.Flag |= mysql.MultipleKeyFlag
			}
		}
	}
}

func buildColumnsAndConstraints(ctx context.Context, colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint) ([]*table.Column, []*ast.Constraint, error) {
	var cols []*table.Column
	colMap := map[string]*table.Column{}
	for i, colDef := range colDefs {
		col, cts, err := buildColumnAndConstraint(ctx, i, colDef)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		col.State = model.StatePublic
		constraints = append(constraints, cts...)
		cols = append(cols, col)
		colMap[colDef.Name.Name.L] = col
	}
	// Traverse table Constraints and set col.flag.
	for _, v := range constraints {
		setColumnFlagWithConstraint(colMap, v)
	}
	return cols, constraints, nil
}

func setCharsetCollationFlenDecimal(tp *types.FieldType) error {
	tp.Charset = strings.ToLower(tp.Charset)
	tp.Collate = strings.ToLower(tp.Collate)
	if len(tp.Charset) == 0 {
		switch tp.Tp {
		case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeEnum, mysql.TypeSet:
			tp.Charset, tp.Collate = getDefaultCharsetAndCollate()
		default:
			tp.Charset = charset.CharsetBin
			tp.Collate = charset.CharsetBin
		}
	} else {
		if !charset.ValidCharsetAndCollation(tp.Charset, tp.Collate) {
			return errUnsupportedCharset.GenByArgs(tp.Charset, tp.Collate)
		}
		if len(tp.Collate) == 0 {
			var err error
			tp.Collate, err = charset.GetDefaultCollation(tp.Charset)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	// If flen is not assigned, assigned it by type.
	if tp.Flen == types.UnspecifiedLength {
		tp.Flen = mysql.GetDefaultFieldLength(tp.Tp)
	}
	if tp.Decimal == types.UnspecifiedLength {
		tp.Decimal = mysql.GetDefaultDecimal(tp.Tp)
	}
	return nil
}

func buildColumnAndConstraint(ctx context.Context, offset int,
	colDef *ast.ColumnDef) (*table.Column, []*ast.Constraint, error) {
	err := setCharsetCollationFlenDecimal(colDef.Tp)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	col, cts, err := columnDefToCol(ctx, offset, colDef)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, cts, nil
}

// checkColumnCantHaveDefaultValue checks the column can have value as default or not.
// Now, TEXT/BLOB/JSON can't have not null value as default.
func checkColumnCantHaveDefaultValue(col *table.Column, value interface{}) (err error) {
	if value != nil && (col.Tp == mysql.TypeJSON ||
		col.Tp == mysql.TypeTinyBlob || col.Tp == mysql.TypeMediumBlob ||
		col.Tp == mysql.TypeLongBlob || col.Tp == mysql.TypeBlob) {
		// TEXT/BLOB/JSON can't have not null default values.
		return errBlobCantHaveDefault.GenByArgs(col.Name.O)
	}
	return nil
}

// columnDefToCol converts ColumnDef to Col and TableConstraints.
func columnDefToCol(ctx context.Context, offset int, colDef *ast.ColumnDef) (*table.Column, []*ast.Constraint, error) {
	constraints := []*ast.Constraint{}
	col := table.ToColumn(&model.ColumnInfo{
		Offset:    offset,
		Name:      colDef.Name.Name,
		FieldType: *colDef.Tp,
	})

	// Check and set TimestampFlag and OnUpdateNowFlag.
	if col.Tp == mysql.TypeTimestamp {
		col.Flag |= mysql.TimestampFlag
		col.Flag |= mysql.OnUpdateNowFlag
		col.Flag |= mysql.NotNullFlag
	}

	setOnUpdateNow := false
	hasDefaultValue := false
	if colDef.Options != nil {
		len := types.UnspecifiedLength

		if types.IsTypePrefixable(colDef.Tp.Tp) {
			len = colDef.Tp.Flen
		}

		keys := []*ast.IndexColName{
			{
				Column: colDef.Name,
				Length: len,
			},
		}

		for _, v := range colDef.Options {
			switch v.Tp {
			case ast.ColumnOptionNotNull:
				col.Flag |= mysql.NotNullFlag
			case ast.ColumnOptionNull:
				col.Flag &= ^uint(mysql.NotNullFlag)
				removeOnUpdateNowFlag(col)
			case ast.ColumnOptionAutoIncrement:
				col.Flag |= mysql.AutoIncrementFlag
			case ast.ColumnOptionPrimaryKey:
				constraint := &ast.Constraint{Tp: ast.ConstraintPrimaryKey, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.PriKeyFlag
			case ast.ColumnOptionUniqKey:
				constraint := &ast.Constraint{Tp: ast.ConstraintUniqKey, Name: colDef.Name.Name.O, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ast.ColumnOptionDefaultValue:
				value, err := getDefaultValue(ctx, v, colDef.Tp.Tp, colDef.Tp.Decimal)
				if err != nil {
					return nil, nil, ErrColumnBadNull.Gen("invalid default value - %s", err)
				}
				if err = checkColumnCantHaveDefaultValue(col, value); err != nil {
					return nil, nil, errors.Trace(err)
				}
				col.DefaultValue = value
				hasDefaultValue = true
				removeOnUpdateNowFlag(col)
			case ast.ColumnOptionOnUpdate:
				// TODO: Support other time functions.
				if !expression.IsCurrentTimeExpr(v.Expr) {
					return nil, nil, ErrInvalidOnUpdate.Gen("invalid ON UPDATE for - %s", col.Name)
				}
				col.Flag |= mysql.OnUpdateNowFlag
				setOnUpdateNow = true
			case ast.ColumnOptionComment:
				err := setColumnComment(ctx, col, v)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
			case ast.ColumnOptionGenerated:
				col.GeneratedExprString = stringutil.RemoveBlanks(v.Expr.Text())
				col.GeneratedStored = v.Stored
				_, dependColNames := findDependedColumnNames(colDef)
				col.Dependences = dependColNames
			case ast.ColumnOptionFulltext:
				// TODO: Support this type.
			}
		}
	}

	setTimestampDefaultValue(col, hasDefaultValue, setOnUpdateNow)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(col, hasDefaultValue)

	if col.Charset == charset.CharsetBin {
		col.Flag |= mysql.BinaryFlag
	}
	err := checkDefaultValue(ctx, col, hasDefaultValue)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, constraints, nil
}

func getDefaultValue(ctx context.Context, c *ast.ColumnOption, tp byte, fsp int) (interface{}, error) {
	if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime {
		vd, err := expression.GetTimeValue(ctx, c.Expr, tp, fsp)
		value := vd.GetValue()
		if err != nil {
			return nil, errors.Trace(err)
		}

		// Value is nil means `default null`.
		if value == nil {
			return nil, nil
		}

		// If value is types.Time, convert it to string.
		if vv, ok := value.(types.Time); ok {
			return vv.String(), nil
		}

		return value, nil
	}
	v, err := expression.EvalAstExpr(c.Expr, ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if v.IsNull() {
		return nil, nil
	}

	if v.Kind() == types.KindMysqlHex {
		return strconv.FormatInt(v.GetMysqlHex().Value, 10), nil
	}

	return v.ToString()
}

func removeOnUpdateNowFlag(c *table.Column) {
	// For timestamp Col, if it is set null or default value,
	// OnUpdateNowFlag should be removed.
	if mysql.HasTimestampFlag(c.Flag) {
		c.Flag &= ^uint(mysql.OnUpdateNowFlag)
	}
}

func setTimestampDefaultValue(c *table.Column, hasDefaultValue bool, setOnUpdateNow bool) {
	if hasDefaultValue {
		return
	}

	// For timestamp Col, if is not set default value or not set null, use current timestamp.
	if mysql.HasTimestampFlag(c.Flag) && mysql.HasNotNullFlag(c.Flag) {
		if setOnUpdateNow {
			c.DefaultValue = expression.ZeroTimestamp
		} else {
			c.DefaultValue = expression.CurrentTimestamp
		}
	}
}

func setNoDefaultValueFlag(c *table.Column, hasDefaultValue bool) {
	if hasDefaultValue {
		return
	}

	if !mysql.HasNotNullFlag(c.Flag) {
		return
	}

	// Check if it is an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	if !mysql.HasAutoIncrementFlag(c.Flag) && !mysql.HasTimestampFlag(c.Flag) {
		c.Flag |= mysql.NoDefaultValueFlag
	}
}

func checkDefaultValue(ctx context.Context, c *table.Column, hasDefaultValue bool) error {
	if !hasDefaultValue {
		return nil
	}

	if c.DefaultValue != nil {
		_, err := table.GetColDefaultValue(ctx, c.ToInfo())
		if terror.ErrorEqual(err, types.ErrTruncated) {
			return errInvalidDefault.GenByArgs(c.Name)
		}
		return errors.Trace(err)
	}

	// Set not null but default null is invalid.
	if mysql.HasNotNullFlag(c.Flag) {
		return errInvalidDefault.GenByArgs(c.Name)
	}

	return nil
}

func checkDuplicateColumn(colDefs []*ast.ColumnDef) error {
	colNames := map[string]bool{}
	for _, colDef := range colDefs {
		nameLower := colDef.Name.Name.L
		if colNames[nameLower] {
			return infoschema.ErrColumnExists.GenByArgs(colDef.Name.Name)
		}
		colNames[nameLower] = true
	}
	return nil
}

func checkGeneratedColumn(colDefs []*ast.ColumnDef) error {
	var colName2Generation = make(map[string]columnGenerationInDDL, len(colDefs))
	for i, colDef := range colDefs {
		generated, depCols := findDependedColumnNames(colDef)
		if !generated {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:  i,
				generated: false,
			}
		} else {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   true,
				dependences: depCols,
			}
		}
	}
	for _, colDef := range colDefs {
		colName := colDef.Name.Name.L
		if err := verifyColumnGeneration(colName2Generation, colName); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func checkTooLongColumn(colDefs []*ast.ColumnDef) error {
	for _, colDef := range colDefs {
		if len(colDef.Name.Name.O) > mysql.MaxColumnNameLength {
			return ErrTooLongIdent.Gen("too long column %s", colDef.Name.Name)
		}
	}
	return nil
}

func checkDuplicateConstraint(namesMap map[string]bool, name string, foreign bool) error {
	if name == "" {
		return nil
	}
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		if foreign {
			return infoschema.ErrCannotAddForeign
		}
		return errDupKeyName.Gen("duplicate key name %s", name)
	}
	namesMap[nameLower] = true
	return nil
}

func setEmptyConstraintName(namesMap map[string]bool, constr *ast.Constraint, foreign bool) {
	if constr.Name == "" && len(constr.Keys) > 0 {
		colName := constr.Keys[0].Column.Name.L
		constrName := colName
		i := 2
		for namesMap[constrName] {
			// We loop forever until we find constrName that haven't been used.
			if foreign {
				constrName = fmt.Sprintf("fk_%s_%d", colName, i)
			} else {
				constrName = fmt.Sprintf("%s_%d", colName, i)
			}
			i++
		}
		constr.Name = constrName
		namesMap[constrName] = true
	}
}

func checkConstraintNames(constraints []*ast.Constraint) error {
	constrNames := map[string]bool{}
	fkNames := map[string]bool{}

	// Check not empty constraint name whether is duplicated.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			err := checkDuplicateConstraint(fkNames, constr.Name, true)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			err := checkDuplicateConstraint(constrNames, constr.Name, false)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// Set empty constraint names.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			setEmptyConstraintName(fkNames, constr, true)
		} else {
			setEmptyConstraintName(constrNames, constr, false)
		}
	}

	return nil
}

func (d *ddl) buildTableInfo(tableName model.CIStr, cols []*table.Column, constraints []*ast.Constraint) (tbInfo *model.TableInfo, err error) {
	tbInfo = &model.TableInfo{
		Name: tableName,
	}
	tbInfo.ID, err = d.genGlobalID()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, v := range cols {
		v.ID = allocateColumnID(tbInfo)
		tbInfo.Columns = append(tbInfo.Columns, v.ToInfo())
	}
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			for _, fk := range tbInfo.ForeignKeys {
				if fk.Name.L == strings.ToLower(constr.Name) {
					return nil, infoschema.ErrCannotAddForeign
				}
			}
			var fk model.FKInfo
			fk.Name = model.NewCIStr(constr.Name)
			fk.RefTable = constr.Refer.Table.Name
			fk.State = model.StatePublic
			for _, key := range constr.Keys {
				fk.Cols = append(fk.Cols, key.Column.Name)
			}
			for _, key := range constr.Refer.IndexColNames {
				fk.RefCols = append(fk.RefCols, key.Column.Name)
			}
			fk.OnDelete = int(constr.Refer.OnDelete.ReferOpt)
			fk.OnUpdate = int(constr.Refer.OnUpdate.ReferOpt)
			if len(fk.Cols) != len(fk.RefCols) {
				return nil, infoschema.ErrForeignKeyNotMatch
			}
			if len(fk.Cols) == 0 {
				// TODO: In MySQL, this case will report a parse error.
				return nil, infoschema.ErrCannotAddForeign
			}
			tbInfo.ForeignKeys = append(tbInfo.ForeignKeys, &fk)
			continue
		}
		if constr.Tp == ast.ConstraintPrimaryKey {
			for _, key := range constr.Keys {
				col := table.FindCol(cols, key.Column.Name.O)
				if col == nil {
					return nil, errKeyColumnDoesNotExits.Gen("key column %s doesn't exist in table", key.Column.Name)
				}
				// Virtual columns cannot be used in primary key.
				if len(col.GeneratedExprString) != 0 && !col.GeneratedStored {
					return nil, errUnsupportedOnGeneratedColumn.GenByArgs("Defining a virtual generated column as primary key")
				}
			}
			if len(constr.Keys) == 1 {
				key := constr.Keys[0]
				col := table.FindCol(cols, key.Column.Name.O)
				if col == nil {
					return nil, errKeyColumnDoesNotExits.Gen("key column %s doesn't exist in table", key.Column.Name)
				}
				switch col.Tp {
				case mysql.TypeLong, mysql.TypeLonglong,
					mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
					tbInfo.PKIsHandle = true
					// Avoid creating index for PK handle column.
					continue
				}
			}
		}
		// build index info.
		idxInfo, err := buildIndexInfo(tbInfo, model.NewCIStr(constr.Name), constr.Keys, model.StatePublic)
		if err != nil {
			return nil, errors.Trace(err)
		}
		//check if the index is primary or uniqiue.
		switch constr.Tp {
		case ast.ConstraintPrimaryKey:
			idxInfo.Primary = true
			idxInfo.Unique = true
			idxInfo.Name = model.NewCIStr(table.PrimaryKeyName)
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			idxInfo.Unique = true
		}
		// set index type.
		if constr.Option != nil {
			idxInfo.Comment = constr.Option.Comment
			idxInfo.Tp = constr.Option.Tp
		} else {
			// Use btree as default index type.
			idxInfo.Tp = model.IndexTypeBtree
		}
		idxInfo.ID = allocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}
	return
}

func (d *ddl) CreateTableWithLike(ctx context.Context, ident, referIdent ast.Ident) error {
	is := d.GetInformationSchema()
	_, ok := is.SchemaByName(referIdent.Schema)
	if !ok {
		return infoschema.ErrTableNotExists.GenByArgs(referIdent.Schema, referIdent.Name)
	}
	referTbl, err := is.TableByName(referIdent.Schema, referIdent.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenByArgs(referIdent.Schema, referIdent.Name)
	}
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ident.Schema)
	}
	if is.TableExists(ident.Schema, ident.Name) {
		return infoschema.ErrTableExists.GenByArgs(ident)
	}

	tblInfo := *referTbl.Meta()
	tblInfo.Name = ident.Name
	tblInfo.AutoIncID = 0
	tblInfo.ForeignKeys = nil
	tblInfo.ID, err = d.genGlobalID()
	if err != nil {
		return errors.Trace(err)
	}
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) CreateTable(ctx context.Context, ident ast.Ident, colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint, options []*ast.TableOption) (err error) {
	is := d.GetInformationSchema()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ident.Schema)
	}
	if is.TableExists(ident.Schema, ident.Name) {
		return infoschema.ErrTableExists.GenByArgs(ident)
	}
	if err = checkTooLongTable(ident.Name); err != nil {
		return errors.Trace(err)
	}
	if err = checkDuplicateColumn(colDefs); err != nil {
		return errors.Trace(err)
	}
	if err = checkGeneratedColumn(colDefs); err != nil {
		return errors.Trace(err)
	}
	if err = checkTooLongColumn(colDefs); err != nil {
		return errors.Trace(err)
	}

	cols, newConstraints, err := buildColumnsAndConstraints(ctx, colDefs, constraints)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkConstraintNames(newConstraints)
	if err != nil {
		return errors.Trace(err)
	}

	tbInfo, err := d.buildTableInfo(ident.Name, cols, newConstraints)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo},
	}

	handleTableOptions(options, tbInfo)
	err = d.doDDLJob(ctx, job)
	if err == nil {
		if tbInfo.AutoIncID > 1 {
			// Default tableAutoIncID base is 0.
			// If the first id is expected to greater than 1, we need to do rebase.
			d.handleAutoIncID(tbInfo, schema.ID)
		}
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// handleAutoIncID handles auto_increment option in DDL. It creates a ID counter for the table and initiates the counter to a proper value.
// For example if the option sets auto_increment to 10. The counter will be set to 9. So the next allocated ID will be 10.
func (d *ddl) handleAutoIncID(tbInfo *model.TableInfo, schemaID int64) error {
	if tbInfo.OldSchemaID != 0 {
		schemaID = tbInfo.OldSchemaID
	}
	alloc := autoid.NewAllocator(d.store, schemaID)

	tbInfo.State = model.StatePublic
	tb, err := table.TableFromMeta(alloc, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}
	// The operation of the minus 1 to make sure that the current value doesn't be used,
	// the next Alloc operation will get this value.
	// Its behavior is consistent with MySQL.
	if err = tb.RebaseAutoID(tbInfo.AutoIncID-1, false); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// handleTableOptions updates tableInfo according to table options.
func handleTableOptions(options []*ast.TableOption, tbInfo *model.TableInfo) {
	for _, op := range options {
		switch op.Tp {
		case ast.TableOptionAutoIncrement:
			tbInfo.AutoIncID = int64(op.UintValue)
		case ast.TableOptionComment:
			tbInfo.Comment = op.StrValue
		case ast.TableOptionCharset:
			tbInfo.Charset = op.StrValue
		case ast.TableOptionCollate:
			tbInfo.Collate = op.StrValue
		}
	}
}

func (d *ddl) AlterTable(ctx context.Context, ident ast.Ident, specs []*ast.AlterTableSpec) (err error) {
	// Only handle valid specs, AlterTableLock is ignored.
	validSpecs := make([]*ast.AlterTableSpec, 0, len(specs))
	for _, spec := range specs {
		if spec.Tp == ast.AlterTableLock {
			continue
		}
		validSpecs = append(validSpecs, spec)
	}

	if len(validSpecs) != 1 {
		// TODO: Hanlde len(validSpecs) == 0.
		// Now we only allow one schema changing at the same time.
		return errRunMultiSchemaChanges
	}

	for _, spec := range validSpecs {
		switch spec.Tp {
		case ast.AlterTableAddColumn:
			err = d.AddColumn(ctx, ident, spec)
		case ast.AlterTableDropColumn:
			err = d.DropColumn(ctx, ident, spec.OldColumnName.Name)
		case ast.AlterTableDropIndex:
			err = d.DropIndex(ctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableAddConstraint:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				err = d.CreateIndex(ctx, ident, false, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				err = d.CreateIndex(ctx, ident, true, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintForeignKey:
				err = d.CreateForeignKey(ctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, spec.Constraint.Refer)
			case ast.ConstraintPrimaryKey:
				err = ErrUnsupportedModifyPrimaryKey.GenByArgs("add")
			default:
				// Nothing to do now.
			}
		case ast.AlterTableDropForeignKey:
			err = d.DropForeignKey(ctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableModifyColumn:
			err = d.ModifyColumn(ctx, ident, spec)
		case ast.AlterTableChangeColumn:
			err = d.ChangeColumn(ctx, ident, spec)
		case ast.AlterTableAlterColumn:
			err = d.AlterColumn(ctx, ident, spec)
		case ast.AlterTableRenameTable:
			newIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
			err = d.RenameTable(ctx, ident, newIdent)
		case ast.AlterTableDropPrimaryKey:
			err = ErrUnsupportedModifyPrimaryKey.GenByArgs("drop")
		default:
			// Nothing to do now.
		}

		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func checkColumnConstraint(constraints []*ast.ColumnOption) error {
	for _, constraint := range constraints {
		switch constraint.Tp {
		case ast.ColumnOptionAutoIncrement, ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
			return errUnsupportedAddColumn.Gen("unsupported add column constraint - %v", constraint.Tp)
		}
	}

	return nil
}

// AddColumn will add a new column to the table.
func (d *ddl) AddColumn(ctx context.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	// Check whether the added column constraints are supported.
	err := checkColumnConstraint(spec.NewColumn.Options)
	if err != nil {
		return errors.Trace(err)
	}

	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	// Check whether added column has existed.
	colName := spec.NewColumn.Name.Name.O
	col := table.FindCol(t.Cols(), colName)
	if col != nil {
		return infoschema.ErrColumnExists.GenByArgs(colName)
	}

	// If new column is a generated column, do validation.
	// NOTE: Because now we can only append columns to table,
	// we dont't need check whether the column refers other
	// generated columns occurring later in table.
	for _, option := range spec.NewColumn.Options {
		if option.Tp == ast.ColumnOptionGenerated {
			referableColNames := make(map[string]struct{}, len(t.Cols()))
			for _, col := range t.Cols() {
				referableColNames[col.Name.L] = struct{}{}
			}
			_, dependColNames := findDependedColumnNames(spec.NewColumn)
			if err := columnNamesCover(referableColNames, dependColNames); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if len(colName) > mysql.MaxColumnNameLength {
		return ErrTooLongIdent.Gen("too long column %s", colName)
	}

	// Ingore table constraints now, maybe return error later.
	// We use length(t.Cols()) as the default offset firstly, we will change the
	// column's offset later.
	col, _, err = buildColumnAndConstraint(ctx, len(t.Cols()), spec.NewColumn)
	if err != nil {
		return errors.Trace(err)
	}
	col.OriginDefaultValue = col.DefaultValue
	if col.OriginDefaultValue == nil && mysql.HasNotNullFlag(col.Flag) {
		zeroVal := table.GetZeroValue(col.ToInfo())
		col.OriginDefaultValue, err = zeroVal.ToString()
		if err != nil {
			return errors.Trace(err)
		}
	}

	if col.OriginDefaultValue == expression.CurrentTimestamp &&
		(col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime) {
		col.OriginDefaultValue = time.Now().Format(types.TimeFormat)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{col, spec.Position, 0},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropColumn will drop a column from the table, now we don't support drop the column with index covered.
func (d *ddl) DropColumn(ctx context.Context, ti ast.Ident, colName model.CIStr) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	// Check whether dropped column has existed.
	col := table.FindCol(t.Cols(), colName.L)
	if col == nil {
		return ErrCantDropFieldOrKey.Gen("column %s doesn't exist", colName)
	}

	// Check whether there are other columns depend on this column or not.
	for _, col := range t.Cols() {
		for dep := range col.Dependences {
			if dep == colName.L {
				return errDependentByGeneratedColumn.GenByArgs(dep)
			}
		}
	}

	tblInfo := t.Meta()
	// We don't support dropping column with index covered now.
	// We must drop the index first, then drop the column.
	if isColumnWithIndex(colName.L, tblInfo.Indices) {
		return errCantDropColWithIndex.Gen("can't drop column %s with index covered now", colName)
	}
	// We don't support dropping column with PK handle covered now.
	if col.IsPKHandleColumn(tblInfo) {
		return errUnsupportedPKHandle
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{colName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// modifiable checks if the 'origin' type can be modified to 'to' type with out the need to
// change or check existing data in the table.
// It returns true if the two types has the same Charset and Collation, the same sign, both are
// integer types or string types, and new Flen and Decimal must be greater than or equal to origin.
func modifiable(origin *types.FieldType, to *types.FieldType) error {
	if to.Flen > 0 && to.Flen < origin.Flen {
		msg := fmt.Sprintf("length %d is less than origin %d", to.Flen, origin.Flen)
		return errUnsupportedModifyColumn.GenByArgs(msg)
	}
	if to.Decimal > 0 && to.Decimal < origin.Decimal {
		msg := fmt.Sprintf("decimal %d is less than origin %d", to.Decimal, origin.Decimal)
		return errUnsupportedModifyColumn.GenByArgs(msg)
	}
	if to.Charset != origin.Charset {
		msg := fmt.Sprintf("charset %s not match origin %s", to.Charset, origin.Charset)
		return errUnsupportedModifyColumn.GenByArgs(msg)
	}
	if to.Collate != origin.Collate {
		msg := fmt.Sprintf("collate %s not match origin %s", to.Collate, origin.Collate)
		return errUnsupportedModifyColumn.GenByArgs(msg)
	}
	toUnsigned := mysql.HasUnsignedFlag(uint(to.Flag))
	originUnsigned := mysql.HasUnsignedFlag(uint(origin.Flag))
	if originUnsigned != toUnsigned {
		msg := fmt.Sprintf("unsigned %v not match origin %v", toUnsigned, originUnsigned)
		return errUnsupportedModifyColumn.GenByArgs(msg)
	}
	switch origin.Tp {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch to.Tp {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
			mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			return nil
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		switch to.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return nil
		}
	case mysql.TypeEnum:
		return errUnsupportedModifyColumn.GenByArgs("modify enum column is not supported")
	default:
		if origin.Tp == to.Tp {
			return nil
		}
	}
	msg := fmt.Sprintf("type %v not match origin %v", to.Tp, origin.Tp)
	return errUnsupportedModifyColumn.GenByArgs(msg)
}

func setDefaultValue(ctx context.Context, col *table.Column, option *ast.ColumnOption) error {
	value, err := getDefaultValue(ctx, option, col.Tp, col.Decimal)
	if err != nil {
		return ErrColumnBadNull.Gen("invalid default value - %s", err)
	}
	col.DefaultValue = value
	return errors.Trace(checkDefaultValue(ctx, col, true))
}

func setColumnComment(ctx context.Context, col *table.Column, option *ast.ColumnOption) error {
	value, err := expression.EvalAstExpr(option.Expr, ctx)
	if err != nil {
		return errors.Trace(err)
	}
	col.Comment, err = value.ToString()
	return errors.Trace(err)
}

// setDefaultAndComment is only used in getModifiableColumnJob.
func setDefaultAndComment(ctx context.Context, col *table.Column, options []*ast.ColumnOption) error {
	if len(options) == 0 {
		return nil
	}
	var hasDefaultValue, setOnUpdateNow bool
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionDefaultValue:
			value, err := getDefaultValue(ctx, opt, col.Tp, col.Decimal)
			if err != nil {
				return ErrColumnBadNull.Gen("invalid default value - %s", err)
			}
			if err = checkColumnCantHaveDefaultValue(col, value); err != nil {
				return errors.Trace(err)
			}
			col.DefaultValue = value
			hasDefaultValue = true
		case ast.ColumnOptionComment:
			err := setColumnComment(ctx, col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionNotNull:
			col.Flag |= mysql.NotNullFlag
		case ast.ColumnOptionNull:
			col.Flag &= ^uint(mysql.NotNullFlag)
		case ast.ColumnOptionAutoIncrement:
			col.Flag |= mysql.AutoIncrementFlag
		case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
			return errUnsupportedModifyColumn.Gen("unsupported modify column constraint - %v", opt.Tp)
		case ast.ColumnOptionOnUpdate:
			// TODO: Support other time functions.
			if !expression.IsCurrentTimeExpr(opt.Expr) {
				return ErrInvalidOnUpdate.Gen("invalid ON UPDATE for - %s", col.Name)
			}
			col.Flag |= mysql.OnUpdateNowFlag
			setOnUpdateNow = true
		case ast.ColumnOptionGenerated:
			col.GeneratedExprString = stringutil.RemoveBlanks(opt.Expr.Text())
			col.GeneratedStored = opt.Stored
			col.Dependences = make(map[string]struct{})
			for _, colName := range findColumnNamesInExpr(opt.Expr) {
				col.Dependences[colName.Name.L] = struct{}{}
			}
		default:
			// TODO: Support other types.
			return errors.Trace(errUnsupportedModifyColumn.GenByArgs(opt.Tp))
		}
	}

	setTimestampDefaultValue(col, hasDefaultValue, setOnUpdateNow)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(col, hasDefaultValue)

	if hasDefaultValue {
		return errors.Trace(checkDefaultValue(ctx, col, true))
	}

	return nil
}

func (d *ddl) getModifiableColumnJob(ctx context.Context, ident ast.Ident, originalColName model.CIStr,
	spec *ast.AlterTableSpec) (*model.Job, error) {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return nil, errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return nil, errors.Trace(infoschema.ErrTableNotExists)
	}

	col := table.FindCol(t.Cols(), originalColName.L)
	if col == nil {
		return nil, infoschema.ErrColumnNotExists.GenByArgs(originalColName, ident.Name)
	}

	// Constraints in the new column means adding new constraints. Errors should thrown,
	// which will be done by `setDefaultAndComment` later.
	if spec.NewColumn.Tp == nil {
		// Make sure the column definition is simple field type.
		return nil, errors.Trace(errUnsupportedModifyColumn)
	}

	newCol := table.ToColumn(&model.ColumnInfo{
		ID:                 col.ID,
		Offset:             col.Offset,
		State:              col.State,
		OriginDefaultValue: col.OriginDefaultValue,
		FieldType:          *spec.NewColumn.Tp,
		Name:               spec.NewColumn.Name.Name,
	})

	err = setCharsetCollationFlenDecimal(&newCol.FieldType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = modifiable(&col.FieldType, &newCol.FieldType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := setDefaultAndComment(ctx, newCol, spec.NewColumn.Options); err != nil {
		return nil, errors.Trace(err)
	}

	// Copy index related options to the new spec.
	indexFlags := col.FieldType.Flag & (mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	newCol.FieldType.Flag |= indexFlags
	if mysql.HasPriKeyFlag(col.FieldType.Flag) {
		newCol.FieldType.Flag |= mysql.NotNullFlag
		// TODO: If user explicitly set NULL, we should throw error ErrPrimaryCantHaveNull.
	}

	// We don't support modifying column from not_auto_increment to auto_increment.
	if !mysql.HasAutoIncrementFlag(col.Flag) && mysql.HasAutoIncrementFlag(newCol.Flag) {
		return nil, errUnsupportedModifyColumn.GenByArgs("set auto_increment")
	}

	// We don't support modifying the type definitions from 'null' to 'not null' now.
	if !mysql.HasNotNullFlag(col.Flag) && mysql.HasNotNullFlag(newCol.Flag) {
		return nil, errUnsupportedModifyColumn.GenByArgs("null to not null")
	}
	// As same with MySQL, we don't support modifying the stored status for generated columns.
	if err = checkModifyGeneratedColumn(t.Cols(), col, newCol); err != nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionModifyColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{&newCol, originalColName, spec.Position},
	}
	return job, nil
}

// ChangeColumn renames an existing column and modifies the column's definition,
// currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ChangeColumn(ctx context.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	if len(spec.NewColumn.Name.Schema.O) != 0 && ident.Schema.L != spec.NewColumn.Name.Schema.L {
		return errWrongDBName.GenByArgs(spec.NewColumn.Name.Schema.O)
	}
	if len(spec.OldColumnName.Schema.O) != 0 && ident.Schema.L != spec.OldColumnName.Schema.L {
		return errWrongDBName.GenByArgs(spec.OldColumnName.Schema.O)
	}
	if len(spec.NewColumn.Name.Table.O) != 0 && ident.Name.L != spec.NewColumn.Name.Table.L {
		return ErrWrongTableName.GenByArgs(spec.NewColumn.Name.Table.O)
	}
	if len(spec.OldColumnName.Table.O) != 0 && ident.Name.L != spec.OldColumnName.Table.L {
		return ErrWrongTableName.GenByArgs(spec.OldColumnName.Table.O)
	}

	job, err := d.getModifiableColumnJob(ctx, ident, spec.OldColumnName.Name, spec)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// ModifyColumn does modification on an existing column, currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ModifyColumn(ctx context.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	if len(spec.NewColumn.Name.Schema.O) != 0 && ident.Schema.L != spec.NewColumn.Name.Schema.L {
		return errWrongDBName.GenByArgs(spec.NewColumn.Name.Schema.O)
	}
	if len(spec.NewColumn.Name.Table.O) != 0 && ident.Name.L != spec.NewColumn.Name.Table.L {
		return ErrWrongTableName.GenByArgs(spec.NewColumn.Name.Table.O)
	}

	originalColName := spec.NewColumn.Name.Name
	job, err := d.getModifiableColumnJob(ctx, ident, originalColName, spec)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) AlterColumn(ctx context.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrTableNotExists.GenByArgs(ident.Schema, ident.Name)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenByArgs(ident.Schema, ident.Name)
	}

	colName := spec.NewColumn.Name.Name
	// Check whether alter column has existed.
	col := table.FindCol(t.Cols(), colName.L)
	if col == nil {
		return errBadField.GenByArgs(colName, ident.Name)
	}

	// Clean the NoDefaultValueFlag value.
	col.Flag &= (^uint(mysql.NoDefaultValueFlag))
	if len(spec.NewColumn.Options) == 0 {
		col.DefaultValue = nil
		setNoDefaultValueFlag(col, false)
	} else {
		err := setDefaultValue(ctx, col, spec.NewColumn.Options[0])
		if err != nil {
			return errors.Trace(err)
		}
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionSetDefaultValue,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{col},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropTable will proceed even if some table in the list does not exists.
func (d *ddl) DropTable(ctx context.Context, ti ast.Ident) (err error) {
	is := d.GetInformationSchema()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}

	tb, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenByArgs(ti)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) TruncateTable(ctx context.Context, ti ast.Ident) error {
	is := d.GetInformationSchema()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}
	tb, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}
	newTableID, err := d.genGlobalID()
	if err != nil {
		return errors.Trace(err)
	}
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionTruncateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newTableID},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) RenameTable(ctx context.Context, oldIdent, newIdent ast.Ident) error {
	is := d.GetInformationSchema()
	oldSchema, ok := is.SchemaByName(oldIdent.Schema)
	if !ok {
		return errFileNotFound.GenByArgs(oldIdent.Schema, oldIdent.Name)
	}
	oldTbl, err := is.TableByName(oldIdent.Schema, oldIdent.Name)
	if err != nil {
		return errFileNotFound.GenByArgs(oldIdent.Schema, oldIdent.Name)
	}
	newSchema, ok := is.SchemaByName(newIdent.Schema)
	if !ok {
		return errErrorOnRename.GenByArgs(oldIdent.Schema, oldIdent.Name, newIdent.Schema, newIdent.Name)
	}
	if is.TableExists(newIdent.Schema, newIdent.Name) {
		return infoschema.ErrTableExists.GenByArgs(newIdent)
	}

	job := &model.Job{
		SchemaID:   newSchema.ID,
		TableID:    oldTbl.Meta().ID,
		Type:       model.ActionRenameTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{oldSchema.ID, newIdent.Name},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func getAnonymousIndex(t table.Table, colName model.CIStr) model.CIStr {
	id := 2
	l := len(t.Indices())
	indexName := colName
	for i := 0; i < l; i++ {
		if t.Indices()[i].Meta().Name.L == indexName.L {
			indexName = model.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
			i = -1
			id++
		}
	}
	return indexName
}

func (d *ddl) CreateIndex(ctx context.Context, ti ast.Ident, unique bool, indexName model.CIStr,
	idxColNames []*ast.IndexColName, indexOption *ast.IndexOption) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		indexName = getAnonymousIndex(t, idxColNames[0].Column.Name)
	}

	if indexInfo := findIndexByName(indexName.L, t.Meta().Indices); indexInfo != nil {
		return errDupKeyName.Gen("index already exist %s", indexName)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{unique, indexName, idxColNames, indexOption},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func buildFKInfo(fkName model.CIStr, keys []*ast.IndexColName, refer *ast.ReferenceDef) (*model.FKInfo, error) {
	var fkInfo model.FKInfo
	fkInfo.Name = fkName
	fkInfo.RefTable = refer.Table.Name

	fkInfo.Cols = make([]model.CIStr, len(keys))
	for i, key := range keys {
		fkInfo.Cols[i] = key.Column.Name
	}

	fkInfo.RefCols = make([]model.CIStr, len(refer.IndexColNames))
	for i, key := range refer.IndexColNames {
		fkInfo.RefCols[i] = key.Column.Name
	}

	fkInfo.OnDelete = int(refer.OnDelete.ReferOpt)
	fkInfo.OnUpdate = int(refer.OnUpdate.ReferOpt)

	return &fkInfo, nil

}

func (d *ddl) CreateForeignKey(ctx context.Context, ti ast.Ident, fkName model.CIStr, keys []*ast.IndexColName, refer *ast.ReferenceDef) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	fkInfo, err := buildFKInfo(fkName, keys, refer)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)

}

func (d *ddl) DropForeignKey(ctx context.Context, ti ast.Ident, fkName model.CIStr) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropIndex(ctx context.Context, ti ast.Ident, indexName model.CIStr) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	if indexInfo := findIndexByName(indexName.L, t.Meta().Indices); indexInfo == nil {
		return ErrCantDropFieldOrKey.Gen("index %s doesn't exist", indexName)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{indexName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// findCol finds column in cols by name.
func findCol(cols []*model.ColumnInfo, name string) *model.ColumnInfo {
	name = strings.ToLower(name)
	for _, col := range cols {
		if col.Name.L == name {
			return col
		}
	}

	return nil
}
