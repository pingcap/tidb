// Copyright 2019 PingCAP, Inc.
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

package tidb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"go.uber.org/atomic"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&mysqlSuite{})

type mysqlSuite struct {
	dbHandle *sql.DB
	mockDB   sqlmock.Sqlmock
	backend  backend.Backend
	tbl      table.Table
}

func (s *mysqlSuite) SetUpTest(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	tys := []byte{
		mysql.TypeLong, mysql.TypeLong, mysql.TypeTiny, mysql.TypeInt24, mysql.TypeFloat, mysql.TypeDouble,
		mysql.TypeDouble, mysql.TypeDouble, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeBit, mysql.TypeNewDecimal, mysql.TypeEnum,
	}
	cols := make([]*model.ColumnInfo, 0, len(tys))
	for i, ty := range tys {
		col := &model.ColumnInfo{ID: int64(i + 1), Name: model.NewCIStr(fmt.Sprintf("c%d", i)), State: model.StatePublic, Offset: i, FieldType: *types.NewFieldType(ty)}
		cols = append(cols, col)
	}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	tbl, err := tables.TableFromMeta(kv.NewPanickingAllocators(0), tblInfo)
	c.Assert(err, IsNil)

	s.dbHandle = db
	s.mockDB = mock
	s.backend = tidb.NewTiDBBackend(db, config.ReplaceOnDup, errormanager.New(nil, config.NewConfig()))
	s.tbl = tbl
}

func (s *mysqlSuite) TearDownTest(c *C) {
	s.backend.Close()
	c.Assert(s.mockDB.ExpectationsWereMet(), IsNil)
}

func (s *mysqlSuite) TestWriteRowsReplaceOnDup(c *C) {
	s.mockDB.
		ExpectExec("\\QREPLACE INTO `foo`.`bar`(`b`,`d`,`e`,`f`,`g`,`h`,`i`,`j`,`k`,`l`,`m`,`n`,`o`) VALUES(-9223372036854775808,NULL,7.5,5e-324,1.7976931348623157e+308,0,'甲乙丙\\r\\n\\0\\Z''\"\\\\`',x'000000abcdef',2557891634,'12.5',51)\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	logger := log.L()

	engine, err := s.backend.OpenEngine(ctx, &backend.EngineConfig{}, "`foo`.`bar`", 1)
	c.Assert(err, IsNil)

	dataRows := s.backend.MakeEmptyRows()
	dataChecksum := verification.MakeKVChecksum(0, 0, 0)
	indexRows := s.backend.MakeEmptyRows()
	indexChecksum := verification.MakeKVChecksum(0, 0, 0)

	cols := s.tbl.Cols()
	perms := make([]int, 0, len(s.tbl.Cols())+1)
	for i := 0; i < len(cols); i++ {
		perms = append(perms, i)
	}
	perms = append(perms, -1)
	// skip column a,c due to ignore-columns
	perms[0] = -1
	perms[2] = -1
	encoder, err := s.backend.NewEncoder(s.tbl, &kv.SessionOptions{SQLMode: 0, Timestamp: 1234567890})
	c.Assert(err, IsNil)
	row, err := encoder.Encode(logger, []types.Datum{
		types.NewUintDatum(18446744073709551615),
		types.NewIntDatum(-9223372036854775808),
		types.NewUintDatum(0),
		{},
		types.NewFloat32Datum(7.5),
		types.NewFloat64Datum(5e-324),
		types.NewFloat64Datum(1.7976931348623157e+308),
		types.NewFloat64Datum(0.0),
		// In Go, the floating-point literal '-0.0' is the same as '0.0', it does not produce a negative zero.
		// types.NewFloat64Datum(-0.0),
		types.NewStringDatum("甲乙丙\r\n\x00\x1a'\"\\`"),
		types.NewBinaryLiteralDatum(types.NewBinaryLiteralFromUint(0xabcdef, 6)),
		types.NewMysqlBitDatum(types.NewBinaryLiteralFromUint(0x98765432, 4)),
		types.NewDecimalDatum(types.NewDecFromFloatForTest(12.5)),
		types.NewMysqlEnumDatum(types.Enum{Name: "ENUM_NAME", Value: 51}),
	}, 1, perms, "0.csv", 0)
	c.Assert(err, IsNil)
	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	writer, err := engine.LocalWriter(ctx, nil)
	c.Assert(err, IsNil)
	err = writer.WriteRows(ctx, []string{"b", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"}, dataRows)
	c.Assert(err, IsNil)
	st, err := writer.Close(ctx)
	c.Assert(err, IsNil)
	c.Assert(st, IsNil)
}

func (s *mysqlSuite) TestWriteRowsIgnoreOnDup(c *C) {
	s.mockDB.
		ExpectExec("\\QINSERT IGNORE INTO `foo`.`bar`(`a`) VALUES(1)\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	logger := log.L()

	ignoreBackend := tidb.NewTiDBBackend(s.dbHandle, config.IgnoreOnDup, errormanager.New(nil, config.NewConfig()))
	engine, err := ignoreBackend.OpenEngine(ctx, &backend.EngineConfig{}, "`foo`.`bar`", 1)
	c.Assert(err, IsNil)

	dataRows := ignoreBackend.MakeEmptyRows()
	dataChecksum := verification.MakeKVChecksum(0, 0, 0)
	indexRows := ignoreBackend.MakeEmptyRows()
	indexChecksum := verification.MakeKVChecksum(0, 0, 0)

	encoder, err := ignoreBackend.NewEncoder(s.tbl, &kv.SessionOptions{})
	c.Assert(err, IsNil)
	row, err := encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(1),
	}, 1, []int{0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, "1.csv", 0)
	c.Assert(err, IsNil)
	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	writer, err := engine.LocalWriter(ctx, nil)
	c.Assert(err, IsNil)
	err = writer.WriteRows(ctx, []string{"a"}, dataRows)
	c.Assert(err, IsNil)
	_, err = writer.Close(ctx)
	c.Assert(err, IsNil)

	// test encode rows with _tidb_rowid
	encoder, err = ignoreBackend.NewEncoder(s.tbl, &kv.SessionOptions{})
	c.Assert(err, IsNil)
	rowWithID, err := encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(1), // _tidb_rowid field
	}, 1, []int{0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1}, "2.csv", 0)
	c.Assert(err, IsNil)
	// tidbRow is stringer.
	c.Assert(fmt.Sprint(rowWithID), Equals, "(1,1)")
}

func (s *mysqlSuite) TestWriteRowsErrorOnDup(c *C) {
	s.mockDB.
		ExpectExec("\\QINSERT INTO `foo`.`bar`(`a`) VALUES(1)\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	logger := log.L()

	ignoreBackend := tidb.NewTiDBBackend(s.dbHandle, config.ErrorOnDup, errormanager.New(nil, config.NewConfig()))
	engine, err := ignoreBackend.OpenEngine(ctx, &backend.EngineConfig{}, "`foo`.`bar`", 1)
	c.Assert(err, IsNil)

	dataRows := ignoreBackend.MakeEmptyRows()
	dataChecksum := verification.MakeKVChecksum(0, 0, 0)
	indexRows := ignoreBackend.MakeEmptyRows()
	indexChecksum := verification.MakeKVChecksum(0, 0, 0)

	encoder, err := ignoreBackend.NewEncoder(s.tbl, &kv.SessionOptions{})
	c.Assert(err, IsNil)
	row, err := encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(1),
	}, 1, []int{0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, "3.csv", 0)
	c.Assert(err, IsNil)

	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	writer, err := engine.LocalWriter(ctx, nil)
	c.Assert(err, IsNil)
	err = writer.WriteRows(ctx, []string{"a"}, dataRows)
	c.Assert(err, IsNil)
	st, err := writer.Close(ctx)
	c.Assert(err, IsNil)
	c.Assert(st, IsNil)
}

// TODO: temporarily disable this test before we fix strict mode
//nolint:unused
func (s *mysqlSuite) testStrictMode(c *C) {
	ft := *types.NewFieldType(mysql.TypeVarchar)
	ft.Charset = charset.CharsetUTF8MB4
	col0 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("s0"), State: model.StatePublic, Offset: 0, FieldType: ft}
	ft = *types.NewFieldType(mysql.TypeString)
	ft.Charset = charset.CharsetASCII
	col1 := &model.ColumnInfo{ID: 2, Name: model.NewCIStr("s1"), State: model.StatePublic, Offset: 1, FieldType: ft}
	tblInfo := &model.TableInfo{ID: 1, Columns: []*model.ColumnInfo{col0, col1}, PKIsHandle: false, State: model.StatePublic}
	tbl, err := tables.TableFromMeta(kv.NewPanickingAllocators(0), tblInfo)
	c.Assert(err, IsNil)

	bk := tidb.NewTiDBBackend(s.dbHandle, config.ErrorOnDup, errormanager.New(nil, config.NewConfig()))
	encoder, err := bk.NewEncoder(tbl, &kv.SessionOptions{SQLMode: mysql.ModeStrictAllTables})
	c.Assert(err, IsNil)

	logger := log.L()
	_, err = encoder.Encode(logger, []types.Datum{
		types.NewStringDatum("test"),
	}, 1, []int{0, -1, -1}, "4.csv", 0)
	c.Assert(err, IsNil)

	_, err = encoder.Encode(logger, []types.Datum{
		types.NewStringDatum("\xff\xff\xff\xff"),
	}, 1, []int{0, -1, -1}, "5.csv", 0)
	c.Assert(err, ErrorMatches, `.*incorrect utf8 value .* for column s0`)

	// oepn a new encode because column count changed.
	encoder, err = bk.NewEncoder(tbl, &kv.SessionOptions{SQLMode: mysql.ModeStrictAllTables})
	c.Assert(err, IsNil)
	_, err = encoder.Encode(logger, []types.Datum{
		types.NewStringDatum(""),
		types.NewStringDatum("非 ASCII 字符串"),
	}, 1, []int{0, 1, -1}, "6.csv", 0)
	c.Assert(err, ErrorMatches, ".*incorrect ascii value .* for column s1")
}

func (s *mysqlSuite) TestFetchRemoteTableModels_3_x(c *C) {
	s.mockDB.ExpectBegin()
	s.mockDB.ExpectQuery("SELECT version()").
		WillReturnRows(sqlmock.NewRows([]string{"version()"}).AddRow("5.7.25-TiDB-v3.0.18"))
	s.mockDB.ExpectQuery("\\QSELECT table_name, column_name, column_type, generation_expression, extra FROM information_schema.columns WHERE table_schema = ? ORDER BY table_name, ordinal_position;\\E").
		WithArgs("test").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "column_type", "generation_expression", "extra"}).
			AddRow("t", "id", "int(10)", "", "auto_increment"))
	s.mockDB.ExpectCommit()

	bk := tidb.NewTiDBBackend(s.dbHandle, config.ErrorOnDup, errormanager.New(nil, config.NewConfig()))
	tableInfos, err := bk.FetchRemoteTableModels(context.Background(), "test")
	c.Assert(err, IsNil)
	c.Assert(tableInfos, DeepEquals, []*model.TableInfo{
		{
			Name:       model.NewCIStr("t"),
			State:      model.StatePublic,
			PKIsHandle: true,
			Columns: []*model.ColumnInfo{
				{
					Name:   model.NewCIStr("id"),
					Offset: 0,
					State:  model.StatePublic,
					FieldType: types.FieldType{
						Flag: mysql.AutoIncrementFlag,
					},
				},
			},
		},
	})
}

func (s *mysqlSuite) TestFetchRemoteTableModels_4_0(c *C) {
	s.mockDB.ExpectBegin()
	s.mockDB.ExpectQuery("SELECT version()").
		WillReturnRows(sqlmock.NewRows([]string{"version()"}).AddRow("5.7.25-TiDB-v4.0.0"))
	s.mockDB.ExpectQuery("\\QSELECT table_name, column_name, column_type, generation_expression, extra FROM information_schema.columns WHERE table_schema = ? ORDER BY table_name, ordinal_position;\\E").
		WithArgs("test").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "column_type", "generation_expression", "extra"}).
			AddRow("t", "id", "bigint(20) unsigned", "", "auto_increment"))
	s.mockDB.ExpectQuery("SHOW TABLE `test`.`t` NEXT_ROW_ID").
		WillReturnRows(sqlmock.NewRows([]string{"DB_NAME", "TABLE_NAME", "COLUMN_NAME", "NEXT_GLOBAL_ROW_ID"}).
			AddRow("test", "t", "id", int64(1)))
	s.mockDB.ExpectCommit()

	bk := tidb.NewTiDBBackend(s.dbHandle, config.ErrorOnDup, errormanager.New(nil, config.NewConfig()))
	tableInfos, err := bk.FetchRemoteTableModels(context.Background(), "test")
	c.Assert(err, IsNil)
	c.Assert(tableInfos, DeepEquals, []*model.TableInfo{
		{
			Name:       model.NewCIStr("t"),
			State:      model.StatePublic,
			PKIsHandle: true,
			Columns: []*model.ColumnInfo{
				{
					Name:   model.NewCIStr("id"),
					Offset: 0,
					State:  model.StatePublic,
					FieldType: types.FieldType{
						Flag: mysql.AutoIncrementFlag | mysql.UnsignedFlag,
					},
				},
			},
		},
	})
}

func (s *mysqlSuite) TestFetchRemoteTableModels_4_x_auto_increment(c *C) {
	s.mockDB.ExpectBegin()
	s.mockDB.ExpectQuery("SELECT version()").
		WillReturnRows(sqlmock.NewRows([]string{"version()"}).AddRow("5.7.25-TiDB-v4.0.7"))
	s.mockDB.ExpectQuery("\\QSELECT table_name, column_name, column_type, generation_expression, extra FROM information_schema.columns WHERE table_schema = ? ORDER BY table_name, ordinal_position;\\E").
		WithArgs("test").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "column_type", "generation_expression", "extra"}).
			AddRow("t", "id", "bigint(20)", "", ""))
	s.mockDB.ExpectQuery("SHOW TABLE `test`.`t` NEXT_ROW_ID").
		WillReturnRows(sqlmock.NewRows([]string{"DB_NAME", "TABLE_NAME", "COLUMN_NAME", "NEXT_GLOBAL_ROW_ID", "ID_TYPE"}).
			AddRow("test", "t", "id", int64(1), "AUTO_INCREMENT"))
	s.mockDB.ExpectCommit()

	bk := tidb.NewTiDBBackend(s.dbHandle, config.ErrorOnDup, errormanager.New(nil, config.NewConfig()))
	tableInfos, err := bk.FetchRemoteTableModels(context.Background(), "test")
	c.Assert(err, IsNil)
	c.Assert(tableInfos, DeepEquals, []*model.TableInfo{
		{
			Name:       model.NewCIStr("t"),
			State:      model.StatePublic,
			PKIsHandle: true,
			Columns: []*model.ColumnInfo{
				{
					Name:   model.NewCIStr("id"),
					Offset: 0,
					State:  model.StatePublic,
					FieldType: types.FieldType{
						Flag: mysql.AutoIncrementFlag,
					},
				},
			},
		},
	})
}

func (s *mysqlSuite) TestFetchRemoteTableModels_4_x_auto_random(c *C) {
	s.mockDB.ExpectBegin()
	s.mockDB.ExpectQuery("SELECT version()").
		WillReturnRows(sqlmock.NewRows([]string{"version()"}).AddRow("5.7.25-TiDB-v4.0.7"))
	s.mockDB.ExpectQuery("\\QSELECT table_name, column_name, column_type, generation_expression, extra FROM information_schema.columns WHERE table_schema = ? ORDER BY table_name, ordinal_position;\\E").
		WithArgs("test").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "column_type", "generation_expression", "extra"}).
			AddRow("t", "id", "bigint(20)", "1 + 2", ""))
	s.mockDB.ExpectQuery("SHOW TABLE `test`.`t` NEXT_ROW_ID").
		WillReturnRows(sqlmock.NewRows([]string{"DB_NAME", "TABLE_NAME", "COLUMN_NAME", "NEXT_GLOBAL_ROW_ID", "ID_TYPE"}).
			AddRow("test", "t", "id", int64(1), "AUTO_RANDOM"))
	s.mockDB.ExpectCommit()

	bk := tidb.NewTiDBBackend(s.dbHandle, config.ErrorOnDup, errormanager.New(nil, config.NewConfig()))
	tableInfos, err := bk.FetchRemoteTableModels(context.Background(), "test")
	c.Assert(err, IsNil)
	c.Assert(tableInfos, DeepEquals, []*model.TableInfo{
		{
			Name:           model.NewCIStr("t"),
			State:          model.StatePublic,
			PKIsHandle:     true,
			AutoRandomBits: 1,
			Columns: []*model.ColumnInfo{
				{
					Name:   model.NewCIStr("id"),
					Offset: 0,
					State:  model.StatePublic,
					FieldType: types.FieldType{
						Flag: mysql.PriKeyFlag,
					},
					GeneratedExprString: "1 + 2",
				},
			},
		},
	})
}

func (s *mysqlSuite) TestWriteRowsErrorDowngrading(c *C) {
	nonRetryableError := sql.ErrNoRows

	// First, batch insert, fail and rollback.
	s.mockDB.
		ExpectExec("\\QINSERT INTO `foo`.`bar`(`a`) VALUES(1),(2),(3),(4),(5)\\E").
		WillReturnError(nonRetryableError)
	// Then, insert row-by-row due to the non-retryable error.
	s.mockDB.
		ExpectExec("\\QINSERT INTO `foo`.`bar`(`a`) VALUES(1)\\E").
		WillReturnError(nonRetryableError)
	s.mockDB.
		ExpectExec("INSERT INTO `tidb_lightning_errors`\\.type_error_v1.*").
		WithArgs(sqlmock.AnyArg(), "`foo`.`bar`", "7.csv", int64(0), nonRetryableError.Error(), "(1)").
		WillReturnResult(driver.ResultNoRows)
	s.mockDB.
		ExpectExec("\\QINSERT INTO `foo`.`bar`(`a`) VALUES(2)\\E").
		WillReturnError(nonRetryableError)
	s.mockDB.
		ExpectExec("INSERT INTO `tidb_lightning_errors`\\.type_error_v1.*").
		WithArgs(sqlmock.AnyArg(), "`foo`.`bar`", "8.csv", int64(0), nonRetryableError.Error(), "(2)").
		WillReturnResult(driver.ResultNoRows)
	s.mockDB.
		ExpectExec("\\QINSERT INTO `foo`.`bar`(`a`) VALUES(3)\\E").
		WillReturnError(nonRetryableError)
	s.mockDB.
		ExpectExec("INSERT INTO `tidb_lightning_errors`\\.type_error_v1.*").
		WithArgs(sqlmock.AnyArg(), "`foo`.`bar`", "9.csv", int64(0), nonRetryableError.Error(), "(3)").
		WillReturnResult(driver.ResultNoRows)
	// the forth row will exceed the error threshold, won't record this error
	s.mockDB.
		ExpectExec("\\QINSERT INTO `foo`.`bar`(`a`) VALUES(4)\\E").
		WillReturnError(nonRetryableError)

	ctx := context.Background()
	logger := log.L()

	ignoreBackend := tidb.NewTiDBBackend(s.dbHandle, config.ErrorOnDup,
		errormanager.New(s.dbHandle, &config.Config{
			App: config.Lightning{
				TaskInfoSchemaName: "tidb_lightning_errors",
				MaxError: config.MaxError{
					Type: *atomic.NewInt64(3),
				},
			},
		}),
	)
	engine, err := ignoreBackend.OpenEngine(ctx, &backend.EngineConfig{}, "`foo`.`bar`", 1)
	c.Assert(err, IsNil)

	dataRows := ignoreBackend.MakeEmptyRows()
	dataChecksum := verification.MakeKVChecksum(0, 0, 0)
	indexRows := ignoreBackend.MakeEmptyRows()
	indexChecksum := verification.MakeKVChecksum(0, 0, 0)

	encoder, err := ignoreBackend.NewEncoder(s.tbl, &kv.SessionOptions{})
	c.Assert(err, IsNil)
	row, err := encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(1),
	}, 1, []int{0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, "7.csv", 0)
	c.Assert(err, IsNil)

	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	row, err = encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(2),
	}, 1, []int{0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, "8.csv", 0)
	c.Assert(err, IsNil)

	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	row, err = encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(3),
	}, 1, []int{0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, "9.csv", 0)
	c.Assert(err, IsNil)

	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	row, err = encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(4),
	}, 1, []int{0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, "10.csv", 0)
	c.Assert(err, IsNil)

	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	row, err = encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(5),
	}, 1, []int{0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, "11.csv", 0)
	c.Assert(err, IsNil)

	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	writer, err := engine.LocalWriter(ctx, nil)
	c.Assert(err, IsNil)
	err = writer.WriteRows(ctx, []string{"a"}, dataRows)
	c.Assert(err, NotNil)
	st, err := writer.Close(ctx)
	c.Assert(err, IsNil)
	c.Assert(st, IsNil)
}
