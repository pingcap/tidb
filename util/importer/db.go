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

package importer

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"

	_ "github.com/go-sql-driver/mysql" // for mysql driver
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbutil"
	"go.uber.org/zap"
)

func intRangeValue(column *column, min int64, max int64) (int64, int64) {
	var err error
	if len(column.min) > 0 {
		min, err = strconv.ParseInt(column.min, 10, 64)
		if err != nil {
			log.Fatal("intRangeValue", zap.Error(err))
		}

		if len(column.max) > 0 {
			max, err = strconv.ParseInt(column.max, 10, 64)
			if err != nil {
				log.Fatal("intRangeValue", zap.Error(err))
			}
		}
	}

	return min, max
}

func randInt64Value(column *column, min int64, max int64) int64 {
	if len(column.set) > 0 {
		idx := randInt(0, len(column.set)-1)
		data, _ := strconv.ParseInt(column.set[idx], 10, 64)
		return data
	}

	min, max = intRangeValue(column, min, max)
	return randInt64(min, max)
}

func uniqInt64Value(column *column, min int64, max int64) int64 {
	min, max = intRangeValue(column, min, max)
	column.data.setInitInt64Value(column.step, min, max)
	return column.data.uniqInt64()
}

func genRowDatas(table *table, count int) ([]string, error) {
	datas := make([]string, 0, count)
	for i := 0; i < count; i++ {
		data, err := genRowData(table)
		if err != nil {
			return nil, errors.Trace(err)
		}
		datas = append(datas, data)
	}

	return datas, nil
}

func genRowData(table *table) (string, error) {
	var values []byte // nolint: prealloc
	for _, column := range table.columns {
		data, err := genColumnData(table, column)
		if err != nil {
			return "", errors.Trace(err)
		}
		values = append(values, []byte(data)...)
		values = append(values, ',')
	}

	values = values[:len(values)-1]
	sql := fmt.Sprintf("insert into %s (%s) values (%s);", table.name, table.columnList, string(values))
	return sql, nil
}

func genColumnData(table *table, column *column) (string, error) {
	tp := column.tp
	_, isUnique := table.uniqIndices[column.name]
	isUnsigned := mysql.HasUnsignedFlag(tp.GetFlag())

	switch tp.GetType() {
	case mysql.TypeTiny:
		var data int64
		if isUnique {
			data = uniqInt64Value(column, 0, math.MaxUint8)
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxUint8)
			} else {
				data = randInt64Value(column, math.MinInt8, math.MaxInt8)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeShort:
		var data int64
		if isUnique {
			data = uniqInt64Value(column, 0, math.MaxUint16)
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxUint16)
			} else {
				data = randInt64Value(column, math.MinInt16, math.MaxInt16)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeLong:
		var data int64
		if isUnique {
			data = uniqInt64Value(column, 0, math.MaxUint32)
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxUint32)
			} else {
				data = randInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeLonglong:
		var data int64
		if isUnique {
			data = uniqInt64Value(column, 0, math.MaxInt64)
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxInt64)
			} else {
				data = randInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqString(tp.GetFlen()))...)
		} else {
			data = append(data, []byte(randString(randInt(1, tp.GetFlen())))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		var data float64
		if isUnique {
			data = float64(uniqInt64Value(column, 0, math.MaxInt64))
		} else {
			if isUnsigned {
				data = float64(randInt64Value(column, 0, math.MaxInt64))
			} else {
				data = float64(randInt64Value(column, math.MinInt32, math.MaxInt32))
			}
		}
		return strconv.FormatFloat(data, 'f', -1, 64), nil
	case mysql.TypeDate:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqDate())...)
		} else {
			data = append(data, []byte(randDate(column.min, column.max))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqTimestamp())...)
		} else {
			data = append(data, []byte(randTimestamp(column.min, column.max))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeDuration:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqTime())...)
		} else {
			data = append(data, []byte(randTime(column.min, column.max))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeYear:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqYear())...)
		} else {
			data = append(data, []byte(randYear(column.min, column.max))...)
		}

		data = append(data, '\'')
		return string(data), nil
	default:
		return "", errors.Errorf("unsupported column type - %v", column)
	}
}

func execSQL(db *sql.DB, sql string) error {
	if len(sql) == 0 {
		return nil
	}

	_, err := db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func createDB(cfg dbutil.DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Schema)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

func closeDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

func createDBs(cfg dbutil.DBConfig, count int) ([]*sql.DB, error) {
	dbs := make([]*sql.DB, 0, count)
	for i := 0; i < count; i++ {
		db, err := createDB(cfg)
		if err != nil {
			return nil, errors.Trace(err)
		}

		dbs = append(dbs, db)
	}

	return dbs, nil
}

func closeDBs(dbs []*sql.DB) {
	for _, db := range dbs {
		err := closeDB(db)
		if err != nil {
			log.Error("close db failed", zap.Error(err))
		}
	}
}
