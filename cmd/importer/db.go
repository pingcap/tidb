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

package main

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

func intRangeValue(column *column, min int64, max int64) (int64, int64) {
	var err error
	if len(column.min) > 0 {
		min, err = strconv.ParseInt(column.min, 10, 64)
		if err != nil {
			log.Fatal(err.Error())
		}

		if len(column.max) > 0 {
			max, err = strconv.ParseInt(column.max, 10, 64)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
	}

	return min, max
}

func randStringValue(column *column, n int) string {
	if column.hist != nil {
		if column.hist.avgLen == 0 {
			column.hist.avgLen = column.hist.getAvgLen(n)
		}
		return column.hist.randString()
	}
	if len(column.set) > 0 {
		idx := randInt(0, len(column.set)-1)
		return column.set[idx]
	}
	return randString(randInt(1, n))
}

func randInt64Value(column *column, min int64, max int64) int64 {
	if column.hist != nil {
		return column.hist.randInt()
	}
	if len(column.set) > 0 {
		idx := randInt(0, len(column.set)-1)
		data, err := strconv.ParseInt(column.set[idx], 10, 64)
		if err != nil {
			log.Warn("rand int64 failed", zap.Error(err))
		}
		return data
	}

	min, max = intRangeValue(column, min, max)
	return randInt64(min, max)
}

func nextInt64Value(column *column, min int64, max int64) int64 {
	min, max = intRangeValue(column, min, max)
	column.data.setInitInt64Value(min, max)
	return column.data.nextInt64()
}

func intToDecimalString(intValue int64, decimal int) string {
	data := fmt.Sprintf("%d", intValue)

	// add leading zero
	if len(data) < decimal {
		data = strings.Repeat("0", decimal-len(data)) + data
	}

	dec := data[len(data)-decimal:]
	if data = data[:len(data)-decimal]; data == "" {
		data = "0"
	}
	if dec != "" {
		data = data + "." + dec
	}
	return data
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
	var values []byte
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
	incremental := column.incremental
	if incremental {
		incremental = uint32(rand.Int31n(100))+1 <= column.data.probability
		// If incremental, there is only one worker, so it is safe to directly access datum.
		if !incremental && column.data.remains > 0 {
			column.data.remains--
		}
	}
	if _, ok := table.uniqIndices[column.name]; ok {
		incremental = true
	}
	isUnsigned := mysql.HasUnsignedFlag(tp.Flag)

	switch tp.Tp {
	case mysql.TypeTiny:
		var data int64
		if incremental {
			if isUnsigned {
				data = nextInt64Value(column, 0, math.MaxUint8)
			} else {
				data = nextInt64Value(column, math.MinInt8, math.MaxInt8)
			}
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
		if incremental {
			if isUnsigned {
				data = nextInt64Value(column, 0, math.MaxUint16)
			} else {
				data = nextInt64Value(column, math.MinInt16, math.MaxInt16)
			}
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
		if incremental {
			if isUnsigned {
				data = nextInt64Value(column, 0, math.MaxUint32)
			} else {
				data = nextInt64Value(column, math.MinInt32, math.MaxInt32)
			}
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
		if incremental {
			if isUnsigned {
				data = nextInt64Value(column, 0, math.MaxInt64-1)
			} else {
				data = nextInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxInt64-1)
			} else {
				data = randInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextString(tp.Flen))...)
		} else {
			data = append(data, []byte(randStringValue(column, tp.Flen))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeFloat, mysql.TypeDouble:
		var data float64
		if incremental {
			if isUnsigned {
				data = float64(nextInt64Value(column, 0, math.MaxInt64-1))
			} else {
				data = float64(nextInt64Value(column, math.MinInt32, math.MaxInt32))
			}
		} else {
			if isUnsigned {
				data = float64(randInt64Value(column, 0, math.MaxInt64-1))
			} else {
				data = float64(randInt64Value(column, math.MinInt32, math.MaxInt32))
			}
		}
		return strconv.FormatFloat(data, 'f', -1, 64), nil
	case mysql.TypeDate:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextDate())...)
		} else {
			data = append(data, []byte(randDate(column))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextTimestamp())...)
		} else {
			data = append(data, []byte(randTimestamp(column))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeDuration:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextTime())...)
		} else {
			data = append(data, []byte(randTime(column))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeYear:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextYear())...)
		} else {
			data = append(data, []byte(randYear(column))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeNewDecimal:
		var limit = int64(math.Pow10(tp.Flen))
		var intVal int64
		if limit < 0 {
			limit = math.MaxInt64
		}
		if incremental {
			if isUnsigned {
				intVal = nextInt64Value(column, 0, limit-1)
			} else {
				intVal = nextInt64Value(column, (-limit+1)/2, (limit-1)/2)
			}
		} else {
			if isUnsigned {
				intVal = randInt64Value(column, 0, limit-1)
			} else {
				intVal = randInt64Value(column, (-limit+1)/2, (limit-1)/2)
			}
		}
		return intToDecimalString(intVal, tp.Decimal), nil
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

func createDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

func closeDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

func createDBs(cfg DBConfig, count int) ([]*sql.DB, error) {
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
			log.Error("close DB failed", zap.Error(err))
		}
	}
}
