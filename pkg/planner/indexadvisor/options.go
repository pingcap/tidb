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

package indexadvisor

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pkg/errors"
)

const (
	// OptModule is the module name for index advisor options.
	OptModule = "index_advisor"
	// OptMaxNumIndex is the option name for the maximum number of indexes to recommend for a table.
	OptMaxNumIndex = "max_num_index"
	// OptMaxIndexColumns is the option name for the maximum number of columns in an index.
	OptMaxIndexColumns = "max_index_columns"
	// OptMaxNumQuery is the option name for the maximum number of queries to recommend indexes.
	OptMaxNumQuery = "max_num_query"
	// OptTimeout is the option name for the timeout of index advisor.
	OptTimeout = "timeout"
)

func fillOption(sctx sessionctx.Context, opt *Option) error {
	vals, err := getOption(sctx, OptMaxNumIndex, OptMaxIndexColumns, OptMaxNumQuery, OptTimeout)
	if err != nil {
		return err
	}
	if opt.MaxNumIndexes == 0 {
		i, _ := strconv.ParseInt(vals[OptMaxNumIndex], 10, 64)
		opt.MaxNumIndexes = int(i)
	}
	if opt.MaxIndexWidth == 0 {
		i, _ := strconv.ParseInt(vals[OptMaxIndexColumns], 10, 64)
		opt.MaxIndexWidth = int(i)
	}
	if opt.MaxNumQuery == 0 {
		i, _ := strconv.ParseInt(vals[OptMaxNumQuery], 10, 64)
		opt.MaxNumQuery = int(i)
	}
	if opt.Timeout == 0 {
		i, err := time.ParseDuration(vals[OptTimeout])
		if err != nil {
			return err
		}
		opt.Timeout = i
	}
	return nil
}

// SetOption sets the value of an option.
func SetOption(sctx sessionctx.Context, opt string, val ast.ValueExpr) error {
	var v string
	switch opt {
	case OptMaxNumIndex, OptMaxIndexColumns, OptMaxNumQuery:
		x, err := intVal(val)
		if err != nil {
			return err
		}
		if x <= 0 {
			return errors.Errorf("invalid value %v for %s", x, opt)
		}
		v = strconv.Itoa(x)
	case OptTimeout:
		v = val.GetValue().(string)
		d, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		if d < 0 {
			return errors.Errorf("invalid value %v for %s", d, opt)
		}
	default:
		return errors.Errorf("unknown option %s", opt)
	}
	template := `INSERT INTO mysql.tidb_kernel_options VALUES (%?, %?, %?, now(), 'valid', %?)
		ON DUPLICATE KEY UPDATE value = %?, updated_at=now(), description = %?`
	_, err := exec(sctx, template, OptModule, opt, v, description(opt), v, description(opt))
	return err
}

func getOption(sctx sessionctx.Context, opts ...string) (map[string]string, error) {
	template := `SELECT name, value FROM mysql.tidb_kernel_options WHERE module = '%v' AND name in (%v)`
	var optStrs string
	for i, opt := range opts {
		if i > 0 {
			optStrs += ","
		}
		optStrs += fmt.Sprintf("'%s'", opt)
	}

	sql := fmt.Sprintf(template, OptModule, optStrs)
	rows, err := exec(sctx, sql)
	if err != nil {
		return nil, err
	}
	res := make(map[string]string)
	for _, row := range rows {
		res[row.GetString(0)] = row.GetString(1)
	}
	for _, opt := range opts {
		if _, ok := res[opt]; !ok {
			res[opt] = defaultVal(opt)
		}
	}
	return res, nil
}

func description(opt string) string {
	switch opt {
	case OptMaxNumIndex:
		return "The maximum number of indexes to recommend for a table."
	case OptMaxIndexColumns:
		return "The maximum number of columns in an index."
	case OptMaxNumQuery:
		return "The maximum number of queries to recommend indexes."
	case OptTimeout:
		return "The timeout of index advisor."
	}
	return ""
}

func defaultVal(opt string) string {
	switch opt {
	case OptMaxNumIndex:
		return "5"
	case OptMaxIndexColumns:
		return "3"
	case OptMaxNumQuery:
		return "1000"
	case OptTimeout:
		return "30s"
	}
	return ""
}

func intVal(val ast.ValueExpr) (int, error) {
	switch v := val.GetValue().(type) {
	case int:
		return v, nil
	case uint64:
		return int(v), nil
	case int64:
		return int(v), nil
	default:
		return 0, errors.Errorf("invalid value type %T", v)
	}
}
