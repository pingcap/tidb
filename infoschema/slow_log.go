package infoschema

import (
	"bufio"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

var slowQueryCols = []columnInfo{
	{variable.SlowLogTimeStr, mysql.TypeDatetime, -1, 0, nil, nil},
	{variable.SlowLogTxnStartTSStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogUserStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogConnIDStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogQueryTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.ProcessTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.WaitTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.BackoffTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.RequestCountStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.TotalKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.ProcessedKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogDBStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogIsInternalStr, mysql.TypeTiny, 1, 0, nil, nil},
	{variable.SlowLogQuerySQLStr, mysql.TypeVarchar, 4096, 0, nil, nil},
}

func dataForSlowLog(ctx sessionctx.Context) ([][]types.Datum, error) {
	rowsMap, err := parseSlowLogFile(ctx.GetSessionVars().SlowQueryFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var rows [][]types.Datum
	for _, row := range rowsMap {
		record := make([]types.Datum, 0, len(slowQueryCols))
		for _, col := range slowQueryCols {
			if v, ok := row[col.name]; ok {
				record = append(record, v)
			} else {
				record = append(record, types.NewDatum(nil))
			}
		}
		rows = append(rows, record)
	}
	return rows, nil
}

func parseSlowLogFile(filePath string) ([]map[string]types.Datum, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer file.Close()

	rows := make([]map[string]types.Datum, 0)
	rowMap := make(map[string]types.Datum, len(slowQueryCols))
	startFlag := false
	startPrefix := variable.SlowLogPrefixStr + variable.SlowLogTimeStr + variable.SlowLogSpaceMarkStr
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// check start
		if !startFlag && strings.Contains(line, startPrefix) {
			t, err := parseTime(line[len(startPrefix):])
			if err != nil {
				log.Errorf("parse slow log error: %v", err)
				// temporary ignore now.
				continue
			}
			rowMap[variable.SlowLogTimeStr] = types.NewTimeDatum(types.Time{
				Time: types.FromGoTime(t),
				Type: mysql.TypeDatetime,
				Fsp:  types.MaxFsp,
			})
			startFlag = true
			continue
		}

		if startFlag {
			// parse field.
			if strings.Contains(line, variable.SlowLogPrefixStr) {
				line = line[len(variable.SlowLogPrefixStr):]
				fieldValues := strings.Split(line, " ")
				for i := 0; i < len(fieldValues)-1; i += 2 {
					field := fieldValues[i]
					if strings.HasSuffix(field, ":") {
						field = field[:len(field)-1]
					}
					col := findColumnByName(slowQueryCols, field)
					if col == nil {
						continue
					}
					var value types.Datum
					switch col.tp {
					case mysql.TypeLonglong:
						num, err := strconv.ParseUint(fieldValues[i+1], 10, 64)
						if err != nil {
							log.Errorf("parse slow log error: %v", err)
							break
						}
						value = types.NewUintDatum(num)
					case mysql.TypeVarchar:
						value = types.NewStringDatum(fieldValues[i+1])
					case mysql.TypeDouble:
						num, err := strconv.ParseFloat(fieldValues[i+1], 64)
						if err != nil {
							log.Errorf("parse slow log error: %v", err)
							break
						}
						value = types.NewDatum(num)
					case mysql.TypeTiny:
						// parse bool
						value = types.NewDatum(fieldValues[i+1] == "true")
					}
					rowMap[field] = value

				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				// get the sql string, and mark the start flag to false.
				rowMap[variable.SlowLogQuerySQLStr] = types.NewStringDatum(copyStringHack(line))
				rows = append(rows, rowMap)
				rowMap = make(map[string]types.Datum, len(slowQueryCols))
				startFlag = false
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(logutil.SlowLogTimeFormat, s)

	if err != nil {
		err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\", err: %v", s, logutil.SlowLogTimeFormat, err)
	}
	return t, err
}

func findColumnByName(cols []columnInfo, colName string) *columnInfo {
	for _, col := range cols {
		if col.name == colName {
			return &col
		}
	}
	return nil
}

func copyString(s string) string {
	return string([]byte(s))
}

func copyStringHack(s string) string {
	return string(hack.Slice(s))
}
