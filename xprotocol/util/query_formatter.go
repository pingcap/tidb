// Copyright 2017 PingCAP, Inc.
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

package util

import (
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	log "github.com/sirupsen/logrus"
)

// FormatQuery formats query for xsql.
func FormatQuery(sql string, args []*Mysqlx_Datatypes.Any) (string, error) {
	var err error
	for _, v := range args {
		if v.GetType() != Mysqlx_Datatypes.Any_SCALAR {
			return "", ErrXInvalidArgument.Gen("Invalid data, expecting scalar")
		}
		s := v.GetScalar()
		param := ""
		switch s.GetType() {
		case Mysqlx_Datatypes.Scalar_V_SINT:
			param = strconv.FormatInt(s.GetVSignedInt(), 10)
		case Mysqlx_Datatypes.Scalar_V_UINT:
			param = strconv.FormatUint(s.GetVUnsignedInt(), 10)
		case Mysqlx_Datatypes.Scalar_V_OCTETS:
			param = string(s.GetVOctets().GetValue())
		case Mysqlx_Datatypes.Scalar_V_DOUBLE:
			param = strconv.FormatFloat(s.GetVDouble(), 'f', -1, 64)
		case Mysqlx_Datatypes.Scalar_V_FLOAT:
			param = strconv.FormatFloat(float64(s.GetVFloat()), 'f', -1, 32)
		case Mysqlx_Datatypes.Scalar_V_BOOL:
			param = strconv.FormatBool(s.GetVBool())
		case Mysqlx_Datatypes.Scalar_V_STRING:
			param = QuoteString(string(s.GetVString().GetValue()))
		default:
			log.Panicf("not supported type %s", s.GetType().String())
		}
		sql, err = replaceQuery(sql, param)
		if err != nil {
			return "", errors.Trace(err)
		}
	}
	return sql, nil
}

func replaceQuery(sql, param string) (string, error) {
	idx := strings.Index(sql, "?")
	if idx == -1 {
		return "", ErrXCmdNumArguments.Gen("Too many arguments")
	}
	if shouldIgnore(idx, sql, "'", "'") {
		return sql, nil
	}
	if shouldIgnore(idx, sql, "\"", "\"") {
		return sql, nil
	}
	if shouldIgnore(idx, sql, "`", "`") {
		return sql, nil
	}
	if shouldIgnore(idx, sql, "/*", "*/") {
		return sql, nil
	}
	if shouldIgnore(idx, sql, "#", "\n") {
		return sql, nil
	}
	if shouldIgnore(idx, sql, "-- ", "\n") {
		return sql, nil
	}
	return strings.Replace(sql, "?", param, 1), nil
}

func shouldIgnore(idx int, sql, begin, end string) bool {
	if idx == 0 || idx == len(sql)-1 {
		return false
	}
	if i := strings.LastIndex(sql[:idx], begin); i == -1 {
		return false
	}
	if i := strings.Index(sql[idx+1:], end); i == -1 {
		return false
	}
	return true
}
