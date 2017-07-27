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

package dashbase

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// ColumnType is the column type
type ColumnType string

const (
	// TypeMeta is the `meta` type in Dashbase
	TypeMeta ColumnType = "meta"

	// TypeTime is the `time` type in Dashbase
	TypeTime ColumnType = "time"

	// TypeNumeric is the `numeric` type in Dashbase
	TypeNumeric ColumnType = "numeric"

	// TypeText is the `text` type in Dashbase
	TypeText ColumnType = "text"
)

type Lo2HiConverter func(interface{}) interface{}

type Column struct {
	Name     string     `json:"name"`
	LowType  ColumnType `json:"ltp"` // Dashbase Type
	HighType byte       `json:"htp"` // MySQL Type
}

type columnConverterDefinition struct {
	lowType  ColumnType
	highType byte
	lo2hi    Lo2HiConverter
}

var typeConverters = []*columnConverterDefinition{
	{TypeText, mysql.TypeBlob, func(input interface{}) interface{} {
		return input.(string)
	}},
	{TypeMeta, mysql.TypeBlob, func(input interface{}) interface{} {
		return input.(string)
	}},
	{TypeNumeric, mysql.TypeDouble, func(input interface{}) interface{} {
		return input.(float64)
	}},
	{TypeTime, mysql.TypeDatetime, func(input interface{}) interface{} {
		timestampMilliseconds := int64(input.(float64))
		// TODO: support milliseconds.
		value := time.Unix(timestampMilliseconds/1000, 0)
		return types.Time{
			Time:     types.FromGoTime(value),
			Type:     mysql.TypeDatetime,
			TimeZone: time.UTC,
		}
	}},
}

var lo2HiConverters map[ColumnType]map[byte]Lo2HiConverter

func init() {
	lo2HiConverters = make(map[ColumnType]map[byte]Lo2HiConverter)
	for _, converter := range typeConverters {
		_, ok := lo2HiConverters[converter.lowType]
		if !ok {
			lo2HiConverters[converter.lowType] = make(map[byte]Lo2HiConverter)
		}
		lo2HiConverters[converter.lowType][converter.highType] = converter.lo2hi
	}
}

func GetLo2HiConverter(column *Column) (Lo2HiConverter, error) {
	_, ok := lo2HiConverters[column.LowType]
	if !ok {
		return nil, errors.Trace(fmt.Errorf("Unsupported Dashbase type %s", column.LowType))
	}
	converter, ok := lo2HiConverters[column.LowType][column.HighType]
	if !ok {
		return nil, errors.Trace(fmt.Errorf("Unsupported MySQL type %d", column.HighType))
	}
	return converter, nil
}
