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

// Lo2HiConverter converts a type parsed from Dashbase JSON into a type datum accepts
type Lo2HiConverter func(interface{}) types.Datum

// Hi2LoConverter converts a datum into a type for Dashbase JSON API
type Hi2LoConverter func(types.Datum) interface{}

type Column struct {
	Name     string     `json:"name"`
	LowType  ColumnType `json:"ltp"` // Dashbase Type
	HighType byte       `json:"htp"` // MySQL Type
}

type columnConverterDefinition struct {
	lowType  ColumnType
	highType byte
	lo2hi    Lo2HiConverter
	hi2lo    Hi2LoConverter
}

var typeConverters = []*columnConverterDefinition{
	{
		TypeText, mysql.TypeBlob,
		func(input interface{}) types.Datum {
			return types.NewDatum(input.(string))
		},
		func(input types.Datum) interface{} {
			return input.GetString()
		},
	},
	{
		TypeMeta, mysql.TypeBlob,
		func(input interface{}) types.Datum {
			return types.NewDatum(input.(string))
		},
		func(input types.Datum) interface{} {
			return input.GetString()
		},
	},
	{
		TypeNumeric, mysql.TypeDouble,
		func(input interface{}) types.Datum {
			return types.NewDatum(input.(float64))
		},
		func(input types.Datum) interface{} {
			return input.GetFloat64()
		},
	},
	{
		TypeTime, mysql.TypeDatetime,
		func(input interface{}) types.Datum {
			timestampMilliseconds := int64(input.(float64))
			// TODO: support milliseconds.
			value := time.Unix(timestampMilliseconds/1000, 0)
			return types.NewDatum(types.Time{
				Time:     types.FromGoTime(value),
				Type:     mysql.TypeDatetime,
				TimeZone: time.Local,
			})
		},
		func(input types.Datum) interface{} {
			time, err := input.GetMysqlTime().Time.GoTime(time.UTC)
			if err != nil {
				return 0
			}
			return time.Unix() * 1000
		},
	},
}

var lo2HiConverters map[ColumnType]map[byte]Lo2HiConverter
var hi2LoConverters map[byte]map[ColumnType]Hi2LoConverter

func init() {
	lo2HiConverters = make(map[ColumnType]map[byte]Lo2HiConverter)
	hi2LoConverters = make(map[byte]map[ColumnType]Hi2LoConverter)
	for _, converter := range typeConverters {
		if _, ok := lo2HiConverters[converter.lowType]; !ok {
			lo2HiConverters[converter.lowType] = make(map[byte]Lo2HiConverter)
		}
		lo2HiConverters[converter.lowType][converter.highType] = converter.lo2hi
		if _, ok := hi2LoConverters[converter.highType]; !ok {
			hi2LoConverters[converter.highType] = make(map[ColumnType]Hi2LoConverter)
		}
		hi2LoConverters[converter.highType][converter.lowType] = converter.hi2lo
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

func GetHi2LoConverter(column *Column) (Hi2LoConverter, error) {
	_, ok := hi2LoConverters[column.HighType]
	if !ok {
		return nil, errors.Trace(fmt.Errorf("Unsupported MySQL type %d", column.HighType))
	}
	converter, ok := hi2LoConverters[column.HighType][column.LowType]
	if !ok {
		return nil, errors.Trace(fmt.Errorf("Unsupported Dashbase type %s", column.LowType))
	}
	return converter, nil
}
