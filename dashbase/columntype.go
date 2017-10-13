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

// KafkaEncoder converts a datum into a type for Dashbase Kafka API
type kafkaEncoder func(types.Datum) interface{}

type codecDefinition struct {
	columnType ColumnType
	encoder    kafkaEncoder
}

var codecs = []*codecDefinition{
	{
		TypeText,
		func(input types.Datum) interface{} {
			return input.GetString()
		},
	},
	{
		TypeMeta,
		func(input types.Datum) interface{} {
			return input.GetString()
		},
	},
	{
		TypeNumeric,
		func(input types.Datum) interface{} {
			return input.GetFloat64()
		},
	},
	{
		TypeTime,
		func(input types.Datum) interface{} {
			time, err := input.GetMysqlTime().Time.GoTime(time.Local)
			if err != nil {
				return 0
			}
			return time.Unix() * 1000
		},
	},
}

var encoders map[ColumnType]kafkaEncoder

func init() {
	encoders = make(map[ColumnType]kafkaEncoder)
	for _, codec := range codecs {
		encoders[codec.columnType] = codec.encoder
	}
}

func getEncoder(columnType ColumnType) (kafkaEncoder, error) {
	encoder, ok := encoders[columnType]
	if !ok {
		return nil, errors.Trace(fmt.Errorf("Unsupported Dashbase type %s", columnType))
	}
	return encoder, nil
}
