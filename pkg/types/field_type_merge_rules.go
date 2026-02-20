// Copyright 2015 PingCAP, Inc.
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

package types

import "github.com/pingcap/tidb/pkg/parser/mysql"

// mergeFieldType merges two MySQL type to a new type.
// This is used in hybrid field type expression.
// For example "select case c when 1 then 2 when 2 then 'tidb' from t;"
// The result field type of the case expression is the merged type of the two when clause.
// See https://github.com/mysql/mysql-server/blob/8.0/sql/field.cc#L1042
//
// This function doesn't handle the range bump: for example, when the unsigned long is merged with signed long,
// the result should be longlong. However, this function returns long for this case. Please use `AggFieldType`
// function if you need to handle the range bump.
func mergeFieldType(a byte, b byte) byte {
	ia := getFieldTypeIndex(a)
	ib := getFieldTypeIndex(b)
	return fieldTypeMergeRules[ia][ib]
}

// mergeTypeFlag merges two MySQL type flag to a new one
// currently only NotNullFlag and UnsignedFlag is checked
// todo more flag need to be checked
func mergeTypeFlag(a, b uint) uint {
	return a & (b&mysql.NotNullFlag | ^mysql.NotNullFlag) & (b&mysql.UnsignedFlag | ^mysql.UnsignedFlag)
}

var (
	fieldTypeIndexes = map[byte]int{
		mysql.TypeUnspecified:       0,
		mysql.TypeTiny:              1,
		mysql.TypeShort:             2,
		mysql.TypeLong:              3,
		mysql.TypeFloat:             4,
		mysql.TypeDouble:            5,
		mysql.TypeNull:              6,
		mysql.TypeTimestamp:         7,
		mysql.TypeLonglong:          8,
		mysql.TypeInt24:             9,
		mysql.TypeDate:              10,
		mysql.TypeDuration:          11,
		mysql.TypeDatetime:          12,
		mysql.TypeYear:              13,
		mysql.TypeNewDate:           14,
		mysql.TypeVarchar:           15,
		mysql.TypeBit:               16,
		mysql.TypeJSON:              17,
		mysql.TypeNewDecimal:        18,
		mysql.TypeEnum:              19,
		mysql.TypeSet:               20,
		mysql.TypeTinyBlob:          21,
		mysql.TypeMediumBlob:        22,
		mysql.TypeLongBlob:          23,
		mysql.TypeBlob:              24,
		mysql.TypeVarString:         25,
		mysql.TypeString:            26,
		mysql.TypeGeometry:          27,
		mysql.TypeTiDBVectorFloat32: 28,
	}
)

func getFieldTypeIndex(tp byte) int {
	return fieldTypeIndexes[tp]
}

// https://github.com/mysql/mysql-server/blob/8.0/sql/field.cc#L248
var fieldTypeMergeRules = [29][29]byte{
	/* mysql.TypeUnspecified -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeUnspecified, mysql.TypeUnspecified,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeTiny -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeTiny,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTiny, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeTiny,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeShort -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeShort,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeShort, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeShort,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeLong -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeLong,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeLong, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeLong, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLong,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeLong,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeFloat -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeDouble, mysql.TypeFloat,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeFloat, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeFloat, mysql.TypeFloat,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeFloat,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeDouble,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeDouble, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeDouble -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDouble, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeDouble,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeDouble,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeDouble, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeNull -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeTiny,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNull, mysql.TypeTimestamp,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLonglong,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDate, mysql.TypeDuration,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeYear,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeBit,
		// mysql.TypeJSON
		mysql.TypeJSON,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeEnum,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeSet, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeGeometry,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeTiDBVectorFloat32,
	},
	/* mysql.TypeTimestamp -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTimestamp, mysql.TypeTimestamp,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDatetime, mysql.TypeDatetime,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeLonglong -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeLonglong,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeLonglong, mysql.TypeLonglong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeLonglong, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLong,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeLonglong,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeInt24 -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeInt24,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeInt24, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeInt24, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeInt24,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal    mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeDate -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDate, mysql.TypeDatetime,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDate, mysql.TypeDatetime,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeTime -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDuration, mysql.TypeDatetime,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDatetime, mysql.TypeDuration,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeDatetime -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDatetime, mysql.TypeDatetime,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDatetime, mysql.TypeDatetime,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeYear -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeUnspecified, mysql.TypeTiny,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeYear, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeYear,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeNewDate -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDate, mysql.TypeDatetime,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeNewDate, mysql.TypeDatetime,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeVarchar -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeBit -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeLonglong,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeLonglong, mysql.TypeLonglong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeBit, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLonglong,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeLonglong,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeBit,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeJSON -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewFloat     mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeJSON, mysql.TypeVarchar,
		// mysql.TypeLongLONG     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         MYSQL_TYPE_TIME
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     MYSQL_TYPE_YEAR
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeJSON,
		// mysql.TypeNewDecimal   MYSQL_TYPE_ENUM
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeLongBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeLongBlob, mysql.TypeVarchar,
		// mysql.TypeString       MYSQL_TYPE_GEOMETRY
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeNewDecimal -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeNewDecimal,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeNewDecimal,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeEnum -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeEnum, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeSet -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeSet, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeTinyBlob -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeTinyBlob,
		// mysql.TypeJSON
		mysql.TypeLongBlob,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeTinyBlob,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeLongBlob,
	},
	/* mysql.TypeMediumBlob -> */
	{
		// mysql.TypeUnspecified    mysql.TypeTiny
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeMediumBlob,
		// mysql.TypeJSON
		mysql.TypeLongBlob,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeLongBlob,
	},
	/* mysql.TypeLongBlob -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLongBlob,
		// mysql.TypeJSON
		mysql.TypeLongBlob,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeLongBlob,
	},
	/* mysql.TypeBlob -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeBlob,
		// mysql.TypeJSON
		mysql.TypeLongBlob,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeLongBlob,
	},
	/* mysql.TypeVarString -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeString -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeString,
		// mysql.TypeJSON
		mysql.TypeString,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeString, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeString,
	},
	/* mysql.TypeGeometry -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeGeometry, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeGeometry,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeVarchar,
	},
	/* mysql.TypeTiDBVectorFloat32 -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewFloat     mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTiDBVectorFloat32, mysql.TypeVarchar,
		// mysql.TypeLongLONG     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         MYSQL_TYPE_TIME
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     MYSQL_TYPE_YEAR
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   MYSQL_TYPE_ENUM
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeLongBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeLongBlob, mysql.TypeVarchar,
		// mysql.TypeString       MYSQL_TYPE_GEOMETRY
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeTiDBVectorFloat32
		mysql.TypeTiDBVectorFloat32,
	},
}
