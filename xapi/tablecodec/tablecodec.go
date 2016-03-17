package tablecodec

import (
	"sort"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi/tipb"
)

var (
	tablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
)

const recordKeyLen = 1 + 8 + 2 + 8 + 8

// EncodeRecordKey encodes the
func EncodeRecordKey(tableID int64, h int64, columnID int64) kv.Key {
	buf := make([]byte, 0, recordKeyLen)
	buf = appendTableRecordPrefix(buf, tableID)
	buf = codec.EncodeInt(buf, h)
	if columnID != 0 {
		buf = codec.EncodeInt(buf, columnID)
	}
	return buf
}

// DecodeRecordKey decodes the key and gets the tableID, handle and columnID.
func DecodeRecordKey(key kv.Key) (tableID int64, handle int64, columnID int64, err error) {
	k := key
	if !key.HasPrefix(tablePrefix) {
		return 0, 0, 0, errors.Errorf("invalid record key - %q", k)
	}

	key = key[len(tablePrefix):]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}

	if !key.HasPrefix(recordPrefixSep) {
		return 0, 0, 0, errors.Errorf("invalid record key - %q", k)
	}

	key = key[len(recordPrefixSep):]

	key, handle, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}
	if len(key) == 0 {
		return
	}

	key, columnID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}
	return
}

// DecodeValue implements table.Table DecodeValue interface.
func DecodeValue(data []byte, tp *tipb.ColumnInfo) (types.Datum, error) {
	values, err := codec.Decode(data)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return Unflatten(values[0], tp)
}

// Unflatten converts a raw datum to a column datum.
func Unflatten(datum types.Datum, tp *tipb.ColumnInfo) (types.Datum, error) {
	if datum.Kind() == types.KindNull {
		return datum, nil
	}
	switch tp.GetTp() {
	case tipb.MysqlType_TypeFloat:
		datum.SetFloat32(float32(datum.GetFloat64()))
		return datum, nil
	case tipb.MysqlType_TypeTiny, tipb.MysqlType_TypeShort, tipb.MysqlType_TypeYear, tipb.MysqlType_TypeInt24,
		tipb.MysqlType_TypeLong, tipb.MysqlType_TypeLonglong, tipb.MysqlType_TypeDouble, tipb.MysqlType_TypeTinyBlob,
		tipb.MysqlType_TypeMediumBlob, tipb.MysqlType_TypeBlob, tipb.MysqlType_TypeLongBlob, tipb.MysqlType_TypeVarchar,
		tipb.MysqlType_TypeString:
		return datum, nil
	case tipb.MysqlType_TypeDate, tipb.MysqlType_TypeDatetime, tipb.MysqlType_TypeTimestamp:
		var t mysql.Time
		t.Type = uint8(tp.GetTp())
		t.Fsp = int(tp.GetDecimal())
		err := t.Unmarshal(datum.GetBytes())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(t)
		return datum, nil
	case tipb.MysqlType_TypeDuration:
		dur := mysql.Duration{Duration: time.Duration(datum.GetInt64())}
		datum.SetValue(dur)
		return datum, nil
	case tipb.MysqlType_TypeNewDecimal:
		dec, err := mysql.ParseDecimal(datum.GetString())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(dec)
		return datum, nil
	case tipb.MysqlType_TypeEnum:
		enum, err := mysql.ParseEnumValue(tp.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(enum)
		return datum, nil
	case tipb.MysqlType_TypeSet:
		set, err := mysql.ParseSetValue(tp.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(set)
		return datum, nil
	case tipb.MysqlType_TypeBit:
		bit := mysql.Bit{Value: datum.GetUint64(), Width: int(tp.GetColumnLen())}
		datum.SetValue(bit)
		return datum, nil
	}
	log.Error(tp.GetTp(), datum)
	return datum, nil
}

// EncodeIndexKey encodes indexed valuse into a key.
func EncodeIndexKey(tableID int64, indexedValues []types.Datum, handle int64, unique bool) (key kv.Key, distinct bool, err error) {
	if unique {
		// See: https://dev.mysql.com/doc/refman/5.7/en/create-index.html
		// A UNIQUE index creates a constraint such that all values in the index must be distinct.
		// An error occurs if you try to add a new row with a key value that matches an existing row.
		// For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
		distinct = true
		for _, cv := range indexedValues {
			if cv.Kind() == types.KindNull {
				distinct = false
				break
			}
		}
	}
	key = appendTableIndexPrefix(key, tableID)
	key, err = codec.EncodeKey(key, indexedValues...)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if !distinct {
		key, err = codec.EncodeKey(key, types.NewDatum(handle))
	}
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return key, distinct, nil
}

// record prefix is "t[tableID]_r"
func appendTableRecordPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// index prefix is "t[tableID]_i"
func appendTableIndexPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// SortHandles sorts int64 handles slice.
func SortHandles(handles []int64) {
	sort.Sort(int64Slice(handles))
}
