// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"bytes"
	"testing"

	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

func encodeTxnMetaKey(key []byte, field []byte, ts uint64) []byte {
	k := tablecodec.EncodeMetaKey(key, field)
	txnKey := codec.EncodeBytes(nil, k)
	return codec.EncodeUintDesc(txnKey, ts)
}

func TestRawMetaKeyForDB(t *testing.T) {
	var (
		dbID int64  = 1
		ts   uint64 = 400036290571534337
		mDbs        = []byte("DBs")
	)

	txnKey := encodeTxnMetaKey(mDbs, meta.DBkey(dbID), ts)

	rawMetaKey, err := ParseTxnMetaKeyFrom(txnKey)
	require.NoError(t, err)

	parseDbID, err := meta.ParseDBKey(rawMetaKey.Field)
	require.NoError(t, err)
	require.Equal(t, dbID, parseDbID)

	newKey := rawMetaKey.EncodeMetaKey()
	require.Equal(t, string(txnKey), string(newKey))
}

func TestRawMetaKeyForTable(t *testing.T) {
	var (
		dbID    int64  = 1
		tableID int64  = 57
		ts      uint64 = 400036290571534337
	)
	txnKey := encodeTxnMetaKey(meta.DBkey(dbID), meta.TableKey(tableID), ts)

	rawMetakey, err := ParseTxnMetaKeyFrom(txnKey)
	require.NoError(t, err)

	parseDBID, err := meta.ParseDBKey(rawMetakey.Key)
	require.NoError(t, err)
	require.Equal(t, dbID, parseDBID)

	parseTableID, err := meta.ParseTableKey(rawMetakey.Field)
	require.NoError(t, err)
	require.Equal(t, tableID, parseTableID)

	newKey := rawMetakey.EncodeMetaKey()
	require.True(t, bytes.Equal(txnKey, newKey))
}

func TestWriteType(t *testing.T) {
	wt, err := WriteTypeFrom(byte('P'))
	require.NoError(t, err)
	require.Equal(t, WriteTypePut, wt)
}

func TestWriteCFValueNoShortValue(t *testing.T) {
	buff := make([]byte, 0, 9)
	buff = append(buff, byte('P'))
	buff = codec.EncodeUvarint(buff, 400036290571534337)

	v := new(RawWriteCFValue)
	err := v.ParseFrom(buff)
	require.NoError(t, err)
	require.False(t, v.HasShortValue())

	encodedBuff := v.EncodeTo()
	require.True(t, bytes.Equal(buff, encodedBuff))
}

func TestWriteCFValueWithShortValue(t *testing.T) {
	var ts uint64 = 400036290571534337
	shortValue := []byte("pingCAP")

	buff := make([]byte, 0, 9)
	buff = append(buff, byte('P'))
	buff = codec.EncodeUvarint(buff, ts)
	buff = append(buff, flagShortValuePrefix)
	buff = append(buff, byte(len(shortValue)))
	buff = append(buff, shortValue...)

	v := new(RawWriteCFValue)
	err := v.ParseFrom(buff)
	require.NoError(t, err)
	require.True(t, v.HasShortValue())
	require.True(t, bytes.Equal(v.GetShortValue(), shortValue))
	require.False(t, v.hasGCFence)
	require.False(t, v.hasOverlappedRollback)

	data := v.EncodeTo()
	require.True(t, bytes.Equal(data, buff))
}
