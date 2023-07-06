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
	var (
		ts        uint64 = 400036290571534337
		txnSource uint64 = 9527
	)

	buff := make([]byte, 0, 9)
	buff = append(buff, WriteTypePut)
	buff = codec.EncodeUvarint(buff, ts)
	buff = append(buff, flagTxnSourcePrefix)
	buff = codec.EncodeUvarint(buff, txnSource)

	v := new(RawWriteCFValue)
	err := v.ParseFrom(buff)
	require.NoError(t, err)
	require.False(t, v.IsDelete())
	require.False(t, v.IsRollback())
	require.False(t, v.HasShortValue())
	require.False(t, v.hasGCFence)
	require.Equal(t, v.lastChangeTs, uint64(0))
	require.Equal(t, v.versionsToLastChange, uint64(0))
	require.Equal(t, v.txnSource, txnSource)

	encodedBuff := v.EncodeTo()
	require.True(t, bytes.Equal(buff, encodedBuff))
}

func TestWriteCFValueWithShortValue(t *testing.T) {
	var (
		ts                   uint64 = 400036290571534337
		shortValue                  = []byte("pingCAP")
		lastChangeTs         uint64 = 9527
		versionsToLastChange uint64 = 95271
	)

	buff := make([]byte, 0, 9)
	buff = append(buff, WriteTypePut)
	buff = codec.EncodeUvarint(buff, ts)
	buff = append(buff, flagShortValuePrefix)
	buff = append(buff, byte(len(shortValue)))
	buff = append(buff, shortValue...)
	buff = append(buff, flagLastChangePrefix)
	buff = codec.EncodeUint(buff, lastChangeTs)
	buff = codec.EncodeUvarint(buff, versionsToLastChange)

	v := new(RawWriteCFValue)
	err := v.ParseFrom(buff)
	require.NoError(t, err)
	require.True(t, v.HasShortValue())
	require.True(t, bytes.Equal(v.GetShortValue(), shortValue))
	require.False(t, v.hasGCFence)
	require.False(t, v.hasOverlappedRollback)
	require.Equal(t, v.lastChangeTs, lastChangeTs)
	require.Equal(t, v.versionsToLastChange, versionsToLastChange)
	require.Equal(t, v.txnSource, uint64(0))

	data := v.EncodeTo()
	require.True(t, bytes.Equal(data, buff))
}

func TestWriteCFValueWithRollback(t *testing.T) {
	var (
		ts                          uint64 = 400036290571534337
		protectedRollbackShortValue        = []byte{'P'}
	)

	buff := make([]byte, 0, 9)
	buff = append(buff, WriteTypeRollback)
	buff = codec.EncodeUvarint(buff, ts)
	buff = append(buff, flagShortValuePrefix, byte(len(protectedRollbackShortValue)))
	buff = append(buff, protectedRollbackShortValue...)

	v := new(RawWriteCFValue)
	err := v.ParseFrom(buff)
	require.NoError(t, err)
	require.True(t, v.IsRollback())
	require.True(t, v.HasShortValue())
	require.Equal(t, v.GetShortValue(), protectedRollbackShortValue)
	require.Equal(t, v.startTs, ts)
	require.Equal(t, v.lastChangeTs, uint64(0))
	require.Equal(t, v.versionsToLastChange, uint64(0))
	require.Equal(t, v.txnSource, uint64(0))

	data := v.EncodeTo()
	require.Equal(t, data, buff)
}

func TestWriteCFValueWithDelete(t *testing.T) {
	var ts uint64 = 400036290571534337
	buff := make([]byte, 0, 9)
	buff = append(buff, byte('D'))
	buff = codec.EncodeUvarint(buff, ts)

	v := new(RawWriteCFValue)
	err := v.ParseFrom(buff)
	require.NoError(t, err)
	require.True(t, v.IsDelete())
	require.False(t, v.HasShortValue())

	data := v.EncodeTo()
	require.Equal(t, data, buff)
}

func TestWriteCFValueWithGcFence(t *testing.T) {
	var (
		ts      uint64 = 400036290571534337
		gcFence uint64 = 9527
	)

	buff := make([]byte, 0, 9)
	buff = append(buff, WriteTypePut)
	buff = codec.EncodeUvarint(buff, ts)
	buff = append(buff, flagOverlappedRollback)
	buff = append(buff, flagGCFencePrefix)
	buff = codec.EncodeUint(buff, gcFence)

	v := new(RawWriteCFValue)
	err := v.ParseFrom(buff)
	require.NoError(t, err)
	require.Equal(t, v.startTs, ts)
	require.True(t, v.hasGCFence)
	require.Equal(t, v.gcFence, gcFence)
	require.True(t, v.hasOverlappedRollback)

	data := v.EncodeTo()
	require.Equal(t, data, buff)
}
