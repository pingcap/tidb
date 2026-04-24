// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"bytes"
	"testing"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestRawMetaKeyForDB(t *testing.T) {
	var (
		dbID int64  = 1
		ts   uint64 = 400036290571534337
		mDbs        = []byte("DBs")
	)

	txnKey := utils.EncodeTxnMetaKey(mDbs, meta.DBkey(dbID), ts)

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
	txnKey := utils.EncodeTxnMetaKey(meta.DBkey(dbID), meta.TableKey(tableID), ts)

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

func TestWriteCFValueShortValueOverflow(t *testing.T) {
	var (
		ts uint64 = 400036290571534337
	)

	// Test case 1: vlen indicates more data than available
	buff := make([]byte, 0, 9)
	buff = append(buff, WriteTypePut)
	buff = codec.EncodeUvarint(buff, ts)
	buff = append(buff, flagShortValuePrefix)
	buff = append(buff, byte(10)) // vlen=10, but only have 0 bytes following
	// Not adding the actual short value data to trigger the overflow protection

	v := new(RawWriteCFValue)
	err := v.ParseFrom(buff)
	require.Error(t, err)
	require.Contains(t, err.Error(), "insufficient data for short value")
	require.Contains(t, err.Error(), "need 12 bytes but only have")

	// Test case 2: vlen indicates more data than partially available
	buff2 := make([]byte, 0, 9)
	buff2 = append(buff2, WriteTypePut)
	buff2 = codec.EncodeUvarint(buff2, ts)
	buff2 = append(buff2, flagShortValuePrefix)
	buff2 = append(buff2, byte(10))           // vlen=10
	buff2 = append(buff2, []byte("short")...) // Only 5 bytes, but need 10

	v2 := new(RawWriteCFValue)
	err2 := v2.ParseFrom(buff2)
	require.Error(t, err2)
	require.Contains(t, err2.Error(), "insufficient data for short value")
	require.Contains(t, err2.Error(), "need 12 bytes but only have 7")

	// Test case 3: Edge case with vlen=255 (max byte value)
	buff3 := make([]byte, 0, 9)
	buff3 = append(buff3, WriteTypePut)
	buff3 = codec.EncodeUvarint(buff3, ts)
	buff3 = append(buff3, flagShortValuePrefix)
	buff3 = append(buff3, byte(255))         // vlen=255
	buff3 = append(buff3, []byte("test")...) // Only 4 bytes, but need 255

	v3 := new(RawWriteCFValue)
	err3 := v3.ParseFrom(buff3)
	require.Error(t, err3)
	require.Contains(t, err3.Error(), "insufficient data for short value")
	require.Contains(t, err3.Error(), "need 257 bytes but only have 6")

	// Test case 4: vlen=255 with sufficient data (should succeed)
	buff4 := make([]byte, 0, 300)
	buff4 = append(buff4, WriteTypePut)
	buff4 = codec.EncodeUvarint(buff4, ts)
	buff4 = append(buff4, flagShortValuePrefix)
	buff4 = append(buff4, byte(255)) // vlen=255
	largeValue := make([]byte, 255)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	buff4 = append(buff4, largeValue...) // Exactly 255 bytes as required

	v4 := new(RawWriteCFValue)
	err4 := v4.ParseFrom(buff4)
	require.NoError(t, err4)
	require.True(t, v4.HasShortValue())
	require.Equal(t, len(v4.GetShortValue()), 255)
	require.True(t, bytes.Equal(v4.GetShortValue(), largeValue))

	// Test case 5: Edge case with only 1 byte (flag), no vlen byte - should error
	buff5 := make([]byte, 0, 9)
	buff5 = append(buff5, WriteTypePut)
	buff5 = codec.EncodeUvarint(buff5, ts)
	buff5 = append(buff5, flagShortValuePrefix) // Only flag, no vlen byte following

	v5 := new(RawWriteCFValue)
	err5 := v5.ParseFrom(buff5)
	require.Error(t, err5)
	require.Contains(t, err5.Error(), "insufficient data for short value prefix")
	require.Contains(t, err5.Error(), "need at least 2 bytes but only have 1")
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
