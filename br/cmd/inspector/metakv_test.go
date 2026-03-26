// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

// encodeTxnMetaKey builds a TxnKey from raw hash key, field, and timestamp —
// the same encoding used by TiDB when writing meta KV entries to TiKV.
func encodeTxnMetaKey(key []byte, field []byte, ts uint64) []byte {
	k := tablecodec.EncodeMetaKey(key, field)
	txnKey := codec.EncodeBytes(nil, k)
	return codec.EncodeUintDesc(txnKey, ts)
}

// mDBs is the outer hash key that holds all database definitions.
var mDBs = []byte("DBs")

func TestClassifyFieldTable(t *testing.T) {
	field := meta.TableKey(42)
	kt, tableID := classifyField(field)
	require.Equal(t, keyTypeTable, kt)
	require.Equal(t, int64(42), tableID)
}

func TestClassifyFieldIID(t *testing.T) {
	field := meta.AutoIncrementIDKey(99)
	kt, tableID := classifyField(field)
	require.Equal(t, keyTypeIID, kt)
	require.Equal(t, int64(99), tableID)
}

func TestClassifyFieldTID(t *testing.T) {
	field := meta.AutoTableIDKey(7)
	kt, tableID := classifyField(field)
	require.Equal(t, keyTypeTID, kt)
	require.Equal(t, int64(7), tableID)
}

func TestClassifyFieldSID(t *testing.T) {
	field := meta.SequenceKey(13)
	kt, tableID := classifyField(field)
	require.Equal(t, keyTypeSID, kt)
	require.Equal(t, int64(13), tableID)
}

func TestClassifyFieldTARID(t *testing.T) {
	field := meta.AutoRandomTableIDKey(55)
	kt, tableID := classifyField(field)
	require.Equal(t, keyTypeTARID, kt)
	require.Equal(t, int64(55), tableID)
}

func TestClassifyFieldDBKey(t *testing.T) {
	field := meta.DBkey(3)
	kt, tableID := classifyField(field)
	// DBkey fields are not a table-level field; classifyField returns unknown for them
	// because classifyField only handles table-level fields.
	require.Equal(t, keyTypeUnknown, kt)
	require.Equal(t, int64(0), tableID)
}

func TestClassifyFieldUnknown(t *testing.T) {
	field := []byte("UnknownField:42")
	kt, tableID := classifyField(field)
	require.Equal(t, keyTypeUnknown, kt)
	require.Equal(t, int64(0), tableID)
}

// TestClassifyEntryDBDefinition verifies that an mDBs/DB:{id} TxnKey is
// classified as catDB with keyTypeDB and the correct dbID.
func TestClassifyEntryDBDefinition(t *testing.T) {
	const (
		dbID int64  = 5
		ts   uint64 = 400036290571534337
	)
	txnKey := encodeTxnMetaKey(mDBs, meta.DBkey(dbID), ts)
	cat, dbk, lk := classifyEntry(txnKey, "default")

	require.Equal(t, catDB, cat)
	require.NotNil(t, dbk)
	require.Equal(t, dbID, dbk.dbID)
	require.Equal(t, int64(0), dbk.tableID)
	require.Equal(t, keyTypeDB, dbk.field)
	require.Equal(t, "default", dbk.cf)
	// logical key is TxnKey with last 8 bytes (TS) stripped
	require.Equal(t, string(txnKey[:len(txnKey)-8]), lk)
}

// TestClassifyEntryTableSchema verifies that a DB:{id}/Table:{id} TxnKey
// is classified as catDB with keyTypeTable.
func TestClassifyEntryTableSchema(t *testing.T) {
	const (
		dbID    int64  = 1
		tableID int64  = 57
		ts      uint64 = 400036290571534337
	)
	txnKey := encodeTxnMetaKey(meta.DBkey(dbID), meta.TableKey(tableID), ts)
	cat, dbk, _ := classifyEntry(txnKey, "write")

	require.Equal(t, catDB, cat)
	require.NotNil(t, dbk)
	require.Equal(t, dbID, dbk.dbID)
	require.Equal(t, tableID, dbk.tableID)
	require.Equal(t, keyTypeTable, dbk.field)
	require.Equal(t, "write", dbk.cf)
}

// TestClassifyEntryAutoIncrement verifies IID entry classification.
func TestClassifyEntryAutoIncrement(t *testing.T) {
	const (
		dbID    int64  = 2
		tableID int64  = 100
		ts      uint64 = 400036290571534337
	)
	txnKey := encodeTxnMetaKey(meta.DBkey(dbID), meta.AutoIncrementIDKey(tableID), ts)
	cat, dbk, _ := classifyEntry(txnKey, "default")

	require.Equal(t, catDB, cat)
	require.NotNil(t, dbk)
	require.Equal(t, dbID, dbk.dbID)
	require.Equal(t, tableID, dbk.tableID)
	require.Equal(t, keyTypeIID, dbk.field)
}

// TestClassifyEntryAutoTableID verifies TID entry classification.
func TestClassifyEntryAutoTableID(t *testing.T) {
	txnKey := encodeTxnMetaKey(meta.DBkey(3), meta.AutoTableIDKey(200), 1000)
	cat, dbk, _ := classifyEntry(txnKey, "default")
	require.Equal(t, catDB, cat)
	require.NotNil(t, dbk)
	require.Equal(t, keyTypeTID, dbk.field)
	require.Equal(t, int64(200), dbk.tableID)
}

// TestClassifyEntryAutoRandom verifies TARID entry classification.
func TestClassifyEntryAutoRandom(t *testing.T) {
	txnKey := encodeTxnMetaKey(meta.DBkey(4), meta.AutoRandomTableIDKey(300), 2000)
	cat, dbk, _ := classifyEntry(txnKey, "write")
	require.Equal(t, catDB, cat)
	require.NotNil(t, dbk)
	require.Equal(t, keyTypeTARID, dbk.field)
	require.Equal(t, int64(300), dbk.tableID)
}

// TestClassifyEntryDDLHistory verifies that mDDLJobHistory keys map to catDDLHistory.
func TestClassifyEntryDDLHistory(t *testing.T) {
	// The TiDB meta layer stores DDL history under hash key "DDLJobHistory".
	// EncodeMetaKey prepends "m", producing a TxnKey that starts with "mDDLJobH",
	// which IsMetaDDLJobHistoryKey recognises.
	ddlHistKey := []byte("DDLJobHistory")
	txnKey := encodeTxnMetaKey(ddlHistKey, []byte("job:1"), 5000)
	cat, dbk, _ := classifyEntry(txnKey, "default")
	require.Equal(t, catDDLHistory, cat)
	require.Nil(t, dbk)
}

// TestClassifyEntryOther verifies that non-meta keys fall into catOther.
func TestClassifyEntryOther(t *testing.T) {
	// A random non-meta key that doesn't start with "mDB" or "mDDLJobH"
	txnKey := encodeTxnMetaKey([]byte("SomeOtherKey"), []byte("field"), 9000)
	cat, dbk, _ := classifyEntry(txnKey, "default")
	require.Equal(t, catOther, cat)
	require.Nil(t, dbk)
}

// TestFmtInt verifies comma-separated integer formatting.
func TestFmtInt(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{999, "999"},
		{1000, "1,000"},
		{1234, "1,234"},
		{1000000, "1,000,000"},
		{12345678, "12,345,678"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, fmtInt(tc.n), "fmtInt(%d)", tc.n)
	}
}

// TestAccumulateConcurrency verifies that concurrent accumulate calls produce
// consistent totals without data races.
func TestAccumulateConcurrency(t *testing.T) {
	const goroutines = 32
	const callsEach = 1000

	stats := newMetaKVStats()
	txnKey := encodeTxnMetaKey(meta.DBkey(1), meta.AutoIncrementIDKey(42), 1000)

	done := make(chan struct{})
	for range goroutines {
		go func() {
			for range callsEach {
				stats.accumulate(txnKey, []byte("val"), "default")
			}
			done <- struct{}{}
		}()
	}
	for range goroutines {
		<-done
	}

	stats.mu.Lock()
	defer stats.mu.Unlock()
	total := stats.topLevel[catDB]["default"].count
	require.Equal(t, int64(goroutines*callsEach), total)
}
