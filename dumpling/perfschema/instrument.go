// Copyright 2016 PingCAP, Inc.
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

package perfschema

import (
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
	"github.com/syndtr/goleveldb/leveldb"
)

// EnumCallerName is used as a parameter to avoid calling runtime.Caller(1) since
// it is too expensive (500ns+ per call), we don't want to invoke it repeatedly for
// each instrument.
type EnumCallerName int

const (
	// CallerNameSessionExecute is for session.go:Execute() method.
	CallerNameSessionExecute EnumCallerName = iota + 1
)

const (
	stageInstrumentPrefix       = "stage/"
	statementInstrumentPrefix   = "statement/"
	transactionInstrumentPrefix = "transaction"
)

// Flag indicators for table setup_timers.
const (
	flagStage = iota + 1
	flagStatement
	flagTransaction
)

type enumTimerName int

// Enum values for the TIMER_NAME columns.
// This enum is found in the following tables:
// - performance_schema.setup_timer (TIMER_NAME)
const (
	timerNameNone enumTimerName = iota
	timerNameNanosec
	timerNameMicrosec
	timerNameMillisec
)

var (
	callerNames = make(map[EnumCallerName]string)
)

// addInstrument is used to add an item to setup_instruments table.
func (ps *perfSchema) addInstrument(name string) (uint64, error) {
	store := ps.stores[TableSetupInstruments]
	lastLsn := atomic.AddUint64(ps.lsns[TableSetupInstruments], 1)

	batch := pool.Get().(*leveldb.Batch)
	defer func() {
		batch.Reset()
		pool.Put(batch)
	}()

	record := []interface{}{name, mysql.Enum{Name: "YES", Value: 1}, mysql.Enum{Name: "YES", Value: 1}}
	lsn := lastLsn - 1
	rawKey := []interface{}{uint64(lsn)}
	key, err := codec.EncodeKey(nil, rawKey...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	val, err := codec.EncodeValue(nil, record...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	batch.Put(key, val)

	err = store.Write(batch, nil)
	return lsn, errors.Trace(err)
}

func (ps *perfSchema) getTimerName(flag int) (enumTimerName, error) {
	store := ps.stores[TableSetupTimers]

	rawKey := []interface{}{uint64(flag)}
	key, err := codec.EncodeKey(nil, rawKey...)
	if err != nil {
		return timerNameNone, errors.Trace(err)
	}

	val, err := store.Get(key, nil)
	if err != nil {
		return timerNameNone, errors.Trace(err)
	}

	record, err := decodeValue(val, ps.tables[TableSetupTimers].Columns)
	if err != nil {
		return timerNameNone, errors.Trace(err)
	}

	timerName, ok := record[1].(string)
	if !ok {
		return timerNameNone, errors.New("Timer type does not match")
	}
	switch timerName {
	case "NANOSECOND":
		return timerNameNanosec, nil
	case "MICROSECOND":
		return timerNameMicrosec, nil
	case "MILLISECOND":
		return timerNameMillisec, nil
	}
	return timerNameNone, nil
}
