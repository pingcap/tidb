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

package meta

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/dxf/framework/schstatus"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/partialjson"
	"github.com/pingcap/tidb/pkg/util/set"
)

// DDL job structure
//	DDLJobHistory: hash
//
// for multi DDL workers, only one can become the owner
// to operate DDL jobs, and dispatch them to MR Jobs.

var (
	mDDLJobHistoryKey = []byte("DDLJobHistory")
)

func (*Mutator) jobIDKey(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

func (m *Mutator) addHistoryDDLJob(key []byte, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err == nil {
		err = m.txn.HSet(key, m.jobIDKey(job.ID), b)
	}
	return errors.Trace(err)
}

// AddHistoryDDLJob adds DDL job to history.
func (m *Mutator) AddHistoryDDLJob(job *model.Job, updateRawArgs bool) error {
	return m.addHistoryDDLJob(mDDLJobHistoryKey, job, updateRawArgs)
}

func (m *Mutator) getHistoryDDLJob(key []byte, id int64) (*model.Job, error) {
	value, err := m.txn.HGet(key, m.jobIDKey(id))
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{}
	err = job.Decode(value)
	return job, errors.Trace(err)
}

// GetHistoryDDLJob gets a history DDL job.
func (m *Mutator) GetHistoryDDLJob(id int64) (*model.Job, error) {
	startTime := time.Now()
	job, err := m.getHistoryDDLJob(mDDLJobHistoryKey, id)
	metrics.MetaHistogram.WithLabelValues(metrics.GetHistoryDDLJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return job, errors.Trace(err)
}

// GetHistoryDDLCount the count of all history DDL jobs.
func (m *Mutator) GetHistoryDDLCount() (uint64, error) {
	return m.txn.HGetLen(mDDLJobHistoryKey)
}

// SetIngestMaxBatchSplitRanges sets the ingest max_batch_split_ranges.
func (m *Mutator) SetIngestMaxBatchSplitRanges(val int) error {
	return errors.Trace(m.txn.Set(mIngestMaxBatchSplitRangesKey, []byte(strconv.Itoa(val))))
}

// GetIngestMaxBatchSplitRanges gets the ingest max_batch_split_ranges.
func (m *Mutator) GetIngestMaxBatchSplitRanges() (val int, isNull bool, err error) {
	sVal, err := m.txn.Get(mIngestMaxBatchSplitRangesKey)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if sVal == nil {
		return 0, true, nil
	}
	val, err = strconv.Atoi(string(sVal))
	return val, false, errors.Trace(err)
}

// SetIngestMaxSplitRangesPerSec sets the max_split_ranges_per_sec.
func (m *Mutator) SetIngestMaxSplitRangesPerSec(val float64) error {
	return errors.Trace(m.txn.Set(mIngestMaxSplitRangesPerSecKey, []byte(strconv.FormatFloat(val, 'f', 2, 64))))
}

// GetIngestMaxSplitRangesPerSec gets the max_split_ranges_per_sec.
func (m *Mutator) GetIngestMaxSplitRangesPerSec() (val float64, isNull bool, err error) {
	sVal, err := m.txn.Get(mIngestMaxSplitRangesPerSecKey)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if sVal == nil {
		return 0, true, nil
	}
	val, err = strconv.ParseFloat(string(sVal), 64)
	return val, false, errors.Trace(err)
}

// SetIngestMaxInflight sets the max_ingest_concurrency.
func (m *Mutator) SetIngestMaxInflight(val int) error {
	return errors.Trace(m.txn.Set(mIngestMaxInflightKey, []byte(strconv.Itoa(val))))
}

// GetIngestMaxInflight gets the max_ingest_concurrency.
func (m *Mutator) GetIngestMaxInflight() (val int, isNull bool, err error) {
	sVal, err := m.txn.Get(mIngestMaxInflightKey)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if sVal == nil {
		return 0, true, nil
	}
	val, err = strconv.Atoi(string(sVal))
	return val, false, errors.Trace(err)
}

// SetIngestMaxPerSec sets the max_ingest_per_sec.
func (m *Mutator) SetIngestMaxPerSec(val float64) error {
	return errors.Trace(m.txn.Set(mIngestMaxPerSecKey, []byte(strconv.FormatFloat(val, 'f', 2, 64))))
}

// GetIngestMaxPerSec gets the max_ingest_per_sec.
func (m *Mutator) GetIngestMaxPerSec() (val float64, isNull bool, err error) {
	sVal, err := m.txn.Get(mIngestMaxPerSecKey)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if sVal == nil {
		return 0, true, nil
	}
	val, err = strconv.ParseFloat(string(sVal), 64)
	return val, false, errors.Trace(err)
}

// LastJobIterator is the iterator for gets latest history.
type LastJobIterator interface {
	GetLastJobs(num int, jobs []*model.Job) ([]*model.Job, error)
}

// GetLastHistoryDDLJobsIterator gets latest history ddl jobs iterator.
func (m *Mutator) GetLastHistoryDDLJobsIterator() (LastJobIterator, error) {
	iter, err := structure.NewHashReverseIter(m.txn, mDDLJobHistoryKey)
	if err != nil {
		return nil, err
	}
	return &HLastJobIterator{
		iter: iter,
	}, nil
}

// GetLastHistoryDDLJobsIteratorWithFilter returns a iterator for getting latest history ddl jobs.
// This iterator will also filter jobs using given schemaNames and tableNames
func (m *Mutator) GetLastHistoryDDLJobsIteratorWithFilter(
	schemaNames set.StringSet,
	tableNames set.StringSet,
) (LastJobIterator, error) {
	iter, err := structure.NewHashReverseIter(m.txn, mDDLJobHistoryKey)
	if err != nil {
		return nil, err
	}
	return &HLastJobIterator{
		iter:        iter,
		schemaNames: schemaNames,
		tableNames:  tableNames,
	}, nil
}

// GetHistoryDDLJobsIterator gets the jobs iterator begin with startJobID.
func (m *Mutator) GetHistoryDDLJobsIterator(startJobID int64) (LastJobIterator, error) {
	field := m.jobIDKey(startJobID)
	iter, err := structure.NewHashReverseIterBeginWithField(m.txn, mDDLJobHistoryKey, field)
	if err != nil {
		return nil, err
	}
	return &HLastJobIterator{
		iter: iter,
	}, nil
}

// SetDXFScheduleTuneFactors sets the DXF schedule TTL tune factors for a keyspace.
func (m *Mutator) SetDXFScheduleTuneFactors(keyspace string, factors *schstatus.TTLTuneFactors) error {
	data, err := json.Marshal(factors)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(m.txn.HSet(mDXFScheduleTuneKey, []byte(keyspace), data))
}

// GetDXFScheduleTuneFactors gets the DXF schedule TTL tune factors for a keyspace.
func (m *Mutator) GetDXFScheduleTuneFactors(keyspace string) (*schstatus.TTLTuneFactors, error) {
	data, err := m.txn.HGet(mDXFScheduleTuneKey, []byte(keyspace))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if data == nil {
		return nil, nil
	}
	res := &schstatus.TTLTuneFactors{}
	err = json.Unmarshal(data, res)
	return res, errors.Trace(err)
}

// HLastJobIterator is the iterator for gets the latest history.
type HLastJobIterator struct {
	iter        *structure.ReverseHashIterator
	schemaNames set.StringSet
	tableNames  set.StringSet
}

var jobExtractFields = []string{"schema_name", "table_name"}

// ExtractSchemaAndTableNameFromJob extract schema_name and table_name from encoded Job structure
// Note, here we strongly rely on the order of fields in marshalled string, just like checkSubstringsInOrder
// Exported for test
func ExtractSchemaAndTableNameFromJob(data []byte) (schemaName, tableName string, err error) {
	m, err := partialjson.ExtractTopLevelMembers(data, jobExtractFields)

	schemaNameToken, ok := m["schema_name"]
	if !ok || len(schemaNameToken) != 1 {
		return "", "", errors.New("name field not found in JSON")
	}
	schemaName, ok = schemaNameToken[0].(string)
	if !ok {
		return "", "", errors.Errorf("unexpected name field in JSON, %v", schemaNameToken)
	}

	tableNameToken, ok := m["table_name"]
	if !ok || len(tableNameToken) != 1 {
		return "", "", errors.New("name field not found in JSON")
	}
	tableName, ok = tableNameToken[0].(string)
	if !ok {
		return "", "", errors.Errorf("unexpected name field in JSON, %v", tableNameToken)
	}
	return
}

// IsJobMatch examines whether given job's table/schema name matches.
func IsJobMatch(job []byte, schemaNames, tableNames set.StringSet) (match bool, err error) {
	if schemaNames.Count() == 0 && tableNames.Count() == 0 {
		return true, nil
	}
	schemaName, tableName, err := ExtractSchemaAndTableNameFromJob(job)
	if err != nil {
		return
	}
	if (schemaNames.Count() == 0 || schemaNames.Exist(schemaName)) &&
		tableNames.Count() == 0 || tableNames.Exist(tableName) {
		match = true
	}
	return
}

// GetLastJobs gets last several jobs.
func (i *HLastJobIterator) GetLastJobs(num int, jobs []*model.Job) ([]*model.Job, error) {
	if len(jobs) < num {
		jobs = make([]*model.Job, 0, num)
	}
	jobs = jobs[:0]
	iter := i.iter
	for iter.Valid() && len(jobs) < num {
		match, err := IsJobMatch(iter.Value(), i.schemaNames, i.tableNames)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if !match {
			err := iter.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}

		job := &model.Job{}
		err = job.Decode(iter.Value())
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
		err = iter.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return jobs, nil
}

// GetBootstrapVersion returns the version of the server which bootstrap the store.
// If the store is not bootstraped, the version will be zero.
func (m *Mutator) GetBootstrapVersion() (int64, error) {
	value, err := m.txn.GetInt64(mBootstrapKey)
	return value, errors.Trace(err)
}

// FinishBootstrap finishes bootstrap.
func (m *Mutator) FinishBootstrap(version int64) error {
	err := m.txn.Set(mBootstrapKey, []byte(strconv.FormatInt(version, 10)))
	return errors.Trace(err)
}

// ElementKeyType is a key type of the element.
type ElementKeyType []byte

var (
	// ColumnElementKey is the key for column element.
	ColumnElementKey ElementKeyType = []byte("_col_")
	// IndexElementKey is the key for index element.
	IndexElementKey ElementKeyType = []byte("_idx_")
)

const elementKeyLen = 5

// Element has the information of the backfill job's type and ID.
type Element struct {
	ID      int64
	TypeKey []byte
}

// String defines a Stringer function for debugging and pretty printing.
func (e *Element) String() string {
	return "ID:" + strconv.FormatInt(e.ID, 10) + "," +
		"TypeKey:" + string(e.TypeKey)
}

// EncodeElement encodes an Element into a byte slice.
// It's exported for testing.
func (e *Element) EncodeElement() []byte {
	b := make([]byte, 13)
	copy(b[:elementKeyLen], e.TypeKey)
	binary.BigEndian.PutUint64(b[elementKeyLen:], uint64(e.ID))
	return b
}

// DecodeElement decodes values from a byte slice generated with an element.
// It's exported for testing.
func DecodeElement(b []byte) (*Element, error) {
	if len(b) < elementKeyLen+8 {
		return nil, errors.Errorf("invalid encoded element %q length %d", b, len(b))
	}

	var tp []byte
	prefix := b[:elementKeyLen]
	b = b[elementKeyLen:]
	switch string(prefix) {
	case string(IndexElementKey):
		tp = IndexElementKey
	case string(ColumnElementKey):
		tp = ColumnElementKey
	default:
		return nil, errors.Errorf("invalid encoded element key prefix %q", prefix)
	}

	id := binary.BigEndian.Uint64(b)
	return &Element{ID: int64(id), TypeKey: tp}, nil
}

func (*Mutator) schemaDiffKey(schemaVersion int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mSchemaDiffPrefix, schemaVersion)
}

// GetSchemaDiff gets the modification information on a given schema version.
func (m *Mutator) GetSchemaDiff(schemaVersion int64) (*model.SchemaDiff, error) {
	diffKey := m.schemaDiffKey(schemaVersion)
	startTime := time.Now()
	data, err := m.txn.Get(diffKey)
	metrics.MetaHistogram.WithLabelValues(metrics.GetSchemaDiff, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil || len(data) == 0 {
		return nil, errors.Trace(err)
	}
	diff := &model.SchemaDiff{}
	err = json.Unmarshal(data, diff)
	return diff, errors.Trace(err)
}

// SetSchemaDiff sets the modification information on a given schema version.
func (m *Mutator) SetSchemaDiff(diff *model.SchemaDiff) error {
	data, err := json.Marshal(diff)
	if err != nil {
		return errors.Trace(err)
	}
	diffKey := m.schemaDiffKey(diff.Version)
	startTime := time.Now()
	err = m.txn.Set(diffKey, data)
	metrics.MetaHistogram.WithLabelValues(metrics.SetSchemaDiff, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// GroupRUStats keeps the ru consumption statistics data.
type GroupRUStats struct {
	ID            int64             `json:"id"`
	Name          string            `json:"name"`
	RUConsumption *rmpb.Consumption `json:"ru_consumption"`
}

// DailyRUStats keeps all the ru consumption statistics data.
type DailyRUStats struct {
	EndTime time.Time      `json:"date"`
	Stats   []GroupRUStats `json:"stats"`
}

// RUStats keeps the lastest and second lastest DailyRUStats data.
type RUStats struct {
	Latest   *DailyRUStats `json:"latest"`
	Previous *DailyRUStats `json:"previous"`
}

// GetRUStats load the persisted RUStats data.
func (m *Mutator) GetRUStats() (*RUStats, error) {
	data, err := m.txn.Get(mRequestUnitStats)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var ruStats *RUStats
	if data != nil {
		ruStats = &RUStats{}
		if err = json.Unmarshal(data, &ruStats); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return ruStats, nil
}

// SetRUStats persist new ru stats data to meta storage.
func (m *Mutator) SetRUStats(stats *RUStats) error {
	data, err := json.Marshal(stats)
	if err != nil {
		return errors.Trace(err)
	}

	err = m.txn.Set(mRequestUnitStats, data)
	return errors.Trace(err)
}

// GetOldestSchemaVersion gets the oldest schema version at the GC safe point.
// It works by checking the MVCC information (internal txn API) of the schema version meta key.
// This function is only used by infoschema v2 currently.
func GetOldestSchemaVersion(h *helper.Helper) (int64, error) {
	ek := make([]byte, 0, len(mMetaPrefix)+len(mSchemaVersionKey)+24)
	ek = append(ek, mMetaPrefix...)
	ek = codec.EncodeBytes(ek, mSchemaVersionKey)
	key := codec.EncodeUint(ek, uint64(structure.StringData))
	mvccResp, err := h.GetMvccByEncodedKeyWithTS(key, math.MaxUint64)
	if err != nil {
		return 0, err
	}
	if mvccResp == nil || mvccResp.Info == nil || len(mvccResp.Info.Writes) == 0 {
		return 0, errors.Errorf("There is no Write MVCC info for the schema version key")
	}

	v := mvccResp.Info.Writes[len(mvccResp.Info.Writes)-1]
	var n int64
	n, err = strconv.ParseInt(string(v.ShortValue), 10, 64)
	return n, errors.Trace(err)
}
