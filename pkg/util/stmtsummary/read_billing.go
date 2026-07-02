// Copyright 2026 PingCAP, Inc.
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

package stmtsummary

import (
	"cmp"
	"slices"
)

const (
	// MaxReadBillingDemoBaseUnitKeysPerRecord bounds read billing dimensions per
	// statement summary digest/window record.
	MaxReadBillingDemoBaseUnitKeysPerRecord = 256
	// MaxReadBillingDemoStatusKeysPerRecord bounds non-reserved read billing
	// status dimensions per statement summary digest/window record.
	MaxReadBillingDemoStatusKeysPerRecord = 128
)

// Read billing demo column names used by INFORMATION_SCHEMA table definitions
// and statement-summary readers.
const (
	ReadBillingDemoModelVersionStr  = "MODEL_VERSION"
	ReadBillingDemoWeightVersionStr = "WEIGHT_VERSION"
	ReadBillingDemoSiteStr          = "SITE"
	ReadBillingDemoOpClassStr       = "OP_CLASS"
	ReadBillingDemoOperatorKindStr  = "OPERATOR_KIND"
	ReadBillingDemoUnitStr          = "UNIT"
	ReadBillingDemoInputSourceStr   = "INPUT_SOURCE"
	ReadBillingDemoInputSideStr     = "INPUT_SIDE"
	ReadBillingDemoRowWidthSource   = "ROW_WIDTH_SOURCE"
	ReadBillingDemoValueStr         = "VALUE"
	ReadBillingDemoSampleCountStr   = "SAMPLE_COUNT"
	ReadBillingDemoRowWidthSumStr   = "ROW_WIDTH_SUM"
	ReadBillingDemoAvgRowWidthStr   = "AVG_ROW_WIDTH"
	ReadBillingDemoStatusStr        = "STATUS"
	ReadBillingDemoReasonStr        = "REASON"
	ReadBillingDemoCountStr         = "COUNT"
)

const (
	readBillingDemoSiteStatement           = "statement"
	readBillingDemoOpClassStatement        = "statement"
	readBillingDemoOperatorStatement       = "statement"
	readBillingDemoStatusUnknownInput      = "unknown_input"
	readBillingDemoReasonAggregation       = "aggregation_overflow"
	readBillingDemoReasonStatusAggregation = "status_aggregation_overflow"
)

// ReadBillingDemoTableKind identifies which read billing demo table is being read.
type ReadBillingDemoTableKind int

const (
	// ReadBillingDemoTableBaseUnits expands coefficient-free base-unit aggregates.
	ReadBillingDemoTableBaseUnits ReadBillingDemoTableKind = iota
	// ReadBillingDemoTableStatus expands statement/operator status aggregates.
	ReadBillingDemoTableStatus
)

// ReadBillingDemoStatementStats is the structured read billing demo snapshot
// attached to one statement execution before statement summary aggregation.
type ReadBillingDemoStatementStats struct {
	ModelVersion  string
	WeightVersion string
	Statuses      []ReadBillingDemoStatusSample
	BaseUnits     []ReadBillingDemoBaseUnitSample
	Totals        ReadBillingDemoBaseUnitSummary
}

// IsEmpty returns whether the snapshot contains no read billing data.
func (s *ReadBillingDemoStatementStats) IsEmpty() bool {
	if s == nil {
		return true
	}
	return len(s.Statuses) == 0 && len(s.BaseUnits) == 0 &&
		s.Totals.SumReadBillingDemoFixedEvents == 0 &&
		s.Totals.SumReadBillingDemoInputRows == 0 &&
		s.Totals.SumReadBillingDemoInputBytes == 0
}

// ReadBillingDemoBaseUnitSample is a coefficient-free read billing unit sample.
type ReadBillingDemoBaseUnitSample struct {
	ModelVersion   string
	WeightVersion  string
	Site           string
	OpClass        string
	OperatorKind   string
	Unit           string
	InputSource    string
	InputSide      string
	RowWidthSource string
	Value          float64
	RowWidth       float64
}

// ReadBillingDemoStatusSample is a statement/operator read billing status sample.
type ReadBillingDemoStatusSample struct {
	ModelVersion  string
	WeightVersion string
	Site          string
	OpClass       string
	OperatorKind  string
	Status        string
	Reason        string
}

// ReadBillingDemoBaseUnitKey is the dimension key for statement-summary base-unit aggregation.
type ReadBillingDemoBaseUnitKey struct {
	ModelVersion   string
	WeightVersion  string
	Site           string
	OpClass        string
	OperatorKind   string
	Unit           string
	InputSource    string
	InputSide      string
	RowWidthSource string
}

// ReadBillingDemoBaseUnitAgg is the aggregate for one base-unit key.
type ReadBillingDemoBaseUnitAgg struct {
	Value       float64
	SampleCount uint64
	RowWidthSum float64
}

// ReadBillingDemoBaseUnitAggEntry is a JSON-stable base-unit aggregate entry.
type ReadBillingDemoBaseUnitAggEntry struct {
	ModelVersion   string  `json:"model_version"`
	WeightVersion  string  `json:"weight_version"`
	Site           string  `json:"site"`
	OpClass        string  `json:"op_class"`
	OperatorKind   string  `json:"operator_kind"`
	Unit           string  `json:"unit"`
	InputSource    string  `json:"input_source"`
	InputSide      string  `json:"input_side"`
	RowWidthSource string  `json:"row_width_source"`
	Value          float64 `json:"value"`
	SampleCount    uint64  `json:"sample_count"`
	RowWidthSum    float64 `json:"row_width_sum"`
}

// ReadBillingDemoStatusKey is the dimension key for statement-summary status aggregation.
type ReadBillingDemoStatusKey struct {
	ModelVersion  string
	WeightVersion string
	Site          string
	OpClass       string
	OperatorKind  string
	Status        string
	Reason        string
}

// ReadBillingDemoStatusAgg is the aggregate for one status key.
type ReadBillingDemoStatusAgg struct {
	Count uint64
}

// ReadBillingDemoStatusAggEntry is a JSON-stable status aggregate entry.
type ReadBillingDemoStatusAggEntry struct {
	ModelVersion  string `json:"model_version"`
	WeightVersion string `json:"weight_version"`
	Site          string `json:"site"`
	OpClass       string `json:"op_class"`
	OperatorKind  string `json:"operator_kind"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
	Count         uint64 `json:"count"`
}

func makeReadBillingDemoBaseUnitKey(sample ReadBillingDemoBaseUnitSample) ReadBillingDemoBaseUnitKey {
	return ReadBillingDemoBaseUnitKey{
		ModelVersion:   sample.ModelVersion,
		WeightVersion:  sample.WeightVersion,
		Site:           sample.Site,
		OpClass:        sample.OpClass,
		OperatorKind:   sample.OperatorKind,
		Unit:           sample.Unit,
		InputSource:    sample.InputSource,
		InputSide:      sample.InputSide,
		RowWidthSource: sample.RowWidthSource,
	}
}

func makeReadBillingDemoStatusKey(sample ReadBillingDemoStatusSample) ReadBillingDemoStatusKey {
	return ReadBillingDemoStatusKey{
		ModelVersion:  sample.ModelVersion,
		WeightVersion: sample.WeightVersion,
		Site:          sample.Site,
		OpClass:       sample.OpClass,
		OperatorKind:  sample.OperatorKind,
		Status:        sample.Status,
		Reason:        sample.Reason,
	}
}

func (e ReadBillingDemoBaseUnitAggEntry) key() ReadBillingDemoBaseUnitKey {
	return ReadBillingDemoBaseUnitKey{
		ModelVersion:   e.ModelVersion,
		WeightVersion:  e.WeightVersion,
		Site:           e.Site,
		OpClass:        e.OpClass,
		OperatorKind:   e.OperatorKind,
		Unit:           e.Unit,
		InputSource:    e.InputSource,
		InputSide:      e.InputSide,
		RowWidthSource: e.RowWidthSource,
	}
}

func (e ReadBillingDemoStatusAggEntry) key() ReadBillingDemoStatusKey {
	return ReadBillingDemoStatusKey{
		ModelVersion:  e.ModelVersion,
		WeightVersion: e.WeightVersion,
		Site:          e.Site,
		OpClass:       e.OpClass,
		OperatorKind:  e.OperatorKind,
		Status:        e.Status,
		Reason:        e.Reason,
	}
}

func (a *ReadBillingDemoBaseUnitAgg) addSample(sample ReadBillingDemoBaseUnitSample) {
	a.Value += sample.Value
	a.SampleCount++
	a.RowWidthSum += sample.RowWidth
}

func (a *ReadBillingDemoBaseUnitAgg) addEntry(entry ReadBillingDemoBaseUnitAggEntry) {
	a.Value += entry.Value
	a.SampleCount += entry.SampleCount
	a.RowWidthSum += entry.RowWidthSum
}

func (a *ReadBillingDemoStatusAgg) addSample() {
	a.Count++
}

func (a *ReadBillingDemoStatusAgg) addEntry(entry ReadBillingDemoStatusAggEntry) {
	a.Count += entry.Count
}

func readBillingDemoBaseUnitEntry(key ReadBillingDemoBaseUnitKey, agg ReadBillingDemoBaseUnitAgg) ReadBillingDemoBaseUnitAggEntry {
	return ReadBillingDemoBaseUnitAggEntry{
		ModelVersion:   key.ModelVersion,
		WeightVersion:  key.WeightVersion,
		Site:           key.Site,
		OpClass:        key.OpClass,
		OperatorKind:   key.OperatorKind,
		Unit:           key.Unit,
		InputSource:    key.InputSource,
		InputSide:      key.InputSide,
		RowWidthSource: key.RowWidthSource,
		Value:          agg.Value,
		SampleCount:    agg.SampleCount,
		RowWidthSum:    agg.RowWidthSum,
	}
}

func readBillingDemoStatusEntry(key ReadBillingDemoStatusKey, agg ReadBillingDemoStatusAgg) ReadBillingDemoStatusAggEntry {
	return ReadBillingDemoStatusAggEntry{
		ModelVersion:  key.ModelVersion,
		WeightVersion: key.WeightVersion,
		Site:          key.Site,
		OpClass:       key.OpClass,
		OperatorKind:  key.OperatorKind,
		Status:        key.Status,
		Reason:        key.Reason,
		Count:         agg.Count,
	}
}

// ReadBillingDemoBaseUnitEntriesFromMap converts map aggregates into deterministic entries.
func ReadBillingDemoBaseUnitEntriesFromMap(aggs map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg) []ReadBillingDemoBaseUnitAggEntry {
	entries := make([]ReadBillingDemoBaseUnitAggEntry, 0, len(aggs))
	for key, agg := range aggs {
		entries = append(entries, readBillingDemoBaseUnitEntry(key, agg))
	}
	sortReadBillingDemoBaseUnitEntries(entries)
	return entries
}

// ReadBillingDemoStatusEntriesFromMap converts map aggregates into deterministic entries.
func ReadBillingDemoStatusEntriesFromMap(aggs map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg) []ReadBillingDemoStatusAggEntry {
	entries := make([]ReadBillingDemoStatusAggEntry, 0, len(aggs))
	for key, agg := range aggs {
		entries = append(entries, readBillingDemoStatusEntry(key, agg))
	}
	sortReadBillingDemoStatusEntries(entries)
	return entries
}

// AddReadBillingDemoStatementStatsToMaps merges one statement snapshot into v1 maps.
func AddReadBillingDemoStatementStatsToMaps(
	baseUnitAggs map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg,
	statusAggs map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg,
	stats *ReadBillingDemoStatementStats,
) (map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg, map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg) {
	if stats == nil {
		return baseUnitAggs, statusAggs
	}
	for _, status := range stats.Statuses {
		statusAggs, _ = addReadBillingDemoStatusSampleToMap(statusAggs, status, false)
	}
	for _, sample := range stats.BaseUnits {
		var overflow bool
		baseUnitAggs, overflow = addReadBillingDemoBaseUnitSampleToMap(baseUnitAggs, sample)
		if overflow {
			statusAggs = addReadBillingDemoReservedStatusToMap(statusAggs, stats.ModelVersion, stats.WeightVersion, readBillingDemoReasonAggregation)
		}
	}
	return baseUnitAggs, statusAggs
}

// MergeReadBillingDemoAggMaps merges source maps into destination maps with the same caps used on write.
func MergeReadBillingDemoAggMaps(
	dstBase map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg,
	dstStatus map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg,
	srcBase map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg,
	srcStatus map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg,
) (map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg, map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg) {
	for key, agg := range srcStatus {
		dstStatus, _ = addReadBillingDemoStatusEntryToMap(dstStatus, readBillingDemoStatusEntry(key, agg), false)
	}
	for key, agg := range srcBase {
		var overflow bool
		dstBase, overflow = addReadBillingDemoBaseUnitEntryToMap(dstBase, readBillingDemoBaseUnitEntry(key, agg))
		if overflow {
			dstStatus = addReadBillingDemoReservedStatusToMap(dstStatus, key.ModelVersion, key.WeightVersion, readBillingDemoReasonAggregation)
		}
	}
	return dstBase, dstStatus
}

// AddReadBillingDemoStatementStatsToEntries merges one statement snapshot into v2 entries.
func AddReadBillingDemoStatementStatsToEntries(
	baseUnitEntries []ReadBillingDemoBaseUnitAggEntry,
	statusEntries []ReadBillingDemoStatusAggEntry,
	stats *ReadBillingDemoStatementStats,
) ([]ReadBillingDemoBaseUnitAggEntry, []ReadBillingDemoStatusAggEntry) {
	if stats == nil {
		return baseUnitEntries, statusEntries
	}
	for _, status := range stats.Statuses {
		statusEntries, _ = addReadBillingDemoStatusSampleToEntries(statusEntries, status, false)
	}
	for _, sample := range stats.BaseUnits {
		var overflow bool
		baseUnitEntries, overflow = addReadBillingDemoBaseUnitSampleToEntries(baseUnitEntries, sample)
		if overflow {
			statusEntries = addReadBillingDemoReservedStatusToEntries(statusEntries, stats.ModelVersion, stats.WeightVersion, readBillingDemoReasonAggregation)
		}
	}
	return baseUnitEntries, statusEntries
}

// MergeReadBillingDemoEntrySlices merges source entries into destination entries with caps.
func MergeReadBillingDemoEntrySlices(
	dstBase []ReadBillingDemoBaseUnitAggEntry,
	dstStatus []ReadBillingDemoStatusAggEntry,
	srcBase []ReadBillingDemoBaseUnitAggEntry,
	srcStatus []ReadBillingDemoStatusAggEntry,
) ([]ReadBillingDemoBaseUnitAggEntry, []ReadBillingDemoStatusAggEntry) {
	for _, entry := range srcStatus {
		dstStatus, _ = addReadBillingDemoStatusEntryToEntries(dstStatus, entry, false)
	}
	for _, entry := range srcBase {
		var overflow bool
		dstBase, overflow = addReadBillingDemoBaseUnitEntryToEntries(dstBase, entry)
		if overflow {
			dstStatus = addReadBillingDemoReservedStatusToEntries(dstStatus, entry.ModelVersion, entry.WeightVersion, readBillingDemoReasonAggregation)
		}
	}
	return dstBase, dstStatus
}

func addReadBillingDemoBaseUnitSampleToMap(
	aggs map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg,
	sample ReadBillingDemoBaseUnitSample,
) (map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg, bool) {
	key := makeReadBillingDemoBaseUnitKey(sample)
	if aggs == nil {
		aggs = make(map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg)
	}
	agg, exists := aggs[key]
	if !exists && len(aggs) >= MaxReadBillingDemoBaseUnitKeysPerRecord {
		return aggs, true
	}
	agg.addSample(sample)
	aggs[key] = agg
	return aggs, false
}

func addReadBillingDemoBaseUnitEntryToMap(
	aggs map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg,
	entry ReadBillingDemoBaseUnitAggEntry,
) (map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg, bool) {
	key := entry.key()
	if aggs == nil {
		aggs = make(map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg)
	}
	agg, exists := aggs[key]
	if !exists && len(aggs) >= MaxReadBillingDemoBaseUnitKeysPerRecord {
		return aggs, true
	}
	agg.addEntry(entry)
	aggs[key] = agg
	return aggs, false
}

func addReadBillingDemoStatusSampleToMap(
	aggs map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg,
	sample ReadBillingDemoStatusSample,
	reserved bool,
) (map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg, bool) {
	key := makeReadBillingDemoStatusKey(sample)
	if aggs == nil {
		aggs = make(map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg)
	}
	agg, exists := aggs[key]
	if !exists && !reserved && readBillingDemoNonReservedStatusKeyCount(aggs) >= MaxReadBillingDemoStatusKeysPerRecord {
		aggs = addReadBillingDemoReservedStatusToMap(aggs, sample.ModelVersion, sample.WeightVersion, readBillingDemoReasonStatusAggregation)
		return aggs, true
	}
	agg.addSample()
	aggs[key] = agg
	return aggs, false
}

func addReadBillingDemoStatusEntryToMap(
	aggs map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg,
	entry ReadBillingDemoStatusAggEntry,
	reserved bool,
) (map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg, bool) {
	key := entry.key()
	if aggs == nil {
		aggs = make(map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg)
	}
	agg, exists := aggs[key]
	if !exists && !reserved && readBillingDemoNonReservedStatusKeyCount(aggs) >= MaxReadBillingDemoStatusKeysPerRecord {
		aggs = addReadBillingDemoReservedStatusToMap(aggs, entry.ModelVersion, entry.WeightVersion, readBillingDemoReasonStatusAggregation)
		return aggs, true
	}
	agg.addEntry(entry)
	aggs[key] = agg
	return aggs, false
}

func addReadBillingDemoReservedStatusToMap(
	aggs map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg,
	modelVersion string,
	weightVersion string,
	reason string,
) map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg {
	sample := readBillingDemoReservedStatusSample(modelVersion, weightVersion, reason)
	aggs, _ = addReadBillingDemoStatusSampleToMap(aggs, sample, true)
	return aggs
}

func readBillingDemoNonReservedStatusKeyCount(aggs map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg) int {
	count := 0
	for key := range aggs {
		if key.Reason == readBillingDemoReasonAggregation || key.Reason == readBillingDemoReasonStatusAggregation {
			continue
		}
		count++
	}
	return count
}

func addReadBillingDemoBaseUnitSampleToEntries(
	entries []ReadBillingDemoBaseUnitAggEntry,
	sample ReadBillingDemoBaseUnitSample,
) ([]ReadBillingDemoBaseUnitAggEntry, bool) {
	key := makeReadBillingDemoBaseUnitKey(sample)
	for i := range entries {
		if entries[i].key() == key {
			entries[i].Value += sample.Value
			entries[i].SampleCount++
			entries[i].RowWidthSum += sample.RowWidth
			return entries, false
		}
	}
	if len(entries) >= MaxReadBillingDemoBaseUnitKeysPerRecord {
		return entries, true
	}
	entries = append(entries, readBillingDemoBaseUnitEntry(key, ReadBillingDemoBaseUnitAgg{
		Value:       sample.Value,
		SampleCount: 1,
		RowWidthSum: sample.RowWidth,
	}))
	sortReadBillingDemoBaseUnitEntries(entries)
	return entries, false
}

func addReadBillingDemoBaseUnitEntryToEntries(
	entries []ReadBillingDemoBaseUnitAggEntry,
	entry ReadBillingDemoBaseUnitAggEntry,
) ([]ReadBillingDemoBaseUnitAggEntry, bool) {
	key := entry.key()
	for i := range entries {
		if entries[i].key() == key {
			entries[i].Value += entry.Value
			entries[i].SampleCount += entry.SampleCount
			entries[i].RowWidthSum += entry.RowWidthSum
			return entries, false
		}
	}
	if len(entries) >= MaxReadBillingDemoBaseUnitKeysPerRecord {
		return entries, true
	}
	entries = append(entries, entry)
	sortReadBillingDemoBaseUnitEntries(entries)
	return entries, false
}

func addReadBillingDemoStatusSampleToEntries(
	entries []ReadBillingDemoStatusAggEntry,
	sample ReadBillingDemoStatusSample,
	reserved bool,
) ([]ReadBillingDemoStatusAggEntry, bool) {
	key := makeReadBillingDemoStatusKey(sample)
	for i := range entries {
		if entries[i].key() == key {
			entries[i].Count++
			return entries, false
		}
	}
	if !reserved && readBillingDemoNonReservedStatusEntryCount(entries) >= MaxReadBillingDemoStatusKeysPerRecord {
		entries = addReadBillingDemoReservedStatusToEntries(entries, sample.ModelVersion, sample.WeightVersion, readBillingDemoReasonStatusAggregation)
		return entries, true
	}
	entries = append(entries, readBillingDemoStatusEntry(key, ReadBillingDemoStatusAgg{Count: 1}))
	sortReadBillingDemoStatusEntries(entries)
	return entries, false
}

func addReadBillingDemoStatusEntryToEntries(
	entries []ReadBillingDemoStatusAggEntry,
	entry ReadBillingDemoStatusAggEntry,
	reserved bool,
) ([]ReadBillingDemoStatusAggEntry, bool) {
	key := entry.key()
	for i := range entries {
		if entries[i].key() == key {
			entries[i].Count += entry.Count
			return entries, false
		}
	}
	if !reserved && readBillingDemoNonReservedStatusEntryCount(entries) >= MaxReadBillingDemoStatusKeysPerRecord {
		entries = addReadBillingDemoReservedStatusToEntries(entries, entry.ModelVersion, entry.WeightVersion, readBillingDemoReasonStatusAggregation)
		return entries, true
	}
	entries = append(entries, entry)
	sortReadBillingDemoStatusEntries(entries)
	return entries, false
}

func addReadBillingDemoReservedStatusToEntries(
	entries []ReadBillingDemoStatusAggEntry,
	modelVersion string,
	weightVersion string,
	reason string,
) []ReadBillingDemoStatusAggEntry {
	sample := readBillingDemoReservedStatusSample(modelVersion, weightVersion, reason)
	entries, _ = addReadBillingDemoStatusSampleToEntries(entries, sample, true)
	return entries
}

func readBillingDemoReservedStatusSample(modelVersion string, weightVersion string, reason string) ReadBillingDemoStatusSample {
	return ReadBillingDemoStatusSample{
		ModelVersion:  modelVersion,
		WeightVersion: weightVersion,
		Site:          readBillingDemoSiteStatement,
		OpClass:       readBillingDemoOpClassStatement,
		OperatorKind:  readBillingDemoOperatorStatement,
		Status:        readBillingDemoStatusUnknownInput,
		Reason:        reason,
	}
}

func readBillingDemoNonReservedStatusEntryCount(entries []ReadBillingDemoStatusAggEntry) int {
	count := 0
	for _, entry := range entries {
		if entry.Reason == readBillingDemoReasonAggregation || entry.Reason == readBillingDemoReasonStatusAggregation {
			continue
		}
		count++
	}
	return count
}

func sortReadBillingDemoBaseUnitEntries(entries []ReadBillingDemoBaseUnitAggEntry) {
	slices.SortFunc(entries, func(i, j ReadBillingDemoBaseUnitAggEntry) int {
		return cmp.Or(
			cmp.Compare(i.ModelVersion, j.ModelVersion),
			cmp.Compare(i.WeightVersion, j.WeightVersion),
			cmp.Compare(i.Site, j.Site),
			cmp.Compare(i.OpClass, j.OpClass),
			cmp.Compare(i.OperatorKind, j.OperatorKind),
			cmp.Compare(i.Unit, j.Unit),
			cmp.Compare(i.InputSource, j.InputSource),
			cmp.Compare(i.InputSide, j.InputSide),
			cmp.Compare(i.RowWidthSource, j.RowWidthSource),
		)
	})
}

func sortReadBillingDemoStatusEntries(entries []ReadBillingDemoStatusAggEntry) {
	slices.SortFunc(entries, func(i, j ReadBillingDemoStatusAggEntry) int {
		return cmp.Or(
			cmp.Compare(i.ModelVersion, j.ModelVersion),
			cmp.Compare(i.WeightVersion, j.WeightVersion),
			cmp.Compare(i.Site, j.Site),
			cmp.Compare(i.OpClass, j.OpClass),
			cmp.Compare(i.OperatorKind, j.OperatorKind),
			cmp.Compare(i.Status, j.Status),
			cmp.Compare(i.Reason, j.Reason),
		)
	})
}
