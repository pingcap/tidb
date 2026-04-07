// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/spf13/cobra"
)

const flagTop = "top"

// keyType classifies the Field component of a meta key.
type keyType int

const (
	keyTypeDB      keyType = iota // DB definition: field = "DB:{dbID}" inside the mDBs hash
	keyTypeTable                  // Table schema: field = "Table:{tableID}"
	keyTypeIID                    // Auto-increment counter: field = "IID:{tableID}"
	keyTypeTID                    // Auto table ID: field = "TID:{tableID}"
	keyTypeSID                    // Sequence counter: field = "SID:{tableID}"
	keyTypeTARID                  // Auto-random counter: field = "TARID:{tableID}"
	keyTypeUnknown                // Unrecognized mDB field
)

func (k keyType) String() string {
	switch k {
	case keyTypeDB:
		return "DB"
	case keyTypeTable:
		return "Table"
	case keyTypeIID:
		return "IID"
	case keyTypeTID:
		return "TID"
	case keyTypeSID:
		return "SID"
	case keyTypeTARID:
		return "TARID"
	default:
		return "unknown"
	}
}

// topLevelCategory classifies the outer key prefix.
type topLevelCategory int

const (
	catDB         topLevelCategory = iota // mDB:* keys
	catDDLHistory                         // mDDLJobHistory:* keys
	catOther                              // any other key
)

func (c topLevelCategory) String() string {
	switch c {
	case catDB:
		return "mDB"
	case catDDLHistory:
		return "mDDLJobHistory"
	default:
		return "other"
	}
}

type entryStats struct {
	count    int64
	keyBytes int64
	valBytes int64
}

func (s *entryStats) add(keyLen, valLen int) {
	s.count++
	s.keyBytes += int64(keyLen)
	s.valBytes += int64(valLen)
}

// dbTableKey identifies a specific (dbID, tableID, fieldType, cf) combination.
type dbTableKey struct {
	dbID    int64
	tableID int64 // 0 for DB-level keys (keyTypeDB)
	field   keyType
	cf      string
}

type metaKVStats struct {
	mu sync.Mutex

	// top-level breakdown: category -> cf -> stats
	topLevel map[topLevelCategory]map[string]*entryStats

	// mDB detailed breakdown per (dbID, tableID, fieldType, cf)
	dbDetail map[dbTableKey]*entryStats

	// unique key tracking: category -> cf -> set of logical keys (TS stripped)
	// Used to compute avg MVCC versions per logical key.
	uniqueKeys map[topLevelCategory]map[string]map[string]struct{}

	// counters
	filesProcessed int64
	parseErrors    int64
}

func newMetaKVStats() *metaKVStats {
	s := &metaKVStats{
		topLevel:   make(map[topLevelCategory]map[string]*entryStats),
		dbDetail:   make(map[dbTableKey]*entryStats),
		uniqueKeys: make(map[topLevelCategory]map[string]map[string]struct{}),
	}
	for _, cat := range []topLevelCategory{catDB, catDDLHistory, catOther} {
		s.topLevel[cat] = make(map[string]*entryStats)
		s.uniqueKeys[cat] = make(map[string]map[string]struct{})
	}
	return s
}

// accumulate adds a single KV entry to the stats.
func (s *metaKVStats) accumulate(key, value []byte, cf string) {
	cat, dbk, lk := classifyEntry(key, cf)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Top-level stats
	if _, ok := s.topLevel[cat][cf]; !ok {
		s.topLevel[cat][cf] = &entryStats{}
	}
	s.topLevel[cat][cf].add(len(key), len(value))

	// Unique key tracking
	if _, ok := s.uniqueKeys[cat][cf]; !ok {
		s.uniqueKeys[cat][cf] = make(map[string]struct{})
	}
	s.uniqueKeys[cat][cf][lk] = struct{}{}

	// DB detail stats
	if cat == catDB && dbk != nil {
		if _, ok := s.dbDetail[*dbk]; !ok {
			s.dbDetail[*dbk] = &entryStats{}
		}
		s.dbDetail[*dbk].add(len(key), len(value))
	}
}

// logicalKeyOf strips the 8-byte TS suffix from a TxnKey.
func logicalKeyOf(key []byte) string {
	if len(key) >= 8 {
		return string(key[:len(key)-8])
	}
	return string(key)
}

// classifyEntry determines the category, optional dbTableKey, and logical key for a TxnKey.
func classifyEntry(key []byte, cf string) (topLevelCategory, *dbTableKey, string) {
	lk := logicalKeyOf(key)

	if utils.IsMetaDDLJobHistoryKey(key) {
		return catDDLHistory, nil, lk
	}
	if !utils.IsMetaDBKey(key) {
		return catOther, nil, lk
	}

	rawKey, err := stream.ParseTxnMetaKeyFrom(key)
	if err != nil {
		return catDB, nil, lk
	}

	var dbk dbTableKey
	dbk.cf = cf

	if meta.IsDBkey(rawKey.Field) {
		// DB definition entry: hash key = "DBs", field = "DB:{dbID}"
		dbID, parseErr := meta.ParseDBKey(rawKey.Field)
		if parseErr != nil {
			return catDB, nil, lk
		}
		dbk.dbID = dbID
		dbk.tableID = 0
		dbk.field = keyTypeDB
		return catDB, &dbk, lk
	}

	// Table-level entry: hash key = "DB:{dbID}", field = "Table:{id}" / "IID:{id}" / etc.
	if !meta.IsDBkey(rawKey.Key) {
		return catDB, nil, lk
	}
	dbID, parseErr := meta.ParseDBKey(rawKey.Key)
	if parseErr != nil {
		return catDB, nil, lk
	}
	dbk.dbID = dbID
	dbk.field, dbk.tableID = classifyField(rawKey.Field)
	return catDB, &dbk, lk
}

// classifyField classifies the field component of a meta key.
func classifyField(field []byte) (keyType, int64) {
	if meta.IsTableKey(field) {
		if id, err := meta.ParseTableKey(field); err == nil {
			return keyTypeTable, id
		}
	}
	if meta.IsAutoIncrementIDKey(field) {
		if id, err := meta.ParseAutoIncrementIDKey(field); err == nil {
			return keyTypeIID, id
		}
	}
	if meta.IsAutoTableIDKey(field) {
		if id, err := meta.ParseAutoTableIDKey(field); err == nil {
			return keyTypeTID, id
		}
	}
	if meta.IsSequenceKey(field) {
		if id, err := meta.ParseSequenceKey(field); err == nil {
			return keyTypeSID, id
		}
	}
	if meta.IsAutoRandomTableIDKey(field) {
		if id, err := meta.ParseAutoRandomTableIDKey(field); err == nil {
			return keyTypeTARID, id
		}
	}
	return keyTypeUnknown, 0
}

func newMetaKVCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metakv",
		Short: "Parse PiTR log backup data files and break down meta KV entries by key type.",
		RunE:  runMetaKVInspect,
	}
	cmd.Flags().String(flagStartTS, "", "Start TS of the restore range (TSO uint64 or datetime string)")
	cmd.Flags().String(flagRestoredTS, "", "End TS of the restore range (TSO uint64 or datetime string)")
	cmd.Flags().Int(flagTop, 20, "Limit per-table output to top N contributors by entry count")
	_ = cmd.MarkFlagRequired(flagStartTS)
	_ = cmd.MarkFlagRequired(flagRestoredTS)
	return cmd
}

func runMetaKVInspect(cmd *cobra.Command, _ []string) error {
	if err := initInspector(cmd); err != nil {
		return err
	}
	ctx := getDefaultContext()

	var cfg task.Config
	if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
		return err
	}
	if cfg.Storage == "" {
		return fmt.Errorf("--storage (-s) is required")
	}

	startTSStr, err := cmd.Flags().GetString(flagStartTS)
	if err != nil {
		return err
	}
	endTSStr, err := cmd.Flags().GetString(flagRestoredTS)
	if err != nil {
		return err
	}
	topN, err := cmd.Flags().GetInt(flagTop)
	if err != nil {
		return err
	}

	startTS, err := task.ParseTSString(startTSStr, false)
	if err != nil {
		return fmt.Errorf("invalid --start-ts: %w", err)
	}
	endTS, err := task.ParseTSString(endTSStr, false)
	if err != nil {
		return fmt.Errorf("invalid --restored-ts: %w", err)
	}
	if startTS > endTS {
		return fmt.Errorf("invalid range: --start-ts (%d) must be <= --restored-ts (%d)", startTS, endTS)
	}

	_, s, err := task.GetStorage(ctx, cfg.Storage, &cfg)
	if err != nil {
		return err
	}

	stats := newMetaKVStats()
	err = stream.FastUnmarshalMetaData(ctx, s, startTS, endTS, 128, func(path string, rawBytes []byte) error {
		// Use a local helper per callback to avoid data races on the shared helper cache.
		localHelper := stream.NewMetadataHelper()
		defer localHelper.Close()

		md, parseErr := localHelper.ParseToMetadataHard(rawBytes)
		if parseErr != nil {
			return fmt.Errorf("parsing %s: %w", path, parseErr)
		}

		for _, fg := range md.FileGroups {
			// Skip groups that contain no meta KV files.
			hasMetaFiles := false
			for _, dfi := range fg.DataFilesInfo {
				if dfi.IsMeta {
					hasMetaFiles = true
					break
				}
			}
			if !hasMetaFiles {
				continue
			}

			// For V2 groups (shared physical file, non-zero range lengths), initialize
			// the cache so ReadFile can slice the shared file correctly.
			// For V1 groups (offset=0, length=0), ReadFile reads the whole file without
			// needing a cache entry.
			isV2Group := false
			for _, dfi := range fg.DataFilesInfo {
				if dfi.RangeLength > 0 {
					isV2Group = true
					break
				}
			}
			if isV2Group {
				localHelper.InitCacheEntry(fg.Path, len(fg.DataFilesInfo))
			}

			for _, dfi := range fg.DataFilesInfo {
				buff, readErr := localHelper.ReadFile(
					ctx, fg.Path, dfi.RangeOffset, dfi.RangeLength,
					dfi.CompressionType, s, dfi.FileEncryptionInfo,
				)
				if readErr != nil {
					stats.mu.Lock()
					stats.parseErrors++
					stats.mu.Unlock()
					continue
				}
				if !dfi.IsMeta {
					// ReadFile was still called to correctly decrement the cache ref count.
					continue
				}

				iter := stream.NewEventIterator(buff)
				for iter.Valid() {
					iter.Next()
					if iter.GetError() != nil {
						stats.mu.Lock()
						stats.parseErrors++
						stats.mu.Unlock()
						break
					}
					stats.accumulate(iter.Key(), iter.Value(), dfi.Cf)
				}

				stats.mu.Lock()
				stats.filesProcessed++
				stats.mu.Unlock()
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	printMetaKVStats(cmd, stats, startTS, endTS, topN)
	return nil
}

// fmtInt formats n with comma separators for readability.
func fmtInt(n int64) string {
	s := fmt.Sprintf("%d", n)
	b := make([]byte, 0, len(s)+5)
	for i := range len(s) {
		pos := len(s) - i
		if i > 0 && pos%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, s[i])
	}
	return string(b)
}

// avgVersions returns (entries / uniqueKeys), or 1.0 if uniqueKeys == 0.
func avgVersions(entries, uniqueKeys int64) float64 {
	if uniqueKeys == 0 {
		return 1.0
	}
	return float64(entries) / float64(uniqueKeys)
}

func printMetaKVStats(cmd *cobra.Command, stats *metaKVStats, startTS, endTS uint64, topN int) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	cmd.Printf("=== PiTR Meta KV Entry Analysis ===\n\n")
	cmd.Printf("TS Range: [%d, %d]\n", startTS, endTS)
	cmd.Printf("Data files processed: %s\n", fmtInt(stats.filesProcessed))
	cmd.Printf("Parse errors:         %s\n\n", fmtInt(stats.parseErrors))

	// ---- Top-Level Key Breakdown ----
	cmd.Printf("--- Top-Level Key Breakdown ---\n")

	type topRow struct {
		cat         string
		cf          string
		entries     int64
		keyBytes    int64
		valBytes    int64
		uniqueCount int64
		avgVer      float64
	}
	var rows []topRow
	for _, cat := range []topLevelCategory{catDB, catDDLHistory, catOther} {
		cfMap := stats.topLevel[cat]
		cfs := make([]string, 0, len(cfMap))
		for cf := range cfMap {
			cfs = append(cfs, cf)
		}
		sort.Strings(cfs)
		for _, cf := range cfs {
			st := cfMap[cf]
			ukeys := int64(len(stats.uniqueKeys[cat][cf]))
			rows = append(rows, topRow{
				cat:         cat.String(),
				cf:          cf,
				entries:     st.count,
				keyBytes:    st.keyBytes,
				valBytes:    st.valBytes,
				uniqueCount: ukeys,
				avgVer:      avgVersions(st.count, ukeys),
			})
		}
	}
	cmd.Printf("  %-15s | %-7s | %-12s | %-11s | %-12s | %-12s | %s\n",
		"Category", "CF", "Entries", "Key Bytes", "Value Bytes", "Unique Keys", "Avg Versions")
	cmd.Printf("  %s\n", strings.Repeat("-", 95))
	for _, r := range rows {
		cmd.Printf("  %-15s | %-7s | %-12s | %-11s | %-12s | %-12s | %.2f\n",
			r.cat, r.cf,
			fmtInt(r.entries),
			formatBytes(uint64(r.keyBytes)),
			formatBytes(uint64(r.valBytes)),
			fmtInt(r.uniqueCount),
			r.avgVer,
		)
	}
	cmd.Printf("\n")

	// ---- mDB Breakdown by Field Type (top N) ----
	type detailRow struct {
		dbID    int64
		tableID int64
		field   keyType
		cf      string
		entries int64
		keyB    int64
		valB    int64
	}
	var detailRows []detailRow
	for k, st := range stats.dbDetail {
		detailRows = append(detailRows, detailRow{
			dbID:    k.dbID,
			tableID: k.tableID,
			field:   k.field,
			cf:      k.cf,
			entries: st.count,
			keyB:    st.keyBytes,
			valB:    st.valBytes,
		})
	}
	sort.Slice(detailRows, func(i, j int) bool {
		if detailRows[i].entries != detailRows[j].entries {
			return detailRows[i].entries > detailRows[j].entries
		}
		// Deterministic tie-breaker: sort by dbID, tableID, field, cf
		if detailRows[i].dbID != detailRows[j].dbID {
			return detailRows[i].dbID < detailRows[j].dbID
		}
		if detailRows[i].tableID != detailRows[j].tableID {
			return detailRows[i].tableID < detailRows[j].tableID
		}
		if detailRows[i].field != detailRows[j].field {
			return detailRows[i].field < detailRows[j].field
		}
		return detailRows[i].cf < detailRows[j].cf
	})
	if topN > 0 && len(detailRows) > topN {
		detailRows = detailRows[:topN]
	}

	cmd.Printf("--- mDB Breakdown by Field Type (top %d) ---\n", topN)
	cmd.Printf("  %-8s | %-8s | %-7s | %-7s | %-12s | %-11s | %s\n",
		"DB ID", "Table ID", "Type", "CF", "Entries", "Key Bytes", "Value Bytes")
	cmd.Printf("  %s\n", strings.Repeat("-", 80))
	for _, r := range detailRows {
		tableIDStr := fmtInt(r.tableID)
		if r.field == keyTypeDB {
			tableIDStr = "-"
		}
		cmd.Printf("  %-8s | %-8s | %-7s | %-7s | %-12s | %-11s | %s\n",
			fmtInt(r.dbID), tableIDStr,
			r.field.String(), r.cf,
			fmtInt(r.entries),
			formatBytes(uint64(r.keyB)),
			formatBytes(uint64(r.valB)),
		)
	}
	cmd.Printf("\n")

	// ---- mDB Summary by Field Type ----
	type summaryKey struct {
		field keyType
		cf    string
	}
	type summaryVal struct {
		entries int64
	}
	summaryMap := make(map[summaryKey]*summaryVal)
	mdbTotalByCF := make(map[string]int64)
	for k, st := range stats.dbDetail {
		sk := summaryKey{field: k.field, cf: k.cf}
		if _, ok := summaryMap[sk]; !ok {
			summaryMap[sk] = &summaryVal{}
		}
		summaryMap[sk].entries += st.count
		mdbTotalByCF[k.cf] += st.count
	}

	type summaryRow struct {
		field   keyType
		cf      string
		entries int64
		pct     float64
	}
	var summaryRows []summaryRow
	for sk, sv := range summaryMap {
		total := mdbTotalByCF[sk.cf]
		var pct float64
		if total > 0 {
			pct = float64(sv.entries) / float64(total) * 100
		}
		summaryRows = append(summaryRows, summaryRow{
			field:   sk.field,
			cf:      sk.cf,
			entries: sv.entries,
			pct:     pct,
		})
	}
	sort.Slice(summaryRows, func(i, j int) bool {
		if summaryRows[i].cf != summaryRows[j].cf {
			return summaryRows[i].cf < summaryRows[j].cf
		}
		return summaryRows[i].entries > summaryRows[j].entries
	})

	cmd.Printf("--- mDB Summary by Field Type ---\n")
	cmd.Printf("  %-7s | %-7s | %-12s | %s\n", "Type", "CF", "Entries", "% of mDB")
	cmd.Printf("  %s\n", strings.Repeat("-", 50))
	for _, r := range summaryRows {
		cmd.Printf("  %-7s | %-7s | %-12s | %.1f%%\n",
			r.field.String(), r.cf, fmtInt(r.entries), r.pct)
	}
}
