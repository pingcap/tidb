// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"fmt"
	"sort"
	"sync"

	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/spf13/cobra"
)

const (
	flagStartTS    = "start-ts"
	flagRestoredTS = "restored-ts"
)

type backupMetaStats struct {
	mu               sync.Mutex
	totalDataSize    uint64
	dataPerStore     map[int64]uint64
	metaSizePerStore map[int64]uint64
	metaFileCount    int
	fileGroupCount   int
}

func newBackupMetaStats() *backupMetaStats {
	return &backupMetaStats{
		dataPerStore:     make(map[int64]uint64),
		metaSizePerStore: make(map[int64]uint64),
	}
}

func (s *backupMetaStats) addMeta(storeID int64, dataSize uint64, metaSize uint64, groupCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalDataSize += dataSize
	s.dataPerStore[storeID] += dataSize
	s.metaSizePerStore[storeID] += metaSize
	s.metaFileCount++
	s.fileGroupCount += groupCount
}

func newBackupMetaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backupmeta",
		Short: "Inspect PiTR log backup metadata: file counts, data sizes, and per-store breakdown.",
		RunE:  runBackupMetaInspect,
	}
	cmd.Flags().String(flagStartTS, "", "Start TS of the restore range (TSO uint64 or datetime string)")
	cmd.Flags().String(flagRestoredTS, "", "End TS of the restore range (TSO uint64 or datetime string)")
	_ = cmd.MarkFlagRequired(flagStartTS)
	_ = cmd.MarkFlagRequired(flagRestoredTS)
	return cmd
}

func runBackupMetaInspect(cmd *cobra.Command, _ []string) error {
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

	stats := newBackupMetaStats()
	helper := stream.NewMetadataHelper()
	defer helper.Close()

	err = stream.FastUnmarshalMetaData(ctx, s, startTS, endTS, 128, func(path string, rawBytes []byte) error {
		meta, parseErr := helper.ParseToMetadataHard(rawBytes)
		if parseErr != nil {
			return fmt.Errorf("parsing %s: %w", path, parseErr)
		}
		var dataSize uint64
		for _, fg := range meta.FileGroups {
			dataSize += fg.Length
		}
		stats.addMeta(meta.StoreId, dataSize, uint64(len(rawBytes)), len(meta.FileGroups))
		return nil
	})
	if err != nil {
		return err
	}

	printStats(cmd, stats, startTS, endTS)
	return nil
}

func printStats(cmd *cobra.Command, stats *backupMetaStats, startTS, endTS uint64) {
	cmd.Printf("=== PiTR Log Backup Metadata Inspection ===\n\n")
	cmd.Printf("TS Range: [%d, %d]\n", startTS, endTS)
	cmd.Printf("Metadata files scanned: %d\n", stats.metaFileCount)
	cmd.Printf("Total file groups: %d\n\n", stats.fileGroupCount)

	cmd.Printf("--- Total Data Size ---\n")
	cmd.Printf("  %s (%d bytes)\n\n", formatBytes(stats.totalDataSize), stats.totalDataSize)

	storeIDs := make([]int64, 0, len(stats.dataPerStore))
	for id := range stats.dataPerStore {
		storeIDs = append(storeIDs, id)
	}
	sort.Slice(storeIDs, func(i, j int) bool { return storeIDs[i] < storeIDs[j] })

	cmd.Printf("--- Data Size Per Store ---\n")
	for _, id := range storeIDs {
		sz := stats.dataPerStore[id]
		cmd.Printf("  Store %d: %s (%d bytes)\n", id, formatBytes(sz), sz)
	}
	cmd.Printf("\n")

	metaStoreIDs := make([]int64, 0, len(stats.metaSizePerStore))
	for id := range stats.metaSizePerStore {
		metaStoreIDs = append(metaStoreIDs, id)
	}
	sort.Slice(metaStoreIDs, func(i, j int) bool { return metaStoreIDs[i] < metaStoreIDs[j] })

	var totalMetaSize uint64
	cmd.Printf("--- Metadata File Size Per Store ---\n")
	for _, id := range metaStoreIDs {
		sz := stats.metaSizePerStore[id]
		totalMetaSize += sz
		cmd.Printf("  Store %d: %s (%d bytes)\n", id, formatBytes(sz), sz)
	}
	cmd.Printf("  Total: %s (%d bytes)\n", formatBytes(totalMetaSize), totalMetaSize)
}
