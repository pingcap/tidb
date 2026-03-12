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

package testutil

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/stream/backupmetas"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// TestHarness wires PDSim/FlushSim/CRR worker and one advancer together.
type TestHarness struct {
	upstreamStorage   storeapi.Storage
	downstreamStorage storeapi.Storage
	tc                *TestContext

	PDSim      *PDSim
	FlushSim   *FlushSim
	CRRWorker  *CRRWorker
	Upstream   *CRRUpstreamStorage
	Downstream storeapi.Storage
	Advancer   *streamhelper.CheckpointAdvancer
}

func NewLocalTestHarnessWithTestContext(
	ctx context.Context,
	tc *TestContext,
	boundaries []RegionBoundary,
) (*TestHarness, error) {
	return newLocalTestHarness(ctx, tc, tc.T.TempDir(), boundaries)
}

func newLocalTestHarness(
	ctx context.Context,
	tc *TestContext,
	baseDir string,
	boundaries []RegionBoundary,
) (*TestHarness, error) {
	upstreamDir := filepath.Join(baseDir, "upstream")
	downstreamDir := filepath.Join(baseDir, "downstream")
	if err := os.MkdirAll(upstreamDir, 0o750); err != nil {
		return nil, fmt.Errorf("create upstream dir %s: %w", upstreamDir, err)
	}
	if err := os.MkdirAll(downstreamDir, 0o750); err != nil {
		return nil, fmt.Errorf("create downstream dir %s: %w", downstreamDir, err)
	}

	upstreamStorage, err := objstore.NewLocalStorage(upstreamDir)
	if err != nil {
		return nil, fmt.Errorf("create local upstream storage at %s: %w", upstreamDir, err)
	}
	downstreamStorage, err := objstore.NewLocalStorage(downstreamDir)
	if err != nil {
		upstreamStorage.Close()
		return nil, fmt.Errorf("create local downstream storage at %s: %w", downstreamDir, err)
	}

	pd, err := NewPDSimWithTestContext(boundaries, defaultTaskName, tc)
	if err != nil {
		upstreamStorage.Close()
		downstreamStorage.Close()
		return nil, fmt.Errorf("create pd sim: %w", err)
	}

	events := make(chan NewVersionCreatedEvent, 1024)
	worker := NewCRRWorker(upstreamStorage, downstreamStorage, events)
	upstream := NewCRRUpstreamStorage(upstreamStorage, events)

	advancer := streamhelper.NewCommandCheckpointAdvancer(pd)
	advancer.StartTaskListener(ctx)
	advancer.SpawnSubscriptionHandler(ctx)

	h := &TestHarness{
		upstreamStorage:   upstreamStorage,
		downstreamStorage: downstreamStorage,
		tc:                tc,
		PDSim:             pd,
		FlushSim:          NewFlushSimWithTestContext(pd, upstream, tc),
		CRRWorker:         worker,
		Upstream:          upstream,
		Downstream:        downstreamStorage,
		Advancer:          advancer,
	}
	tc.T.Cleanup(h.Close)
	return h, nil
}

// Tick drives one deterministic advancer state transition.
func (h *TestHarness) Tick(ctx context.Context) (uint64, error) {
	if err := h.Advancer.OnTick(ctx); err != nil {
		return 0, err
	}
	return h.PDSim.GlobalCheckpoint(), nil
}

// PullMessages pulls pending replication events into worker local buffer.
func (h *TestHarness) PullMessages(limit int) int {
	return h.CRRWorker.PullMessages(limit)
}

// Replicate copies buffered events to downstream.
func (h *TestHarness) Replicate(ctx context.Context, limit int) (int, error) {
	return h.CRRWorker.ReplicateBuffered(ctx, limit)
}

// UploadGlobalCheckpoint uploads task global checkpoint in simulator metadata.
func (h *TestHarness) UploadGlobalCheckpoint(ctx context.Context, checkpoint uint64) error {
	if err := h.PDSim.UploadV3GlobalCheckpointForTask(ctx, defaultTaskName, checkpoint); err != nil {
		return fmt.Errorf("upload global checkpoint %d: %w", checkpoint, err)
	}
	return nil
}

func assertReadableFile(ctx context.Context, storage storeapi.Storage, name string) error {
	if _, err := storage.ReadFile(ctx, name); err != nil {
		return fmt.Errorf("%s is not readable: %w", name, err)
	}
	return nil
}

func extractDataFilePaths(meta *backuppb.Metadata) []string {
	paths := make([]string, 0, len(meta.FileGroups))
	for _, group := range meta.FileGroups {
		if group.Path != "" {
			paths = append(paths, group.Path)
			continue
		}
		for _, file := range group.DataFilesInfo {
			if file.Path != "" {
				paths = append(paths, file.Path)
			}
		}
	}
	return paths
}

func parseBackupMetadata(raw []byte) (*backuppb.Metadata, error) {
	return (*stream.MetadataHelper).ParseToMetadata(nil, raw)
}

// AssertDownstreamCanRestoreTo validates that downstream can read every
// backupmeta and corresponding log files that were already part of a checkpoint
// transition to tso.
func (h *TestHarness) AssertDownstreamCanRestoreTo(ctx context.Context, tso uint64) error {
	globalCheckpoint := h.PDSim.GlobalCheckpoint()
	if globalCheckpoint < tso {
		return fmt.Errorf("global checkpoint %d is behind target %d", globalCheckpoint, tso)
	}

	records := h.FlushSim.RecordsUpTo(tso)
	for _, record := range records {
		if err := assertReadableFile(ctx, h.Downstream, record.MetadataPath); err != nil {
			return err
		}

		baseName := strings.TrimSuffix(path.Base(record.MetadataPath), path.Ext(record.MetadataPath))
		parsed, err := backupmetas.ParseName(baseName)
		if err != nil {
			return fmt.Errorf("parse backupmeta %s: %w", record.MetadataPath, err)
		}
		if parsed.FlushTS != record.FlushTS {
			return fmt.Errorf(
				"backupmeta %s has flush ts %d, expected %d",
				record.MetadataPath,
				parsed.FlushTS,
				record.FlushTS,
			)
		}

		content, err := h.Downstream.ReadFile(ctx, record.MetadataPath)
		if err != nil {
			return fmt.Errorf("read backupmeta %s: %w", record.MetadataPath, err)
		}
		meta, err := parseBackupMetadata(content)
		if err != nil {
			return fmt.Errorf("parse backupmeta %s: %w", record.MetadataPath, err)
		}

		for _, logPath := range extractDataFilePaths(meta) {
			if err := assertReadableFile(ctx, h.Downstream, logPath); err != nil {
				return fmt.Errorf("backupmeta %s references unreadable log file: %w", record.MetadataPath, err)
			}
		}
	}
	return nil
}

// Close releases harness storage resources.
func (h *TestHarness) Close() {
	h.upstreamStorage.Close()
	h.downstreamStorage.Close()
}
