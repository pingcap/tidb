// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package show

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/tikv/client-go/v2/oracle"
)

type Config struct {
	Storage    string
	BackendCfg storage.BackendOptions
	Cipher     backuppb.CipherInfo
}

// lameTaskConfig creates a `task.Config` via the `ShowConfig`.
// Because the call `ReadBackupMeta` requires a `task.Cfg` (which is a huge context!),
// for reusing it, we need to make a lame config with fields the call needs.
func (s *Config) lameTaskConfig() *task.Config {
	return &task.Config{
		Storage:        s.Storage,
		BackendOptions: s.BackendCfg,
		CipherInfo:     s.Cipher,
	}
}

// TimeStamp is a simple wrapper for the timestamp type.
// Perhaps we can enhance its display.
type TimeStamp uint64

// String implements fmt.String.
func (ts TimeStamp) String() string {
	return fmt.Sprintf("%d(%s)", ts, oracle.GetTimeFromTS(uint64(ts)).Format("Y06M01D02,15:03:04"))
}

type RawRange struct {
	ColumnFamily string           `json:"column-family"`
	StartKey     logutil.HexBytes `json:"start-key"`
	EndKey       logutil.HexBytes `json:"end-key"`
}

type Table struct {
	DBName         string `json:"db-name"`
	TableName      string `json:"table-name"`
	KVCount        uint   `json:"kv-count"`
	KVSize         uint   `json:"kv-size"`
	TiFlashReplica uint   `json:"tiflash-replica"`
}

type ShowResult struct {
	ClusterID      uint64    `json:"cluster-id"`
	ClusterVersion string    `json:"cluster-version"`
	BRVersion      string    `json:"br-version"`
	Version        int32     `json:"version"`
	StartVersion   TimeStamp `json:"start-version"`
	EndVersion     TimeStamp `json:"end-version"`

	IsRawKV   bool       `json:"is-raw-kv"`
	RawRanges []RawRange `json:"raw-ranges"`
	Tables    []Table    `json:"tables"`
}

type CmdExecutor struct {
	meta *metautil.MetaReader
}

func CreateExec(ctx context.Context, cfg Config) (*CmdExecutor, error) {
	_, strg, backupMeta, err := task.ReadBackupMeta(ctx, metautil.MetaFile, cfg.lameTaskConfig())
	if err != nil {
		return nil, errors.Annotate(err, "failed to create execution")
	}
	reader := metautil.NewMetaReader(backupMeta, strg, &cfg.Cipher)
	return &CmdExecutor{meta: reader}, nil
}

func (exec *CmdExecutor) Read(ctx context.Context) (ShowResult, error) {
	res := convertBasic(exec.meta.GetBasic())
	if res.EndVersion < res.StartVersion {
		return ShowResult{}, berrors.ErrInvalidMetaFile.GenWithStackByArgs(fmt.Sprintf(
			"the start version(%s) is greater than the end version(%s), perhaps reading a backup meta from log backup",
			res.StartVersion, res.EndVersion))
	}
	if !res.IsRawKV {
		out := make(chan *metautil.Table, 16)
		errc := make(chan error, 1)
		go func() {
			errc <- exec.meta.ReadSchemasFiles(ctx, out, metautil.SkipFiles, metautil.SkipStats)
			close(out)
		}()
		ts, err := collectResult(ctx, out, errc, convertTable)
		if err != nil {
			return ShowResult{}, err
		}
		res.Tables = ts
	} else {
		// NOTE: here we assumed raw KV backup isn't executed in V2.
		if exec.meta.GetBasic().RawRangeIndex != nil {
			return ShowResult{}, berrors.ErrInvalidMetaFile.GenWithStackByArgs("show raw kv with backup meta v2 isn't supported for now")
		}
		for _, rr := range exec.meta.GetBasic().RawRanges {
			res.RawRanges = append(res.RawRanges, convertRawRange(rr))
		}
	}
	return res, nil
}

// collectResult collects the result from an output channel(c) and an error channel(e).
// the items would be converted by a mapper function(m).
// NOTE: this function mixes many functions, can we make it composited by some functions?
func collectResult[T any, R any](ctx context.Context, c <-chan T, e <-chan error, m func(T) R) ([]R, error) {
	var collected []R
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case item, ok := <-c:
			if !ok {
				return collected, nil
			}
			collected = append(collected, m(item))
		case err := <-e:
			if err != nil {
				return nil, err
			}
		}
	}
}

func convertTable(t *metautil.Table) Table {
	// The name table may be empty (which means this is a record for a database.)
	tableName := ""
	if t.Info != nil {
		tableName = t.Info.Name.String()
	}
	result := Table{
		DBName:         t.DB.Name.String(),
		TableName:      tableName,
		KVCount:        uint(t.TotalKvs),
		KVSize:         uint(t.TotalBytes),
		TiFlashReplica: uint(t.TiFlashReplicas),
	}
	return result
}

func convertRawRange(r *backuppb.RawRange) RawRange {
	result := RawRange{
		ColumnFamily: r.Cf,
		StartKey:     r.StartKey,
		EndKey:       r.EndKey,
	}
	return result
}

func convertBasic(basic backuppb.BackupMeta) ShowResult {
	basicResult := ShowResult{
		ClusterID:      basic.ClusterId,
		ClusterVersion: basic.ClusterVersion,
		BRVersion:      basic.BrVersion,
		Version:        basic.Version,
		StartVersion:   TimeStamp(basic.StartVersion),
		EndVersion:     TimeStamp(basic.EndVersion),
		IsRawKV:        basic.IsRawKv,
	}
	return basicResult
}
