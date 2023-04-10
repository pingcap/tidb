// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package show_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/task/show"
	"github.com/stretchr/testify/require"

	"embed"
)

var (
	//go:embed testdata/full-schema.meta
	FullMeta []byte

	//go:embed testdata/v2/*
	V2ManyTables embed.FS
)

func tempBackupDir(t *testing.T) string {
	req := require.New(t)
	tempBackup := path.Join(os.TempDir(), strconv.Itoa(os.Getpid()), t.Name())
	err := os.MkdirAll(tempBackup, 0o755)
	req.NoError(err)
	t.Cleanup(func() { _ = os.RemoveAll(tempBackup) })
	return tempBackup
}

func TestFull(t *testing.T) {
	req := require.New(t)
	tempBackup := tempBackupDir(t)

	metaPath := path.Join(tempBackup, "backupmeta")
	err := os.WriteFile(metaPath, FullMeta, 0o444)
	req.NoError(err)

	cfg := show.Config{
		Storage: fmt.Sprintf("local://%s", tempBackup),
		Cipher: backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
	}
	ctx := context.Background()
	exec, err := show.CreateExec(ctx, cfg)
	req.NoError(err)
	items, err := exec.Read(ctx)
	req.NoError(err)

	req.EqualValues(items.ClusterID, 7211076907329653533)
	req.EqualValues(items.ClusterVersion, "\"7.1.0-alpha\"\n")
	req.EqualValues(items.EndVersion, 440689413714870273)
	tableNames := make([]string, 0, len(items.Tables))
	for _, tbl := range items.Tables {
		req.Equal(tbl.DBName, "tpcc")
		tableNames = append(tableNames, tbl.TableName)
	}
	req.ElementsMatch(tableNames, []string{
		"customer",
		"district",
		"history",
		"item",
		"new_order",
		"order_line",
		"orders",
		"stock",
		"warehouse",
	})
}

func TestV2AndSmallTables(t *testing.T) {
	req := require.New(t)
	tempBackup := tempBackupDir(t)
	req.NoError(cloneFS(V2ManyTables, "testdata/v2", tempBackup))

	cfg := show.Config{
		Storage: fmt.Sprintf("local://%s", tempBackup),
		Cipher: backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
	}
	ctx := context.Background()
	exec, err := show.CreateExec(ctx, cfg)
	req.NoError(err)
	items, err := exec.Read(ctx)
	req.NoError(err)

	t.Fatalf("meow? %v", len(items.Tables))
}

func cloneFS(f fs.FS, base string, target string) error {
	return fs.WalkDir(f, base, func(p string, d fs.DirEntry, err error) error {
		rel := strings.TrimPrefix(p, base)
		if err != nil {
			return errors.Trace(err)
		}
		if d.IsDir() {
			return nil
		}
		tgt := path.Join(target, rel)
		dst, err := os.Create(tgt)
		if err != nil {
			return errors.Annotatef(err, "failed to create target item %s", tgt)
		}
		src, err := f.Open(p)
		if err != nil {
			return errors.Annotatef(err, "failed to open %s", p)
		}
		_, err = io.Copy(dst, src)
		return errors.Annotate(err, "failed to copy")
	})
}
