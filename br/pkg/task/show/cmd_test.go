// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package show_test

import (
	"bytes"
	"context"
	"embed"
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
	"github.com/pingcap/tidb/br/pkg/task/show"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

var (
	//go:embed testdata/full-schema.meta
	FullMeta []byte

	//go:embed testdata/v2/*
	V2ManyTables embed.FS

	//go:embed testdata/v2-enc/*
	V2Encrypted embed.FS
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

	req.Len(items.Tables, 500)
	req.EqualValues(items.ClusterID, 7211076907329653533)
	req.EqualValues(items.EndVersion, 440691270926467074)
}

func TestV2Encrypted(t *testing.T) {
	req := require.New(t)
	tempBackup := tempBackupDir(t)

	req.NoError(cloneFS(V2Encrypted, "testdata/v2-enc", tempBackup))
	cfg := show.Config{
		Storage: fmt.Sprintf("local://%s", tempBackup),
		Cipher: backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  bytes.Repeat([]byte{0x42}, 32),
		},
	}

	ctx := context.Background()
	exec, err := show.CreateExec(ctx, cfg)
	req.NoError(err)
	items, err := exec.Read(ctx)
	req.NoError(err)

	req.EqualValues(items.ClusterID, 7211076907329653533)
	req.EqualValues(items.ClusterVersion, "\"7.1.0-alpha\"\n")
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

func TestShowViaSQL(t *testing.T) {
	req := require.New(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tempBackup := tempBackupDir(t)
	metaPath := path.Join(tempBackup, "backupmeta")
	err := os.WriteFile(metaPath, FullMeta, 0o444)
	req.NoError(err)

	tk.MustExec("set @@time_zone='+08:00'")
	res := tk.MustQuery(fmt.Sprintf("SHOW BACKUP METADATA FROM 'local://%s'", tempBackup))
	fmt.Printf("%#v", res.Sort().Rows())
	res.Sort().Check([][]any{
		{"tpcc", "customer", "0", "0", "<nil>", "2023-04-10 11:18:21"},
		{"tpcc", "district", "0", "0", "<nil>", "2023-04-10 11:18:21"},
		{"tpcc", "history", "0", "0", "<nil>", "2023-04-10 11:18:21"},
		{"tpcc", "item", "0", "0", "<nil>", "2023-04-10 11:18:21"},
		{"tpcc", "new_order", "0", "0", "<nil>", "2023-04-10 11:18:21"},
		{"tpcc", "order_line", "0", "0", "<nil>", "2023-04-10 11:18:21"},
		{"tpcc", "orders", "0", "0", "<nil>", "2023-04-10 11:18:21"},
		{"tpcc", "stock", "0", "0", "<nil>", "2023-04-10 11:18:21"},
		{"tpcc", "warehouse", "0", "0", "<nil>", "2023-04-10 11:18:21"},
	})

	// Test result in different time_zone
	tk.MustExec("set @@time_zone='-08:00'")
	res = tk.MustQuery(fmt.Sprintf("SHOW BACKUP METADATA FROM 'local://%s'", tempBackup))
	fmt.Printf("%#v", res.Sort().Rows())
	res.Sort().Check([][]any{
		{"tpcc", "customer", "0", "0", "<nil>", "2023-04-09 19:18:21"},
		{"tpcc", "district", "0", "0", "<nil>", "2023-04-09 19:18:21"},
		{"tpcc", "history", "0", "0", "<nil>", "2023-04-09 19:18:21"},
		{"tpcc", "item", "0", "0", "<nil>", "2023-04-09 19:18:21"},
		{"tpcc", "new_order", "0", "0", "<nil>", "2023-04-09 19:18:21"},
		{"tpcc", "order_line", "0", "0", "<nil>", "2023-04-09 19:18:21"},
		{"tpcc", "orders", "0", "0", "<nil>", "2023-04-09 19:18:21"},
		{"tpcc", "stock", "0", "0", "<nil>", "2023-04-09 19:18:21"},
		{"tpcc", "warehouse", "0", "0", "<nil>", "2023-04-09 19:18:21"},
	})
}
