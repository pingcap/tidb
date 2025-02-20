// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package snapclient

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/stretchr/testify/require"
)

func tmp(t *testing.T) *storage.LocalStorage {
	tmpDir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpDir)
	require.NoError(t, err)
	s.IgnoreEnoentForDelete = true
	return s
}

type pitrCollectorT struct {
	t       *testing.T
	coll    *pitrCollector
	tsoCnt  *atomic.Uint64
	success *atomic.Bool
	cx      context.Context
}

func (p pitrCollectorT) RestoreAFile(fs restore.BatchBackupFileSet) func() error {
	for _, b := range fs {
		for _, file := range b.SSTFiles {
			require.NoError(p.t, p.coll.restoreStorage.WriteFile(p.cx, file.Name, []byte("something")))
		}
	}

	res, err := p.coll.onBatch(p.cx, fs)
	require.NoError(p.t, err)
	return res
}

func (p pitrCollectorT) Done() {
	require.NoError(p.t, p.coll.close())
}

func (p pitrCollectorT) ExtFullBkups() []backuppb.IngestedSSTs {
	est := stream.MigrationExtension(p.coll.taskStorage)
	migs, err := est.Load(p.cx)
	require.NoError(p.t, err)
	res := []backuppb.IngestedSSTs{}
	for _, m := range migs.ListAll() {
		for _, pth := range m.IngestedSstPaths {
			content, err := p.coll.taskStorage.ReadFile(p.cx, pth)
			require.NoError(p.t, err)
			var sst backuppb.IngestedSSTs
			require.NoError(p.t, sst.Unmarshal(content))
			res = append(res, sst)
		}
	}
	return res
}

func (p *pitrCollectorT) MarkSuccess() {
	p.success.Store(true)
}

func (p *pitrCollectorT) Reopen() {
	newColl := &pitrCollector{
		enabled:        p.coll.enabled,
		taskStorage:    p.coll.taskStorage,
		restoreStorage: p.coll.restoreStorage,
		name:           fmt.Sprintf("test-%s-%d", p.t.Name(), p.tsoCnt.Add(1)),
		restoreUUID:    p.coll.restoreUUID,
		tso:            p.coll.tso,
		restoreSuccess: p.coll.restoreSuccess,
	}
	p.success.Store(false)
	p.coll = newColl

	p.coll.resetCommitting()
	p.coll.goPersister()
}

func (p pitrCollectorT) RequireCopied(extBk backuppb.IngestedSSTs, files ...string) {
	extFiles := make([]string, 0)
	for _, f := range extBk.Files {
		extFiles = append(extFiles, f.Name)
	}

	locatedFiles := make([]string, 0)
	for _, f := range files {
		locatedFiles = append(locatedFiles, p.coll.sstPath(f))
	}

	require.ElementsMatch(p.t, extFiles, locatedFiles)
}

func (p pitrCollectorT) RequireRewrite(extBk backuppb.IngestedSSTs, rules ...utils.TableIDRemap) {
	rulesInExtBk := []utils.TableIDRemap{}
	for _, f := range extBk.RewrittenTables {
		rulesInExtBk = append(rulesInExtBk, utils.TableIDRemap{
			Origin:    f.AncestorUpstream,
			Rewritten: f.Upstream,
		})
	}
	require.ElementsMatch(p.t, rulesInExtBk, rules)
}

func newPiTRCollForTest(t *testing.T) pitrCollectorT {
	taskStorage := tmp(t)
	restoreStorage := tmp(t)

	coll := &pitrCollector{
		enabled:        true,
		taskStorage:    taskStorage,
		restoreStorage: restoreStorage,
		name:           "test-" + t.Name(),
		restoreUUID:    uuid.New(),
	}
	tsoCnt := new(atomic.Uint64)
	restoreSuccess := new(atomic.Bool)
	coll.tso = func(ctx context.Context) (uint64, error) {
		return tsoCnt.Add(1), nil
	}
	coll.restoreSuccess = restoreSuccess.Load
	coll.goPersister()
	coll.resetCommitting()

	return pitrCollectorT{
		t:       t,
		coll:    coll,
		tsoCnt:  tsoCnt,
		success: restoreSuccess,
		cx:      context.Background(),
	}
}

type backupFileSetOp func(*restore.BackupFileSet)

func backupFileSet(ops ...backupFileSetOp) restore.BackupFileSet {
	set := restore.BackupFileSet{
		RewriteRules: new(utils.RewriteRules),
	}
	for _, op := range ops {
		op(&set)
	}
	return set
}

func nameFile(n string) *backuppb.File {
	return &backuppb.File{
		Name: n,
	}
}

func withFile(f *backuppb.File) backupFileSetOp {
	return func(set *restore.BackupFileSet) {
		set.SSTFiles = append(set.SSTFiles, f)
	}
}

func remap(from, to int64) utils.TableIDRemap {
	return utils.TableIDRemap{Origin: from, Rewritten: to}
}

func withRewriteRule(hints ...utils.TableIDRemap) backupFileSetOp {
	return func(set *restore.BackupFileSet) {
		set.RewriteRules.TableIDRemapHint = append(set.RewriteRules.TableIDRemapHint, hints...)
	}
}

func TestCollAFile(t *testing.T) {
	coll := newPiTRCollForTest(t)
	batch := restore.BatchBackupFileSet{backupFileSet(withFile(nameFile("foo.txt")))}

	require.NoError(t, coll.RestoreAFile(batch)())
	coll.MarkSuccess()
	coll.Done()

	exts := coll.ExtFullBkups()
	require.Len(t, exts, 1)
	e := exts[0]
	coll.RequireCopied(e, "foo.txt")
	require.True(t, e.Finished, "%v", e)
	require.Equal(t, coll.coll.restoreUUID[:], e.BackupUuid)
}

func TestCollManyFileAndRewriteRules(t *testing.T) {
	coll := newPiTRCollForTest(t)
	batch := restore.BatchBackupFileSet{
		backupFileSet(withFile(nameFile("foo.txt"))),
		backupFileSet(withFile(nameFile("bar.txt")), withRewriteRule(remap(1, 10))),
		backupFileSet(withFile(nameFile("baz.txt")), withRewriteRule(remap(2, 20))),
		backupFileSet(withFile(nameFile("quux.txt")), withRewriteRule(remap(3, 21))),
	}

	require.NoError(t, coll.RestoreAFile(batch)())
	coll.MarkSuccess()
	coll.Done()

	exts := coll.ExtFullBkups()
	require.Len(t, exts, 1)
	e := exts[0]
	coll.RequireCopied(e, "foo.txt", "bar.txt", "baz.txt", "quux.txt")
	coll.RequireRewrite(e, remap(1, 10), remap(2, 20), remap(3, 21))
	require.True(t, e.Finished, "%v", e)
	require.Equal(t, coll.coll.restoreUUID[:], e.BackupUuid)
}

func TestReopen(t *testing.T) {
	coll := newPiTRCollForTest(t)
	batch1 := restore.BatchBackupFileSet{
		backupFileSet(withFile(nameFile("foo.txt"))),
		backupFileSet(withFile(nameFile("bar.txt")), withRewriteRule(remap(1, 10)))}
	batch2 := restore.BatchBackupFileSet{backupFileSet(withFile(nameFile("baz.txt")), withRewriteRule(remap(2, 20)))}
	batch3 := restore.BatchBackupFileSet{backupFileSet(withFile(nameFile("quux.txt")), withRewriteRule(remap(3, 21)))}

	require.NoError(t, coll.RestoreAFile(batch1)())
	coll.Done()
	exts := coll.ExtFullBkups()
	require.Len(t, exts, 1)
	e := exts[0]
	coll.RequireCopied(e, "foo.txt", "bar.txt")
	coll.RequireRewrite(e, remap(1, 10))
	require.False(t, e.Finished, "%v", e)
	require.Equal(t, coll.coll.restoreUUID[:], e.BackupUuid)

	coll.Reopen()
	require.NoError(t, coll.RestoreAFile(batch2)())
	exts = coll.ExtFullBkups()
	require.Len(t, exts, 2)
	e = exts[1]
	coll.RequireCopied(e, "baz.txt")
	coll.RequireRewrite(e, remap(2, 20))
	require.False(t, e.Finished, "%v", e)
	require.Equal(t, coll.coll.restoreUUID[:], e.BackupUuid)
	coll.coll.writerRoutine.close()

	coll.Reopen()
	require.NoError(t, coll.RestoreAFile(batch3)())
	coll.MarkSuccess()
	coll.Done()
	exts = coll.ExtFullBkups()
	require.Len(t, exts, 3)
	e = exts[2]
	coll.RequireCopied(e, "quux.txt")
	coll.RequireRewrite(e, remap(3, 21))
	require.True(t, e.Finished, "%v", e)
	require.Equal(t, coll.coll.restoreUUID[:], e.BackupUuid)
}

func TestConflict(t *testing.T) {
	coll := newPiTRCollForTest(t)
	batch := restore.BatchBackupFileSet{
		backupFileSet(withFile(nameFile("foo.txt")), withRewriteRule(remap(1, 10))),
		backupFileSet(withFile(nameFile("foo.txt")), withRewriteRule(remap(1, 11))),
	}

	cb, err := coll.coll.onBatch(coll.cx, batch)
	// NOTE: An error here is also acceptable.
	require.NoError(t, err)
	require.Error(t, cb())

	coll.Done()
}
