// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package snapclient

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/operation"
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

type capturedLockWrite struct {
	path string
	meta objstore.LockMeta
}

type lockCaptureStorage struct {
	storeapi.Storage
	mu     sync.Mutex
	writes []capturedLockWrite
}

func (s *lockCaptureStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	s.captureLockWrite(name, data)
	return s.Storage.WriteFile(ctx, name, data)
}

func (s *lockCaptureStorage) captureLockWrite(name string, data []byte) {
	if !strings.HasPrefix(name, "v1/LOCK") && !strings.HasPrefix(name, "v1/APPEND_LOCK") {
		return
	}

	var meta objstore.LockMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return
	}
	if meta.OwnerID == "" || meta.LockType == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, capturedLockWrite{path: name, meta: meta})
}

func (s *lockCaptureStorage) capturedLockWrites() []capturedLockWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.writes)
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
		enabled:          p.coll.enabled,
		taskStorage:      p.coll.taskStorage,
		restoreStorage:   p.coll.restoreStorage,
		name:             fmt.Sprintf("test-%s-%d", p.t.Name(), p.tsoCnt.Add(1)),
		restoreUUID:      p.coll.restoreUUID,
		operationContext: p.coll.operationContext,
		tso:              p.coll.tso,
		restoreSuccess:   p.coll.restoreSuccess,
	}
	p.success.Store(false)
	p.coll = newColl
	p.coll.init()
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
	opCtx, err := operation.NewContext("test pitr collector")
	require.NoError(t, err)
	opCtx.SetHintField("restore_id", "789")

	coll := &pitrCollector{
		enabled:          true,
		taskStorage:      taskStorage,
		restoreStorage:   restoreStorage,
		name:             "test-" + t.Name(),
		restoreUUID:      uuid.New(),
		operationContext: opCtx,
	}
	tsoCnt := new(atomic.Uint64)
	restoreSuccess := new(atomic.Bool)
	coll.tso = func(ctx context.Context) (uint64, error) {
		return tsoCnt.Add(1), nil
	}
	coll.restoreSuccess = restoreSuccess.Load
	coll.init()

	return pitrCollectorT{
		t:       t,
		coll:    coll,
		tsoCnt:  tsoCnt,
		success: restoreSuccess,
		cx:      context.Background(),
	}
}

func TestPiTRCollectorPrepareMigWritesOperationMetadata(t *testing.T) {
	coll := newPiTRCollForTest(t)
	defer coll.Done()
	capturingStorage := &lockCaptureStorage{Storage: coll.coll.taskStorage}
	coll.coll.taskStorage = capturingStorage

	require.NoError(t, coll.coll.prepareMig(coll.cx))
	writes := capturingStorage.capturedLockWrites()

	var appendLocks []capturedLockWrite
	for _, write := range writes {
		if write.meta.LockType == string(operation.LockResourceMigrationAppend) {
			appendLocks = append(appendLocks, write)
		}
	}
	require.Len(t, appendLocks, 1)
	meta := appendLocks[0].meta
	require.Equal(t, coll.coll.operationContext.OperationID, meta.OwnerID)
	require.Contains(t, meta.Hint, "operation_started_at="+coll.coll.operationContext.StartedAt.Format(time.RFC3339))
	require.Contains(t, meta.Hint, "restore_id=789")
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

func TestConcurrency(t *testing.T) {
	coll := newPiTRCollForTest(t)
	coll.coll.setConcurrency(2)

	cnt := int64(0)
	fence := make(chan struct{})

	failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/restore/snap_client/put-sst", func() {
		atomic.AddInt64(&cnt, 1)
		<-fence
	})

	cbs := []func() error{}
	l := sync.Mutex{}
	for i := range 10 {
		batch := restore.BatchBackupFileSet{
			backupFileSet(withFile(nameFile(fmt.Sprintf("foo%02d.txt", i)))),
		}

		go func() {
			cb := coll.RestoreAFile(batch)
			l.Lock()
			cbs = append(cbs, cb)
			l.Unlock()
		}()
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&cnt) == 2
	}, time.Second, 10*time.Millisecond)
	close(fence)

	func() {
		l.Lock()
		defer l.Unlock()

		for _, cb := range cbs {
			require.NoError(t, cb())
		}
	}()

	coll.Done()
}
