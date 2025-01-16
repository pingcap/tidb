package snapclient

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type persistCall struct {
	cx context.Context
	cb func(error)
}

// persisterHandle is a handle to the background writer persisting the metadata.
type persisterHandle struct {
	hnd chan<- persistCall
}

// close releases the handle.
func (w persisterHandle) close() {
	if w.hnd != nil {
		close(w.hnd)
	}
}

// write starts a request to persist the current metadata to the external storage.
//
// all modification before the `write` call will be persisted in the external storage
// after this returns.
func (w persisterHandle) write(ctx context.Context) error {
	// A buffer here is necessrary.
	// Or once the writerCall finished too fastly, it calls the callback before the `select`
	// block entered, we may lose the response.
	ch := make(chan error, 1)
	w.hnd <- persistCall{
		cx: ctx,
		cb: func(err error) {
			select {
			case ch <- err:
			default:
				log.Warn("Blocked when sending to a oneshot channel, dropping the message.",
					logutil.AShortError("dropped-result", err), zap.StackSkip("caller", 1))
			}
		},
	}

	select {
	case err, ok := <-ch:
		if !ok {
			// Though the channel is never closed, we can still gracefully exit
			// by canceling the context.
			log.Panic("[unreachable] A channel excepted to be never closed was closed.")
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// goPersister spawns the background centeralized persister.
//
// this would be the sole goroutine that writes to `c.metaPath()`.
func (c *pitrCollector) goPersister() {
	hnd := make(chan persistCall, 2048)
	exhaust := func(f func(persistCall)) {
	collect:
		for {
			select {
			case cb, ok := <-hnd:
				if !ok {
					log.Warn("Early channel close. Should not happen.")
					return
				}
				f(cb)
			default:
				break collect
			}
		}
	}

	go func() {
		for newCall := range hnd {
			cs := []persistCall{newCall}
			// Consuming all pending writes.
			exhaust(func(newCall persistCall) {
				cs = append(cs, newCall)
			})

			err := c.doPersistExtraBackupMeta(cs[0].cx)

			for _, c := range cs {
				c.cb(err)
			}
		}
	}()

	c.writerRoutine = persisterHandle{
		hnd: hnd,
	}
}

// pitrCollector controls the process of copying restored SSTs to a log backup storage.
//
// log backup cannot back `Ingest`ed SSTs directly. As a workaround, ingested SSTs will
// be copied to the log backup storage when restoring. Then when doing PiTR, those SSTs
// can be ingested.
//
// This provides two hooks to the `Importer` and those will be called syncrhonously.
// - `onBatch`: this starts the upload process of a batch of files, returns a closure that
// .            waits unfinished upload process done.
// - `close`: flush all pending metadata and commit all SSTs to the log backup storage so
// .          they are visible for a PiTR.
// The two hooks are goroutine safe.
type pitrCollector struct {
	// Immutable state.

	// taskStorage is the log backup storage.
	taskStorage storage.ExternalStorage
	// restoreStorage is where the running restoration from.
	restoreStorage storage.ExternalStorage
	// name is a human-friendly identity to this restoration.
	// When restart from a checkpoint, a new name will be generated.
	name string
	// enabled indicites whether the pitrCollector needs to work.
	enabled bool
	// restoreUUID is the identity of this restoration.
	// This will be kept among restarting from checkpoints.
	restoreUUID uuid.UUID

	// Mutable state.
	ingestedSSTMeta     ingestedSSTsMeta
	ingestedSSTMetaLock sync.Mutex
	putMigOnce          sync.Once
	writerRoutine       persisterHandle

	// Delegates.

	// tso fetches a recent timestamp oracle from somewhere.
	tso func(ctx context.Context) (uint64, error)
	// restoreSuccess returns whether the restore was fully done.
	restoreSuccess func() bool
}

// ingestedSSTsMeta is state of already imported SSTs.
//
// This and only this will be fully persisted to the
// ingested ssts meta in the external storage.
type ingestedSSTsMeta struct {
	msg      pb.IngestedSSTs
	rewrites map[int64]int64
}

// toProtoMessage generates the protocol buffer message to persist.
func (c *ingestedSSTsMeta) toProtoMessage() *pb.IngestedSSTs {
	msg := util.ProtoV1Clone(&c.msg)
	for old, new := range c.rewrites {
		msg.RewrittenTables = append(msg.RewrittenTables, &pb.RewrittenTableID{AncestorUpstream: old, Upstream: new})
	}
	return msg
}

func (c *pitrCollector) close() error {
	defer c.writerRoutine.close()

	if !c.enabled {
		return nil
	}

	cx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !c.restoreSuccess() {
		log.Warn("Backup not success, put a half-finished metadata to the log backup.",
			zap.Stringer("uuid", c.restoreUUID))
		return errors.Annotatef(c.persistExtraBackupMeta(cx), "failed to persist the meta")
	}

	commitTS, err := c.commit(cx)
	if err != nil {
		return errors.Annotate(err, "failed to commit pitrCollector")
	}
	log.Info("Log backup SSTs are committed.",
		zap.Uint64("commitTS", commitTS), zap.String("committedTo", c.outputPath()))

	return nil
}

func (c *pitrCollector) verifyCompatibilityFor(fileset *restore.BackupFileSet) error {
	if len(fileset.RewriteRules.NewKeyspace) > 0 {
		return errors.Annotate(berrors.ErrUnsupportedOperation, "keyspace rewriting isn't supported when log backup enabled")
	}
	for i, r := range fileset.RewriteRules.Data {
		if r.NewTimestamp > 0 {
			return errors.Annotatef(berrors.ErrUnsupportedOperation,
				"rewrite rule #%d: rewrite timestamp isn't supported when log backup enabled", i)
		}
		if r.IgnoreAfterTimestamp > 0 || r.IgnoreBeforeTimestamp > 0 {
			return errors.Annotatef(berrors.ErrUnsupportedOperation,
				"rewrite rule #%d: truncating timestamp isn't supported when log backup enabled", i)
		}
	}
	return nil
}

func (c *pitrCollector) onBatch(ctx context.Context, fileSets restore.BatchBackupFileSet) (func() error, error) {
	if !c.enabled {
		return nil, nil
	}

	if err := c.prepareMigIfNeeded(ctx); err != nil {
		return nil, err
	}

	begin := time.Now()
	eg, ectx := errgroup.WithContext(ctx)
	fileCount := 0
	for _, fileSet := range fileSets {
		if err := c.verifyCompatibilityFor(&fileSet); err != nil {
			return nil, err
		}

		for _, file := range fileSet.SSTFiles {
			fileCount += 1
			eg.Go(func() error {
				if err := c.putSST(ectx, file); err != nil {
					return errors.Annotatef(err, "failed to put sst %s", file.GetName())
				}
				return nil
			})
		}
		for _, hint := range fileSet.RewriteRules.TableIDRemapHint {
			eg.Go(func() error {
				if err := c.putRewriteRule(ectx, hint.Origin, hint.Rewritten); err != nil {
					return errors.Annotatef(err, "failed to put rewrite rule of %v", fileSet.RewriteRules)
				}
				return nil
			})
		}
	}

	waitDone := func() error {
		err := eg.Wait()
		if err != nil {
			logutil.CL(ctx).Warn("Failed to upload SSTs for future PiTR.", logutil.ShortError(err))
			return err
		}

		logutil.CL(ctx).Info("Uploaded a batch of SSTs for future PiTR.",
			zap.Duration("take", time.Since(begin)), zap.Int("file-count", fileCount))

		err = c.persistExtraBackupMeta(ctx)
		if err != nil {
			return errors.Annotatef(err, "failed to persist backup meta when finishing batch")
		}
		return nil
	}
	return waitDone, nil
}

func (c *pitrCollector) doWithMetaLock(f func()) {
	c.ingestedSSTMetaLock.Lock()
	f()
	c.ingestedSSTMetaLock.Unlock()
}

// outputPath constructs the path by a relative path for outputting.
func (c *pitrCollector) outputPath(segs ...string) string {
	return filepath.Join(append([]string{"v1", "ext_backups", c.name}, segs...)...)
}

func (c *pitrCollector) metaPath() string {
	return c.outputPath("extbackupmeta")
}

func (c *pitrCollector) sstPath(name string) string {
	return c.outputPath("sst_files", name)
}

// putSST records an SST file.
func (c *pitrCollector) putSST(ctx context.Context, f *pb.File) error {
	if !c.enabled {
		return nil
	}

	begin := time.Now()

	f = util.ProtoV1Clone(f)
	out := c.sstPath(f.Name)

	copier, ok := c.taskStorage.(storage.Copier)
	if !ok {
		return errors.Annotatef(berrors.ErrInvalidArgument, "storage %T does not support copying", c.taskStorage)
	}
	spec := storage.CopySpec{
		From: f.GetName(),
		To:   out,
	}

	copyStart := time.Now()
	if err := copier.CopyFrom(ctx, c.restoreStorage, spec); err != nil {
		return errors.Annotatef(err, "failed to copy sst file %s to %s, "+
			"you may check whether permissions are granted in both %s and %s, "+
			"and the two storages are provided by the same cloud vendor",
			spec.From, spec.To, c.restoreStorage.URI(), c.taskStorage.URI())
	}
	log.Info("Copy SST to log backup storage success.", zap.String("file", f.Name), zap.Stringer("takes", time.Since(copyStart)))

	f.Name = out
	c.doWithMetaLock(func() { c.ingestedSSTMeta.msg.Files = append(c.ingestedSSTMeta.msg.Files, f) })

	metrics.RestoreUploadSSTForPiTRSeconds.Observe(time.Since(begin).Seconds())
	return nil
}

// putRewriteRule records a rewrite rule.
func (c *pitrCollector) putRewriteRule(_ context.Context, oldID int64, newID int64) error {
	if !c.enabled {
		return nil
	}
	var err error
	c.doWithMetaLock(func() {
		if oldVal, ok := c.ingestedSSTMeta.rewrites[oldID]; ok && oldVal != newID {
			err = errors.Annotatef(
				berrors.ErrInvalidArgument,
				"pitr coll rewrite rule conflict: we had %v -> %v, but you want rewrite to %v",
				oldID,
				oldVal,
				newID,
			)
			return
		}
		c.ingestedSSTMeta.rewrites[oldID] = newID
	})
	return err
}

// doPersistExtraBackupMeta writes the current content of extra backup meta to the external storage.
// This isn't goroutine-safe. Please don't call it concurrently.
func (c *pitrCollector) doPersistExtraBackupMeta(ctx context.Context) (err error) {
	if !c.enabled {
		return nil
	}

	var bs []byte
	begin := time.Now()
	c.doWithMetaLock(func() {
		msg := c.ingestedSSTMeta.toProtoMessage()
		// Here, after generating a snapshot of the current message then we can continue.
		// This requires only a single active writer at anytime.
		// (i.e. concurrent call to `doPersistExtraBackupMeta` may cause data race.)
		// If there are many writers, the writer gets a stale snapshot may overwrite
		// the latest persisted file.
		bs, err = msg.Marshal()
	})

	if err != nil {
		return errors.Annotate(err, "failed to marsal the committing message")
	}
	logutil.CL(ctx).Info("Persisting extra backup meta.",
		zap.Stringer("uuid", c.restoreUUID), zap.String("path", c.metaPath()), zap.Stringer("takes", time.Since(begin)))

	err = c.taskStorage.WriteFile(ctx, c.metaPath(), bs)
	if err != nil {
		return errors.Annotatef(err, "failed to put content to meta to %s", c.metaPath())
	}

	metrics.RestoreUploadSSTMetaForPiTRSeconds.Observe(time.Since(begin).Seconds())
	logutil.CL(ctx).Debug("Persisting extra backup meta.",
		zap.Stringer("uuid", c.restoreUUID), zap.String("path", c.metaPath()), zap.Stringer("takes", time.Since(begin)))
	return nil
}

func (c *pitrCollector) persistExtraBackupMeta(ctx context.Context) (err error) {
	if !c.enabled {
		return nil
	}

	return c.writerRoutine.write(ctx)
}

// Commit commits the collected SSTs to a migration.
func (c *pitrCollector) prepareMig(ctx context.Context) error {
	if !c.enabled {
		return nil
	}

	est := stream.MigrationExtension(c.taskStorage)

	m := stream.NewMigration()
	m.IngestedSstPaths = append(m.IngestedSstPaths, c.metaPath())

	_, err := est.AppendMigration(ctx, m)
	if err != nil {
		return errors.Annotatef(err, "failed to add the extra backup at path %s", c.metaPath())
	}

	c.doWithMetaLock(func() {
		c.resetCommitting()
	})
	// Persist the metadata in case of SSTs were uploaded but the meta wasn't,
	// which leads to a leakage.
	return c.persistExtraBackupMeta(ctx)
}

func (c *pitrCollector) prepareMigIfNeeded(ctx context.Context) (err error) {
	c.putMigOnce.Do(func() {
		err = c.prepareMig(ctx)
	})
	return
}

func (c *pitrCollector) commit(ctx context.Context) (uint64, error) {
	c.ingestedSSTMeta.msg.Finished = true
	ts, err := c.tso(ctx)
	if err != nil {
		return 0, err
	}
	c.ingestedSSTMeta.msg.AsIfTs = ts
	return ts, c.persistExtraBackupMeta(ctx)
}

func (c *pitrCollector) resetCommitting() {
	c.ingestedSSTMeta = ingestedSSTsMeta{
		rewrites: map[int64]int64{},
	}
	c.ingestedSSTMeta.msg.FilesPrefixHint = c.sstPath("")
	c.ingestedSSTMeta.msg.Finished = false
	c.ingestedSSTMeta.msg.BackupUuid = c.restoreUUID[:]
}

// PiTRCollDep is the dependencies of a PiTR collector.
type PiTRCollDep struct {
	PDCli   pd.Client
	EtcdCli *clientv3.Client
	Storage *pb.StorageBackend
}

// newPiTRColl creates a new PiTR collector.
func newPiTRColl(ctx context.Context, deps PiTRCollDep) (*pitrCollector, error) {
	mcli := streamhelper.NewMetaDataClient(deps.EtcdCli)
	ts, err := mcli.GetAllTasks(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ts) > 1 {
		taskNames := []string{}
		for _, t := range ts {
			taskNames = append(taskNames, t.Info.Name)
		}
		return nil, errors.Annotatef(berrors.ErrInvalidArgument,
			"more than one task found, pitr collector doesn't support that, tasks are: %#v", taskNames)
	}
	if len(ts) == 0 {
		return &pitrCollector{}, nil
	}

	coll := &pitrCollector{
		enabled: true,
	}

	strg, err := storage.Create(ctx, ts[0].Info.Storage, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	coll.taskStorage = strg

	tso := func(ctx context.Context) (uint64, error) {
		l, o, err := deps.PDCli.GetTS(ctx)
		return oracle.ComposeTS(l, o), err
	}
	coll.tso = tso

	t, err := tso(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	coll.name = fmt.Sprintf("backup-%016X", t)

	restoreStrg, err := storage.Create(ctx, deps.Storage, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	coll.restoreStorage = restoreStrg
	coll.restoreSuccess = summary.Succeed
	coll.goPersister()
	coll.resetCommitting()
	return coll, nil
}
