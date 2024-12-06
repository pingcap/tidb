package snapclient

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"
)

type pitrCollectorRestorer struct {
	// the context used for committing.
	cx context.Context
	// the context bound to the errgroup.
	ecx context.Context

	coll *pitrCollector
	wg   *errgroup.Group
}

// wrapRestorer wraps a restorer and the restorer will upload the SST file to the collector during restoring.
func (c *pitrCollector) createRestorer(ctx context.Context) *pitrCollectorRestorer {
	wg, ecx := errgroup.WithContext(ctx)
	return &pitrCollectorRestorer{
		cx:   ctx,
		ecx:  ecx,
		coll: c,
		wg:   wg,
	}
}

// GoRestore imports the specified backup file sets into TiKV asynchronously.
// The onProgress function is called with progress updates as files are processed.
func (p pitrCollectorRestorer) GoRestore(onProgress func(int64), batchFileSets ...restore.BatchBackupFileSet) error {
	if !p.coll.enabled {
		return nil
	}

	if err := p.coll.prepareMigIfNeeded(p.cx); err != nil {
		return err
	}

	p.wg.Go(func() error {
		for _, fileSets := range batchFileSets {
			for _, fileSet := range fileSets {
				for _, file := range fileSet.SSTFiles {
					if err := p.coll.PutSST(p.ecx, file); err != nil {
						return errors.Annotatef(err, "failed to put sst %s", file.GetName())
					}
				}
				for _, hint := range fileSet.RewriteRules.TableIDRemapHint {
					if err := p.coll.PutRewriteRule(p.ecx, hint.Origin, hint.Rewritten); err != nil {
						return errors.Annotatef(err, "failed to put rewrite rule of %v", fileSet.RewriteRules)
					}
				}
			}
		}
		return nil
	})
	return nil
}

// WaitUntilFinish blocks until all pending restore files have completed processing.
func (p pitrCollectorRestorer) WaitUntilFinish() error {
	if !p.coll.enabled {
		return nil
	}
	err := p.wg.Wait()
	if err != nil {
		return errors.Annotate(err, "failed to wait on pitrCollector")
	}
	return errors.Annotatef(p.coll.persist(p.cx), "failed to persist the metadata of uploaded SSTs")
}

// Close releases any resources associated with the restoration process.
func (p pitrCollectorRestorer) Close() error {
	if !p.coll.enabled {
		return nil
	}
	return errors.Annotate(p.coll.commit(p.cx), "failed to commit pitrCollector")
}

type pitrCollector struct {
	// Immutable state.
	taskStorage    storage.ExternalStorage
	restoreStorage storage.ExternalStorage
	name           string
	enabled        bool
	restoreUUID    uuid.UUID

	// Mutable state.
	committing     committing
	committingLock sync.Mutex
	putMigOnce     sync.Once

	// Delegates.
	tso func(ctx context.Context) (uint64, error)
}

type committing struct {
	msg      pb.ExtraFullBackup
	rewrites map[int64]int64
}

func (c *committing) commit() {
	c.msg.Finished = true
}

func (c *committing) genMsg() *pb.ExtraFullBackup {
	msg := util.ProtoV1Clone(&c.msg)
	for old, new := range c.rewrites {
		msg.RewrittenTables = append(msg.RewrittenTables, &pb.RewrittenTableID{UpstreamOfUpstream: old, Upstream: new})
	}
	return msg
}

// doWithCommittingLock edits the committing ExtraFullBackup.
func (c *pitrCollector) doWithCommittingLock(f func()) {
	c.committingLock.Lock()
	f()
	c.committingLock.Unlock()
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

// PutSST records an SST file.
func (c *pitrCollector) PutSST(ctx context.Context, f *pb.File) error {
	if !c.enabled {
		return nil
	}

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
	if err := copier.CopyFrom(ctx, c.restoreStorage, spec); err != nil {
		return err
	}

	f.Name = out
	c.doWithCommittingLock(func() { c.committing.msg.Files = append(c.committing.msg.Files, f) })
	return nil
}

// PutRewriteRule records a rewrite rule.
func (c *pitrCollector) PutRewriteRule(_ context.Context, oldID int64, newID int64) error {
	if !c.enabled {
		return nil
	}
	var err error
	c.doWithCommittingLock(func() {
		if oldVal, ok := c.committing.rewrites[oldID]; ok && oldVal != newID {
			err = errors.Annotatef(
				berrors.ErrInvalidArgument,
				"pitr coll rewrite rule conflict: we had %v -> %v, but you want rewrite to %v",
				oldID,
				oldVal,
				newID,
			)
			return
		}
		c.committing.rewrites[oldID] = newID
	})
	return err
}

func (c *pitrCollector) persist(ctx context.Context) (err error) {
	c.committingLock.Lock()
	defer c.committingLock.Unlock()

	msg := c.committing.genMsg()
	bs, err := msg.Marshal()
	if err != nil {
		return errors.Annotate(err, "failed to marsal the committing message")
	}
	err = c.taskStorage.WriteFile(ctx, c.metaPath(), bs)
	if err != nil {
		return errors.Annotatef(err, "failed to put content to meta to %s", c.metaPath())
	}
	return nil
}

// Commit commits the collected SSTs to a migration.
func (c *pitrCollector) prepareMig(ctx context.Context) error {
	if !c.enabled {
		return nil
	}

	est := stream.MigrationExtension(c.taskStorage)

	m := stream.NewMigration()
	m.ExtraFullBackupPaths = append(m.ExtraFullBackupPaths, c.metaPath())

	_, err := est.AppendMigration(ctx, m)
	if err != nil {
		return errors.Annotatef(err, "failed to add the extra backup at path %s", c.metaPath())
	}

	c.doWithCommittingLock(func() {
		c.resetCommitting()
	})
	// Persist the metadata in case of SSTs were uploaded but the meta wasn't,
	// which leads to a leakage.
	return c.persist(ctx)
}

func (c *pitrCollector) prepareMigIfNeeded(ctx context.Context) (err error) {
	c.putMigOnce.Do(func() {
		err = c.prepareMig(ctx)
	})
	return
}

func (c *pitrCollector) commit(ctx context.Context) error {
	c.committing.commit()
	return c.persist(ctx)
}

func (c *pitrCollector) resetCommitting() {
	c.committing = committing{
		rewrites: map[int64]int64{},
	}
	c.committing.msg.FilesPrefixHint = c.sstPath("")
	c.committing.msg.Finished = false
	c.committing.msg.BackupUuid = c.restoreUUID[:]
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
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "more than one task found, pitr collector doesn't support that")
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

	coll.resetCommitting()
	return coll, nil
}
