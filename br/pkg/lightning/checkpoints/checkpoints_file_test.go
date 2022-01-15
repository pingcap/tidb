package checkpoints_test

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&cpFileSuite{})

type cpFileSuite struct {
	cpdb *checkpoints.FileCheckpointsDB
}

func newTestConfig() *config.Config {
	cfg := config.NewConfig()
	cfg.Mydumper.SourceDir = "/data"
	cfg.TaskID = 123
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "127.0.0.1:2379"
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.Addr = "127.0.0.1:8287"
	cfg.TikvImporter.SortedKVDir = "/tmp/sorted-kv"
	return cfg
}

func (s *cpFileSuite) SetUpTest(c *C) {
	dir := c.MkDir()
	s.cpdb = checkpoints.NewFileCheckpointsDB(filepath.Join(dir, "cp.pb"))

	ctx := context.Background()
	cpdb := s.cpdb

	// 2. initialize with checkpoint data.
	cfg := newTestConfig()
	err := cpdb.Initialize(ctx, cfg, map[string]*checkpoints.TidbDBInfo{
		"db1": {
			Name: "db1",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"t1": {Name: "t1"},
				"t2": {Name: "t2"},
			},
		},
		"db2": {
			Name: "db2",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"t3": {Name: "t3"},
			},
		},
	})
	c.Assert(err, IsNil)

	// 3. set some checkpoints

	err = cpdb.InsertEngineCheckpoints(ctx, "`db1`.`t2`", map[int32]*checkpoints.EngineCheckpoint{
		0: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{{
				Key: checkpoints.ChunkCheckpointKey{
					Path:   "/tmp/path/1.sql",
					Offset: 0,
				},
				FileMeta: mydump.SourceFileMeta{
					Path:     "/tmp/path/1.sql",
					Type:     mydump.SourceTypeSQL,
					FileSize: 12345,
				},
				Chunk: mydump.Chunk{
					Offset:       12,
					EndOffset:    102400,
					PrevRowIDMax: 1,
					RowIDMax:     5000,
				},
			}},
		},
		-1: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: nil,
		},
	})
	c.Assert(err, IsNil)

	err = cpdb.InsertEngineCheckpoints(ctx, "`db2`.`t3`", map[int32]*checkpoints.EngineCheckpoint{
		-1: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: nil,
		},
	})
	c.Assert(err, IsNil)

	// 4. update some checkpoints

	cpd := checkpoints.NewTableCheckpointDiff()
	scm := checkpoints.StatusCheckpointMerger{
		EngineID: 0,
		Status:   checkpoints.CheckpointStatusImported,
	}
	scm.MergeInto(cpd)
	scm = checkpoints.StatusCheckpointMerger{
		EngineID: checkpoints.WholeTableEngineID,
		Status:   checkpoints.CheckpointStatusAllWritten,
	}
	scm.MergeInto(cpd)
	rcm := checkpoints.RebaseCheckpointMerger{
		AllocBase: 132861,
	}
	rcm.MergeInto(cpd)
	cksum := checkpoints.TableChecksumMerger{
		Checksum: verification.MakeKVChecksum(4492, 686, 486070148910),
	}
	cksum.MergeInto(cpd)
	ccm := checkpoints.ChunkCheckpointMerger{
		EngineID: 0,
		Key:      checkpoints.ChunkCheckpointKey{Path: "/tmp/path/1.sql", Offset: 0},
		Checksum: verification.MakeKVChecksum(4491, 586, 486070148917),
		Pos:      55904,
		RowID:    681,
	}
	ccm.MergeInto(cpd)

	cpdb.Update(map[string]*checkpoints.TableCheckpointDiff{"`db1`.`t2`": cpd})
}

func (s *cpFileSuite) TearDownTest(c *C) {
	c.Assert(s.cpdb.Close(), IsNil)
}

func (s *cpFileSuite) setInvalidStatus() {
	cpd := checkpoints.NewTableCheckpointDiff()
	scm := checkpoints.StatusCheckpointMerger{
		EngineID: -1,
		Status:   checkpoints.CheckpointStatusAllWritten,
	}
	scm.SetInvalid()
	scm.MergeInto(cpd)

	s.cpdb.Update(map[string]*checkpoints.TableCheckpointDiff{
		"`db1`.`t2`": cpd,
		"`db2`.`t3`": cpd,
	})
}

func (s *cpFileSuite) TestGet(c *C) {
	ctx := context.Background()

	// 5. get back the checkpoints

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp, DeepEquals, &checkpoints.TableCheckpoint{
		Status:    checkpoints.CheckpointStatusAllWritten,
		AllocBase: 132861,
		Checksum:  verification.MakeKVChecksum(4492, 686, 486070148910),
		Engines: map[int32]*checkpoints.EngineCheckpoint{
			-1: {
				Status: checkpoints.CheckpointStatusLoaded,
				Chunks: []*checkpoints.ChunkCheckpoint{},
			},
			0: {
				Status: checkpoints.CheckpointStatusImported,
				Chunks: []*checkpoints.ChunkCheckpoint{{
					Key: checkpoints.ChunkCheckpointKey{
						Path:   "/tmp/path/1.sql",
						Offset: 0,
					},
					FileMeta: mydump.SourceFileMeta{
						Path:     "/tmp/path/1.sql",
						Type:     mydump.SourceTypeSQL,
						FileSize: 12345,
					},
					ColumnPermutation: []int{},
					Chunk: mydump.Chunk{
						Offset:       55904,
						EndOffset:    102400,
						PrevRowIDMax: 681,
						RowIDMax:     5000,
					},
					Checksum: verification.MakeKVChecksum(4491, 586, 486070148917),
				}},
			},
		},
	})

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp, DeepEquals, &checkpoints.TableCheckpoint{
		Status: checkpoints.CheckpointStatusLoaded,
		Engines: map[int32]*checkpoints.EngineCheckpoint{
			-1: {
				Status: checkpoints.CheckpointStatusLoaded,
				Chunks: []*checkpoints.ChunkCheckpoint{},
			},
		},
	})

	cp, err = s.cpdb.Get(ctx, "`db3`.`not-exists`")
	c.Assert(cp, IsNil)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (s *cpFileSuite) TestRemoveAllCheckpoints(c *C) {
	ctx := context.Background()

	err := s.cpdb.RemoveCheckpoint(ctx, "all")
	c.Assert(err, IsNil)

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(cp, IsNil)
	c.Assert(errors.IsNotFound(err), IsTrue)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(cp, IsNil)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (s *cpFileSuite) TestRemoveOneCheckpoint(c *C) {
	ctx := context.Background()

	err := s.cpdb.RemoveCheckpoint(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(cp, IsNil)
	c.Assert(errors.IsNotFound(err), IsTrue)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, checkpoints.CheckpointStatusLoaded)
}

func (s *cpFileSuite) TestIgnoreAllErrorCheckpoints(c *C) {
	ctx := context.Background()

	s.setInvalidStatus()

	err := s.cpdb.IgnoreErrorCheckpoint(ctx, "all")
	c.Assert(err, IsNil)

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, checkpoints.CheckpointStatusLoaded)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, checkpoints.CheckpointStatusLoaded)
}

func (s *cpFileSuite) TestIgnoreOneErrorCheckpoints(c *C) {
	ctx := context.Background()

	s.setInvalidStatus()

	err := s.cpdb.IgnoreErrorCheckpoint(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, checkpoints.CheckpointStatusLoaded)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, checkpoints.CheckpointStatusAllWritten/10)
}

func (s *cpFileSuite) TestDestroyAllErrorCheckpoints(c *C) {
	ctx := context.Background()

	s.setInvalidStatus()

	dtc, err := s.cpdb.DestroyErrorCheckpoint(ctx, "all")
	c.Assert(err, IsNil)
	sort.Slice(dtc, func(i, j int) bool { return dtc[i].TableName < dtc[j].TableName })
	c.Assert(dtc, DeepEquals, []checkpoints.DestroyedTableCheckpoint{
		{
			TableName:   "`db1`.`t2`",
			MinEngineID: -1,
			MaxEngineID: 0,
		},
		{
			TableName:   "`db2`.`t3`",
			MinEngineID: -1,
			MaxEngineID: -1,
		},
	})

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(cp, IsNil)
	c.Assert(errors.IsNotFound(err), IsTrue)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(cp, IsNil)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (s *cpFileSuite) TestDestroyOneErrorCheckpoint(c *C) {
	ctx := context.Background()

	s.setInvalidStatus()

	dtc, err := s.cpdb.DestroyErrorCheckpoint(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(dtc, DeepEquals, []checkpoints.DestroyedTableCheckpoint{
		{
			TableName:   "`db1`.`t2`",
			MinEngineID: -1,
			MaxEngineID: 0,
		},
	})

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(cp, IsNil)
	c.Assert(errors.IsNotFound(err), IsTrue)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, checkpoints.CheckpointStatusAllWritten/10)
}
