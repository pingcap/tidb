// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"math"

	. "github.com/pingcap/check"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/util/testleak"
)

type testLogRestoreSuite struct {
	mock *mock.Cluster

	client *restore.LogClient
}

var _ = Suite(&testLogRestoreSuite{})

func (s *testLogRestoreSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
	restoreClient, err := restore.NewRestoreClient(
		gluetidb.New(), s.mock.PDClient, s.mock.Storage, nil, defaultKeepaliveCfg)
	c.Assert(err, IsNil)

	s.client, err = restore.NewLogRestoreClient(
		context.Background(),
		restoreClient,
		0,
		math.MaxInt64,
		filter.NewSchemasFilter("test"),
		8,
		16,
		5<<20,
		16,
	)
	c.Assert(err, IsNil)
}

func (s *testLogRestoreSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testLogRestoreSuite) TestTsInRange(c *C) {
	fileName1 := "cdclog.1"
	s.client.ResetTSRange(1, 2)
	collected, err := s.client.NeedRestoreRowChange(fileName1)
	c.Assert(err, IsNil)
	c.Assert(collected, IsTrue)

	// cdclog.3 may have events in [1, 2]
	// so we should collect it.
	fileName2 := "cdclog.3"
	s.client.ResetTSRange(1, 2)
	collected, err = s.client.NeedRestoreRowChange(fileName2)
	c.Assert(err, IsNil)
	c.Assert(collected, IsTrue)

	fileName3 := "cdclog.3"
	s.client.ResetTSRange(4, 5)
	collected, err = s.client.NeedRestoreRowChange(fileName3)
	c.Assert(err, IsNil)
	c.Assert(collected, IsFalse)

	// format cdclog will collect, because file sink will generate cdclog for streaming write.
	fileName4 := "cdclog"
	collected, err = s.client.NeedRestoreRowChange(fileName4)
	c.Assert(err, IsNil)
	c.Assert(collected, IsTrue)

	for _, fileName := range []string{"cdclog.3.1", "cdclo.3"} {
		// wrong format won't collect
		collected, err = s.client.NeedRestoreRowChange(fileName)
		c.Assert(err, IsNil)
		c.Assert(collected, IsFalse)
	}

	// format cdclog will collect, because file sink will generate cdclog for streaming write.
	ddlFile := "ddl.18446744073709551615"
	collected, err = s.client.NeedRestoreDDL(ddlFile)
	c.Assert(err, IsNil)
	c.Assert(collected, IsTrue)

	for _, fileName := range []string{"ddl", "dld.1"} {
		// wrong format won't collect
		collected, err = s.client.NeedRestoreDDL(fileName)
		c.Assert(err, IsNil)
		c.Assert(collected, IsFalse)
	}

	s.client.ResetTSRange(424839867765096449, 424839886560821249)
	// ddl suffix records the first event's commit ts

	// the file name include the end ts, collect it.(maxUint64 - 424839886560821249)
	ddlFile = "ddl.18021904187148730366"
	collected, err = s.client.NeedRestoreDDL(ddlFile)
	c.Assert(err, IsNil)
	c.Assert(collected, IsTrue)

	// the file name include the start ts, collect it.(maxUint64 - 424839867765096449)
	ddlFile = "ddl.18021904205944455166"
	collected, err = s.client.NeedRestoreDDL(ddlFile)
	c.Assert(err, IsNil)
	c.Assert(collected, IsTrue)

	// the file first event's ts is smaller than the start ts, collect it.
	// because we only know this file's first event not in TSRange.
	// FIXME find a unified logic for collection.
	ddlFile = "ddl.18021904205944455167"
	collected, err = s.client.NeedRestoreDDL(ddlFile)
	c.Assert(err, IsNil)
	c.Assert(collected, IsTrue)

	// the file first event's ts is large than end ts, skip it.
	ddlFile = "ddl.18021904187148730365"
	collected, err = s.client.NeedRestoreDDL(ddlFile)
	c.Assert(err, IsNil)
	c.Assert(collected, IsFalse)
}
