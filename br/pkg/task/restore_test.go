// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/restore"
)

type testRestoreSuite struct{}

var _ = Suite(&testRestoreSuite{})

func (s *testRestoreSuite) TestRestoreConfigAdjust(c *C) {
	cfg := &RestoreConfig{}
	cfg.adjustRestoreConfig()

	c.Assert(cfg.Config.Concurrency, Equals, uint32(defaultRestoreConcurrency))
	c.Assert(cfg.Config.SwitchModeInterval, Equals, defaultSwitchInterval)
	c.Assert(cfg.MergeSmallRegionKeyCount, Equals, restore.DefaultMergeRegionKeyCount)
	c.Assert(cfg.MergeSmallRegionSizeBytes, Equals, restore.DefaultMergeRegionSizeBytes)
}
