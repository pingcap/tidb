// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"encoding/json"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
)

// EBSVolume is passed by TiDB deployment tools: TiDB Operator and TiUP(in future)
// we should do snapshot inside BR, because we need some logic to determine the order of snapshot starts.
// TODO finish the info with TiDB Operator developer.
type EBSVolume struct {
	ID string `json:"id" toml:"id"`
	Type string `json:"type" toml:"type"`
}

type EBSStore struct {
	Volumes []*EBSVolume `json:"volumes" toml:"volumes"`
}

type EBSBackupConfig struct {
	Stores map[string]*EBSStore `json:"stores" toml:"stores"`
}

func (c *EBSBackupConfig) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}

// ConfigFromFile loads config from file.
func (c *EBSBackupConfig) ConfigFromFile(path string) error {
	meta, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
	}
	if len(meta.Undecoded()) > 0 {
		return errors.Errorf("unknown keys in config file %s: %v", path, meta.Undecoded())
	}
	return nil
}
