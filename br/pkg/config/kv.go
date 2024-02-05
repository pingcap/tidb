// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.
package config

import (
	"encoding/json"

	"github.com/docker/go-units"
)

type ConfigSet[T uint | uint64] struct {
	Value  T
	HasSet bool
}

type KVConfig struct {
	ImportGoroutines    ConfigSet[uint]
	MergeRegionSize     ConfigSet[uint64]
	MergeRegionKeyCount ConfigSet[uint64]
}

func ParseImportThreadsFromConfig(resp []byte) (uint, error) {
	type importer struct {
		Threads uint `json:"num-threads"`
	}

	type config struct {
		Import importer `json:"import"`
	}
	var c config
	e := json.Unmarshal(resp, &c)
	if e != nil {
		return 0, e
	}

	return c.Import.Threads, nil
}

func ParseMergeRegionSizeFromConfig(resp []byte) (uint64, uint64, error) {
	type coprocessor struct {
		RegionSplitSize string `json:"region-split-size"`
		RegionSplitKeys uint64 `json:"region-split-keys"`
	}

	type config struct {
		Cop coprocessor `json:"coprocessor"`
	}
	var c config
	e := json.Unmarshal(resp, &c)
	if e != nil {
		return 0, 0, e
	}
	rs, e := units.RAMInBytes(c.Cop.RegionSplitSize)
	if e != nil {
		return 0, 0, e
	}
	urs := uint64(rs)
	return urs, c.Cop.RegionSplitKeys, nil
}
