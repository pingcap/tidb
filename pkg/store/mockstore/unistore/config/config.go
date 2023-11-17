// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"time"

	"github.com/pingcap/badger/options"
	"github.com/pingcap/log"
)

// Config contains configuration options.
type Config struct {
	Server         Server         `toml:"server"`          // Unistore server options
	Engine         Engine         `toml:"engine"`          // Engine options.
	RaftStore      RaftStore      `toml:"raftstore"`       // RaftStore configs
	Coprocessor    Coprocessor    `toml:"coprocessor"`     // Coprocessor options
	PessimisticTxn PessimisticTxn `toml:"pessimistic-txn"` // Pessimistic txn related
}

// Server is the config for server.
type Server struct {
	PDAddr      string `toml:"pd-addr"`
	StoreAddr   string `toml:"store-addr"`
	StatusAddr  string `toml:"status-addr"`
	LogLevel    string `toml:"log-level"`
	RegionSize  int64  `toml:"region-size"` // Average region size.
	MaxProcs    int    `toml:"max-procs"`   // Max CPU cores to use, set 0 to use all CPU cores in the machine.
	Raft        bool   `toml:"raft"`        // Enable raft.
	LogfilePath string `toml:"log-file"`    // Log file path for unistore server
}

// RaftStore is the config for raft store.
type RaftStore struct {
	PdHeartbeatTickInterval  string `toml:"pd-heartbeat-tick-interval"`  // pd-heartbeat-tick-interval in seconds
	RaftStoreMaxLeaderLease  string `toml:"raft-store-max-leader-lease"` // raft-store-max-leader-lease in milliseconds
	RaftBaseTickInterval     string `toml:"raft-base-tick-interval"`     // raft-base-tick-interval in milliseconds
	RaftHeartbeatTicks       int    `toml:"raft-heartbeat-ticks"`        // raft-heartbeat-ticks times
	RaftElectionTimeoutTicks int    `toml:"raft-election-timeout-ticks"` // raft-election-timeout-ticks times
	CustomRaftLog            bool   `toml:"custom-raft-log"`
}

// Coprocessor is the config for coprocessor.
type Coprocessor struct {
	RegionMaxKeys   int64 `toml:"region-max-keys"`
	RegionSplitKeys int64 `toml:"region-split-keys"`
}

// Engine is the config for engine.
type Engine struct {
	DBPath           string `toml:"db-path"`            // Directory to store the data in. Should exist and be writable.
	ValueThreshold   int    `toml:"value-threshold"`    // If value size >= this threshold, only store value offsets in tree.
	MaxMemTableSize  int64  `toml:"max-mem-table-size"` // Each mem table is at most this size.
	MaxTableSize     int64  `toml:"max-table-size"`     // Each table file is at most this size.
	L1Size           int64  `toml:"l1-size"`
	NumMemTables     int    `toml:"num-mem-tables"`      // Maximum number of tables to keep in memory, before stalling.
	NumL0Tables      int    `toml:"num-L0-tables"`       // Maximum number of Level 0 tables before we start compacting.
	NumL0TablesStall int    `toml:"num-L0-tables-stall"` // Maximum number of Level 0 tables before stalling.
	VlogFileSize     int64  `toml:"vlog-file-size"`      // Value log file size.

	// 	Sync all writes to disk. Setting this to true would slow down data loading significantly.")
	SyncWrite         bool     `toml:"sync-write"`
	NumCompactors     int      `toml:"num-compactors"`
	SurfStartLevel    int      `toml:"surf-start-level"`
	BlockCacheSize    int64    `toml:"block-cache-size"`
	IndexCacheSize    int64    `toml:"index-cache-size"`
	Compression       []string `toml:"compression"` // Compression types for each level
	IngestCompression string   `toml:"ingest-compression"`

	// Only used in tests.
	VolatileMode bool

	CompactL0WhenClose bool `toml:"compact-l0-when-close"`
}

// PessimisticTxn is the config for pessimistic txn.
type PessimisticTxn struct {
	// The default and maximum delay in milliseconds before responding to TiDB when pessimistic
	// transactions encounter locks
	WaitForLockTimeout int64 `toml:"wait-for-lock-timeout"`

	// The duration between waking up lock waiter, in milliseconds
	WakeUpDelayDuration int64 `toml:"wake-up-delay-duration"`
}

// ParseCompression parses the string s and returns a compression type.
func ParseCompression(s string) options.CompressionType {
	switch s {
	case "snappy":
		return options.Snappy
	case "zstd":
		return options.ZSTD
	default:
		return options.None
	}
}

// MB represents the MB size.
const MB = 1024 * 1024

// DefaultConf returns the default configuration.
var DefaultConf = Config{
	Server: Server{
		PDAddr:      "127.0.0.1:2379",
		StoreAddr:   "127.0.0.1:9191",
		StatusAddr:  "127.0.0.1:9291",
		RegionSize:  64 * MB,
		LogLevel:    "info",
		MaxProcs:    0,
		Raft:        true,
		LogfilePath: "",
	},
	RaftStore: RaftStore{
		PdHeartbeatTickInterval:  "20s",
		RaftStoreMaxLeaderLease:  "9s",
		RaftBaseTickInterval:     "1s",
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		CustomRaftLog:            true,
	},
	Engine: Engine{
		DBPath:             "/tmp/badger",
		ValueThreshold:     256,
		MaxMemTableSize:    64 * MB,
		MaxTableSize:       8 * MB,
		NumMemTables:       3,
		NumL0Tables:        4,
		NumL0TablesStall:   8,
		VlogFileSize:       256 * MB,
		NumCompactors:      3,
		SurfStartLevel:     8,
		L1Size:             512 * MB,
		Compression:        make([]string, 7),
		BlockCacheSize:     0, // 0 means disable block cache, use mmap to access sst.
		IndexCacheSize:     0,
		CompactL0WhenClose: true,
	},
	Coprocessor: Coprocessor{
		RegionMaxKeys:   1440000,
		RegionSplitKeys: 960000,
	},
	PessimisticTxn: PessimisticTxn{
		WaitForLockTimeout:  1000, // 1000ms same with tikv default value
		WakeUpDelayDuration: 100,  // 100ms same with tikv default value
	},
}

// ParseDuration parses duration argument string.
func ParseDuration(durationStr string) time.Duration {
	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		dur, err = time.ParseDuration(durationStr + "s")
	}
	if err != nil || dur < 0 {
		log.S().Fatalf("invalid duration=%v", durationStr)
	}
	return dur
}
