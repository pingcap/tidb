// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"flag"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/pkg/metricutil"
	"github.com/pingcap/pd/pkg/typeutil"
	"github.com/pingcap/pd/server/namespace"
)

// Config is the pd server configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	Version bool `json:"-"`

	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`

	Name    string `toml:"name" json:"name"`
	DataDir string `toml:"data-dir" json:"data-dir"`

	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`

	// Join to an existing pd cluster, a string of endpoints.
	Join string `toml:"join" json:"join"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd onlys support seoncds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	// Log related config.
	Log logutil.LogConfig `toml:"log" json:"log"`

	// Backward compatibility.
	LogFileDeprecated  string `toml:"log-file" json:"log-file"`
	LogLevelDeprecated string `toml:"log-level" json:"log-level"`

	// TsoSaveInterval is the interval to save timestamp.
	TsoSaveInterval typeutil.Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	Schedule ScheduleConfig `toml:"schedule" json:"schedule"`

	Replication ReplicationConfig `toml:"replication" json:"replication"`

	Namespace map[string]NamespaceConfig `json:"namespace"`

	// QuotaBackendBytes Raise alarms when backend size exceeds the given quota. 0 means use the default quota.
	// the default size is 2GB, the maximum is 8GB.
	QuotaBackendBytes typeutil.ByteSize `toml:"quota-backend-bytes" json:"quota-backend-bytes"`
	// AutoCompactionRetention for mvcc key value store in hour. 0 means disable auto compaction.
	// the default retention is 1 hour
	AutoCompactionRetention int `toml:"auto-compaction-retention" json:"auto-compaction-retention"`

	// TickInterval is the interval for etcd Raft tick.
	TickInterval typeutil.Duration `toml:"tick-interval"`
	// ElectionInterval is the interval for etcd Raft election.
	ElectionInterval typeutil.Duration `toml:"election-interval"`

	Security SecurityConfig `toml:"security" json:"security"`

	configFile string

	// For all warnings during parsing.
	WarningMsgs []string

	// NamespaceClassifier is for classifying stores/regions into different
	// namespaces.
	NamespaceClassifier string `toml:"namespace-classifier" json:"namespace-classifier"`

	// Only test can change them.
	nextRetryDelay             time.Duration
	disableStrictReconfigCheck bool
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("pd", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.Version, "V", false, "print version information and exit")
	fs.BoolVar(&cfg.Version, "version", false, "print version information and exit")
	fs.StringVar(&cfg.configFile, "config", "", "Config file")

	fs.StringVar(&cfg.Name, "name", defaultName, "human-readable name for this pd member")

	fs.StringVar(&cfg.DataDir, "data-dir", "", "path to the data directory (default 'default.${name}')")
	fs.StringVar(&cfg.ClientUrls, "client-urls", defaultClientUrls, "url for client traffic")
	fs.StringVar(&cfg.AdvertiseClientUrls, "advertise-client-urls", "", "advertise url for client traffic (default '${client-urls}')")
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "url for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", "", "advertise url for peer traffic (default '${peer-urls}')")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")
	fs.StringVar(&cfg.Join, "join", "", "join to an existing cluster (usage: cluster's '${advertise-client-urls}'")

	fs.StringVar(&cfg.Log.Level, "L", "", "log level: debug, info, warn, error, fatal (default 'info')")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")
	fs.BoolVar(&cfg.Log.File.LogRotate, "log-rotate", true, "rotate log")
	fs.StringVar(&cfg.NamespaceClassifier, "namespace-classifier", "default", "namespace classifier (default 'default')")

	fs.StringVar(&cfg.Security.CAPath, "cacert", "", "Path of file that contains list of trusted TLS CAs")
	fs.StringVar(&cfg.Security.CertPath, "cert", "", "Path of file that contains X509 certificate in PEM format")
	fs.StringVar(&cfg.Security.KeyPath, "key", "", "Path of file that contains X509 key in PEM format")

	cfg.Namespace = make(map[string]NamespaceConfig)

	return cfg
}

const (
	defaultLeaderLease             = int64(3)
	defaultNextRetryDelay          = time.Second
	defaultAutoCompactionRetention = 1

	defaultName                = "pd"
	defaultClientUrls          = "http://127.0.0.1:2379"
	defaultPeerUrls            = "http://127.0.0.1:2380"
	defualtInitialClusterState = embed.ClusterStateFlagNew

	// etcd use 100ms for heartbeat and 1s for election timeout.
	// We can enlarge both a little to reduce the network aggression.
	// now embed etcd use TickMs for heartbeat, we will update
	// after embed etcd decouples tick and heartbeat.
	defaultTickInterval = 500 * time.Millisecond
	// embed etcd has a check that `5 * tick > election`
	defaultElectionInterval = 3000 * time.Millisecond
)

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustFloat64(v *float64, defValue float64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustDuration(v *typeutil.Duration, defValue time.Duration) {
	if v.Duration == 0 {
		v.Duration = defValue
	}
}

func adjustSchedulers(v *SchedulerConfigs, defValue SchedulerConfigs) {
	if len(*v) == 0 {
		*v = defValue
	}
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}

		// Backward compatibility for toml config
		if c.LogFileDeprecated != "" && c.Log.File.Filename == "" {
			c.Log.File.Filename = c.LogFileDeprecated
			msg := fmt.Sprintf("log-file in %s is deprecated, use [log.file] instead", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
		}
		if c.LogLevelDeprecated != "" && c.Log.Level == "" {
			c.Log.Level = c.LogLevelDeprecated
			msg := fmt.Sprintf("log-level in %s is deprecated, use [log] instead", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	err = c.adjust()
	return errors.Trace(err)
}

func (c *Config) validate() error {
	if c.Join != "" && c.InitialCluster != "" {
		return errors.New("-initial-cluster and -join can not be provided at the same time")
	}
	return nil
}

func (c *Config) adjust() error {
	if err := c.validate(); err != nil {
		return errors.Trace(err)
	}

	adjustString(&c.Name, defaultName)
	adjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))

	adjustString(&c.ClientUrls, defaultClientUrls)
	adjustString(&c.AdvertiseClientUrls, c.ClientUrls)
	adjustString(&c.PeerUrls, defaultPeerUrls)
	adjustString(&c.AdvertisePeerUrls, c.PeerUrls)

	if len(c.InitialCluster) == 0 {
		// The advertise peer urls may be http://127.0.0.1:2380,http://127.0.0.1:2381
		// so the initial cluster is pd=http://127.0.0.1:2380,pd=http://127.0.0.1:2381
		items := strings.Split(c.AdvertisePeerUrls, ",")

		sep := ""
		for _, item := range items {
			c.InitialCluster += fmt.Sprintf("%s%s=%s", sep, c.Name, item)
			sep = ","
		}
	}

	adjustString(&c.InitialClusterState, defualtInitialClusterState)

	adjustInt64(&c.LeaderLease, defaultLeaderLease)

	adjustDuration(&c.TsoSaveInterval, time.Duration(defaultLeaderLease)*time.Second)

	if c.nextRetryDelay == 0 {
		c.nextRetryDelay = defaultNextRetryDelay
	}
	if c.AutoCompactionRetention == 0 {
		c.AutoCompactionRetention = defaultAutoCompactionRetention
	}

	adjustDuration(&c.TickInterval, defaultTickInterval)
	adjustDuration(&c.ElectionInterval, defaultElectionInterval)

	adjustString(&c.NamespaceClassifier, "default")

	adjustString(&c.Metric.PushJob, c.Name)

	c.Schedule.adjust()
	c.Replication.adjust()
	return nil
}

func (c *Config) clone() *Config {
	cfg := &Config{}
	*cfg = *c
	return cfg
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// ScheduleConfig is the schedule configuration.
type ScheduleConfig struct {
	// If the snapshot count of one store is greater than this value,
	// it will never be used as a source or target store.
	MaxSnapshotCount    uint64 `toml:"max-snapshot-count,omitempty" json:"max-snapshot-count"`
	MaxPendingPeerCount uint64 `toml:"max-pending-peer-count,omitempty" json:"max-pending-peer-count"`
	// MaxStoreDownTime is the max duration after which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownTime typeutil.Duration `toml:"max-store-down-time,omitempty" json:"max-store-down-time"`
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `toml:"leader-schedule-limit,omitempty" json:"leader-schedule-limit"`
	// RegionScheduleLimit is the max coexist region schedules.
	RegionScheduleLimit uint64 `toml:"region-schedule-limit,omitempty" json:"region-schedule-limit"`
	// ReplicaScheduleLimit is the max coexist replica schedules.
	ReplicaScheduleLimit uint64 `toml:"replica-schedule-limit,omitempty" json:"replica-schedule-limit"`
	// TolerantSizeRatio is the ratio of buffer size for balance scheduler.
	TolerantSizeRatio float64 `toml:"tolerant-size-ratio,omitempty" json:"tolerant-size-ratio"`
	// Schedulers support for loding customized schedulers
	Schedulers SchedulerConfigs `toml:"schedulers,omitempty" json:"schedulers-v2"` // json v2 is for the sake of compatible upgrade
}

func (c *ScheduleConfig) clone() *ScheduleConfig {
	schedulers := make(SchedulerConfigs, len(c.Schedulers))
	copy(schedulers, c.Schedulers)
	return &ScheduleConfig{
		MaxSnapshotCount:     c.MaxSnapshotCount,
		MaxStoreDownTime:     c.MaxStoreDownTime,
		LeaderScheduleLimit:  c.LeaderScheduleLimit,
		RegionScheduleLimit:  c.RegionScheduleLimit,
		ReplicaScheduleLimit: c.ReplicaScheduleLimit,
		TolerantSizeRatio:    c.TolerantSizeRatio,
		Schedulers:           schedulers,
	}
}

// SchedulerConfigs is a slice of customized scheduler configuration.
type SchedulerConfigs []SchedulerConfig

// SchedulerConfig is customized scheduler configuration
type SchedulerConfig struct {
	Type string   `toml:"type" json:"type"`
	Args []string `toml:"args,omitempty" json:"args"`
}

const (
	defaultMaxReplicas          = 3
	defaultMaxSnapshotCount     = 3
	defaultMaxPendingPeerCount  = 16
	defaultMaxStoreDownTime     = time.Hour
	defaultLeaderScheduleLimit  = 64
	defaultRegionScheduleLimit  = 12
	defaultReplicaScheduleLimit = 16
	defaultTolerantSizeRatio    = 2.5
)

var defaultSchedulers = SchedulerConfigs{
	{Type: "balance-region"},
	{Type: "balance-leader"},
	{Type: "hot-region"},
}

func (c *ScheduleConfig) adjust() {
	adjustUint64(&c.MaxSnapshotCount, defaultMaxSnapshotCount)
	adjustUint64(&c.MaxPendingPeerCount, defaultMaxPendingPeerCount)
	adjustDuration(&c.MaxStoreDownTime, defaultMaxStoreDownTime)
	adjustUint64(&c.LeaderScheduleLimit, defaultLeaderScheduleLimit)
	adjustUint64(&c.RegionScheduleLimit, defaultRegionScheduleLimit)
	adjustUint64(&c.ReplicaScheduleLimit, defaultReplicaScheduleLimit)
	adjustFloat64(&c.TolerantSizeRatio, defaultTolerantSizeRatio)
	adjustSchedulers(&c.Schedulers, defaultSchedulers)
}

// ReplicationConfig is the replication configuration.
type ReplicationConfig struct {
	// MaxReplicas is the number of replicas for each region.
	MaxReplicas uint64 `toml:"max-replicas,omitempty" json:"max-replicas"`

	// The label keys specified the location of a store.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LocationLabels typeutil.StringSlice `toml:"location-labels,omitempty" json:"location-labels"`
}

func (c *ReplicationConfig) clone() *ReplicationConfig {
	locationLabels := make(typeutil.StringSlice, len(c.LocationLabels))
	copy(locationLabels, c.LocationLabels)
	return &ReplicationConfig{
		MaxReplicas:    c.MaxReplicas,
		LocationLabels: locationLabels,
	}
}

func (c *ReplicationConfig) adjust() {
	adjustUint64(&c.MaxReplicas, defaultMaxReplicas)
}

// NamespaceConfig is to overwrite the global setting for specific namespace
type NamespaceConfig struct {
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `json:"leader-schedule-limit"`
	// RegionScheduleLimit is the max coexist region schedules.
	RegionScheduleLimit uint64 `json:"region-schedule-limit"`
	// ReplicaScheduleLimit is the max coexist replica schedules.
	ReplicaScheduleLimit uint64 `json:"replica-schedule-limit"`
	// MaxReplicas is the number of replicas for each region.
	MaxReplicas uint64 `json:"max-replicas"`
}

func (c *NamespaceConfig) clone() *NamespaceConfig {
	return &NamespaceConfig{
		LeaderScheduleLimit:  c.LeaderScheduleLimit,
		RegionScheduleLimit:  c.RegionScheduleLimit,
		ReplicaScheduleLimit: c.ReplicaScheduleLimit,
		MaxReplicas:          c.MaxReplicas,
	}
}

func (c *NamespaceConfig) adjust(opt *scheduleOption) {
	adjustUint64(&c.LeaderScheduleLimit, opt.GetLeaderScheduleLimit(namespace.DefaultNamespace))
	adjustUint64(&c.RegionScheduleLimit, opt.GetRegionScheduleLimit(namespace.DefaultNamespace))
	adjustUint64(&c.ReplicaScheduleLimit, opt.GetReplicaScheduleLimit(namespace.DefaultNamespace))
	adjustUint64(&c.MaxReplicas, uint64(opt.GetMaxReplicas(namespace.DefaultNamespace)))
}

// SecurityConfig is the configuration for supporting tls.
type SecurityConfig struct {
	// CAPath is the path of file that contains list of trusted SSL CAs. if set, following four settings shouldn't be empty
	CAPath string `toml:"cacert-path" json:"cacert-path"`
	// CertPath is the path of file that contains X509 certificate in PEM format.
	CertPath string `toml:"cert-path" json:"cert-path"`
	// KeyPath is the path of file that contains X509 key in PEM format.
	KeyPath string `toml:"key-path" json:"key-path"`
}

// ParseUrls parse a string into multiple urls.
// Export for api.
func ParseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.Trace(err)
		}

		urls = append(urls, *u)
	}

	return urls, nil
}

// generates a configuration for embedded etcd.
func (c *Config) genEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.EnablePprof = true
	cfg.StrictReconfigCheck = !c.disableStrictReconfigCheck
	cfg.TickMs = uint(c.TickInterval.Duration / time.Millisecond)
	cfg.ElectionMs = uint(c.ElectionInterval.Duration / time.Millisecond)
	cfg.AutoCompactionRetention = c.AutoCompactionRetention
	cfg.QuotaBackendBytes = int64(c.QuotaBackendBytes)

	cfg.ClientTLSInfo.ClientCertAuth = len(c.Security.CAPath) != 0
	cfg.ClientTLSInfo.TrustedCAFile = c.Security.CAPath
	cfg.ClientTLSInfo.CertFile = c.Security.CertPath
	cfg.ClientTLSInfo.KeyFile = c.Security.KeyPath
	cfg.PeerTLSInfo.TrustedCAFile = c.Security.CAPath
	cfg.PeerTLSInfo.CertFile = c.Security.CertPath
	cfg.PeerTLSInfo.KeyFile = c.Security.KeyPath

	var err error

	cfg.LPUrls, err = ParseUrls(c.PeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.APUrls, err = ParseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LCUrls, err = ParseUrls(c.ClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.ACUrls, err = ParseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}
