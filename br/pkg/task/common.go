// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/docker/go-units"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
	"google.golang.org/grpc/keepalive"
)

const (
	// flagSendCreds specify whether to send credentials to tikv
	flagSendCreds = "send-credentials-to-tikv"
	// No credentials specifies that cloud credentials should not be loaded
	flagNoCreds = "no-credentials"
	// flagStorage is the name of storage flag.
	flagStorage = "storage"
	// flagPD is the name of PD url flag.
	flagPD = "pd"
	// flagCA is the name of TLS CA flag.
	flagCA = "ca"
	// flagCert is the name of TLS cert flag.
	flagCert = "cert"
	// flagKey is the name of TLS key flag.
	flagKey = "key"

	flagDatabase = "db"
	flagTable    = "table"

	flagChecksumConcurrency = "checksum-concurrency"
	flagRateLimit           = "ratelimit"
	flagRateLimitUnit       = "ratelimit-unit"
	flagConcurrency         = "concurrency"
	flagChecksum            = "checksum"
	flagFilter              = "filter"
	flagCaseSensitive       = "case-sensitive"
	flagRemoveTiFlash       = "remove-tiflash"
	flagCheckRequirement    = "check-requirements"
	flagSwitchModeInterval  = "switch-mode-interval"
	// flagGrpcKeepaliveTime is the interval of pinging the server.
	flagGrpcKeepaliveTime = "grpc-keepalive-time"
	// flagGrpcKeepaliveTimeout is the max time a grpc conn can keep idel before killed.
	flagGrpcKeepaliveTimeout = "grpc-keepalive-timeout"
	// flagEnableOpenTracing is whether to enable opentracing
	flagEnableOpenTracing = "enable-opentracing"
	flagSkipCheckPath     = "skip-check-path"

	defaultSwitchInterval       = 5 * time.Minute
	defaultGRPCKeepaliveTime    = 10 * time.Second
	defaultGRPCKeepaliveTimeout = 3 * time.Second

	unlimited = 0
)

// TLSConfig is the common configuration for TLS connection.
type TLSConfig struct {
	CA   string `json:"ca" toml:"ca"`
	Cert string `json:"cert" toml:"cert"`
	Key  string `json:"key" toml:"key"`
}

// IsEnabled checks if TLS open or not.
func (tls *TLSConfig) IsEnabled() bool {
	return tls.CA != ""
}

// ToTLSConfig generate tls.Config.
func (tls *TLSConfig) ToTLSConfig() (*tls.Config, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      tls.Cert,
		KeyFile:       tls.Key,
		TrustedCAFile: tls.CA,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tlsConfig, nil
}

// Config is the common configuration for all BRIE tasks.
type Config struct {
	storage.BackendOptions

	Storage             string    `json:"storage" toml:"storage"`
	PD                  []string  `json:"pd" toml:"pd"`
	TLS                 TLSConfig `json:"tls" toml:"tls"`
	RateLimit           uint64    `json:"rate-limit" toml:"rate-limit"`
	ChecksumConcurrency uint      `json:"checksum-concurrency" toml:"checksum-concurrency"`
	Concurrency         uint32    `json:"concurrency" toml:"concurrency"`
	Checksum            bool      `json:"checksum" toml:"checksum"`
	SendCreds           bool      `json:"send-credentials-to-tikv" toml:"send-credentials-to-tikv"`
	// LogProgress is true means the progress bar is printed to the log instead of stdout.
	LogProgress bool `json:"log-progress" toml:"log-progress"`

	// CaseSensitive should not be used.
	//
	// Deprecated: This field is kept only to satisfy the cyclic dependency with TiDB. This field
	// should be removed after TiDB upgrades the BR dependency.
	CaseSensitive bool

	// NoCreds means don't try to load cloud credentials
	NoCreds bool `json:"no-credentials" toml:"no-credentials"`

	CheckRequirements bool `json:"check-requirements" toml:"check-requirements"`
	// EnableOpenTracing is whether to enable opentracing
	EnableOpenTracing bool `json:"enable-opentracing" toml:"enable-opentracing"`
	// SkipCheckPath skips verifying the path
	// deprecated
	SkipCheckPath bool `json:"skip-check-path" toml:"skip-check-path"`
	// Filter should not be used, use TableFilter instead.
	//
	// Deprecated: This field is kept only to satisfy the cyclic dependency with TiDB. This field
	// should be removed after TiDB upgrades the BR dependency.
	Filter filter.MySQLReplicationRules

	TableFilter        filter.Filter `json:"-" toml:"-"`
	SwitchModeInterval time.Duration `json:"switch-mode-interval" toml:"switch-mode-interval"`
	// Schemas is a database name set, to check whether the restore database has been backup
	Schemas map[string]struct{}
	// Tables is a table name set, to check whether the restore table has been backup
	Tables map[string]struct{}

	// GrpcKeepaliveTime is the interval of pinging the server.
	GRPCKeepaliveTime time.Duration `json:"grpc-keepalive-time" toml:"grpc-keepalive-time"`
	// GrpcKeepaliveTimeout is the max time a grpc conn can keep idel before killed.
	GRPCKeepaliveTimeout time.Duration `json:"grpc-keepalive-timeout" toml:"grpc-keepalive-timeout"`
}

// DefineCommonFlags defines the flags common to all BRIE commands.
func DefineCommonFlags(flags *pflag.FlagSet) {
	flags.BoolP(flagSendCreds, "c", true, "Whether send credentials to tikv")
	flags.StringP(flagStorage, "s", "", `specify the url where backup storage, eg, "s3://bucket/path/prefix"`)
	flags.StringSliceP(flagPD, "u", []string{"127.0.0.1:2379"}, "PD address")
	flags.String(flagCA, "", "CA certificate path for TLS connection")
	flags.String(flagCert, "", "Certificate path for TLS connection")
	flags.String(flagKey, "", "Private key path for TLS connection")
	flags.Uint(flagChecksumConcurrency, variable.DefChecksumTableConcurrency, "The concurrency of table checksumming")
	_ = flags.MarkHidden(flagChecksumConcurrency)

	flags.Uint64(flagRateLimit, unlimited, "The rate limit of the task, MB/s per node")
	flags.Bool(flagChecksum, true, "Run checksum at end of task")
	flags.Bool(flagRemoveTiFlash, true,
		"Remove TiFlash replicas before backup or restore, for unsupported versions of TiFlash")

	// Default concurrency is different for backup and restore.
	// Leave it 0 and let them adjust the value.
	flags.Uint32(flagConcurrency, 0, "The size of thread pool on each node that executes the task")
	// It may confuse users , so just hide it.
	_ = flags.MarkHidden(flagConcurrency)

	flags.Uint64(flagRateLimitUnit, units.MiB, "The unit of rate limit")
	_ = flags.MarkHidden(flagRateLimitUnit)
	_ = flags.MarkDeprecated(flagRemoveTiFlash,
		"TiFlash is fully supported by BR now, removing TiFlash isn't needed any more. This flag would be ignored.")

	flags.Bool(flagCheckRequirement, true,
		"Whether start version check before execute command")
	flags.Duration(flagSwitchModeInterval, defaultSwitchInterval, "maintain import mode on TiKV during restore")
	flags.Duration(flagGrpcKeepaliveTime, defaultGRPCKeepaliveTime,
		"the interval of pinging gRPC peer, must keep the same value with TiKV and PD")
	flags.Duration(flagGrpcKeepaliveTimeout, defaultGRPCKeepaliveTimeout,
		"the max time a gRPC connection can keep idle before killed, must keep the same value with TiKV and PD")
	_ = flags.MarkHidden(flagGrpcKeepaliveTime)
	_ = flags.MarkHidden(flagGrpcKeepaliveTimeout)

	flags.Bool(flagEnableOpenTracing, false,
		"Set whether to enable opentracing during the backup/restore process")

	flags.BoolP(flagNoCreds, "", false, "Don't load credentials")
	_ = flags.MarkHidden(flagNoCreds)
	flags.BoolP(flagSkipCheckPath, "", false, "Skip path verification")
	_ = flags.MarkHidden(flagSkipCheckPath)

	storage.DefineFlags(flags)
}

// DefineDatabaseFlags defines the required --db flag for `db` subcommand.
func DefineDatabaseFlags(command *cobra.Command) {
	command.Flags().String(flagDatabase, "", "database name")
	_ = command.MarkFlagRequired(flagDatabase)
}

// DefineTableFlags defines the required --db and --table flags for `table` subcommand.
func DefineTableFlags(command *cobra.Command) {
	DefineDatabaseFlags(command)
	command.Flags().StringP(flagTable, "t", "", "table name")
	_ = command.MarkFlagRequired(flagTable)
}

// DefineFilterFlags defines the --filter and --case-sensitive flags for `full` subcommand.
func DefineFilterFlags(command *cobra.Command, defaultFilter []string) {
	flags := command.Flags()
	flags.StringArrayP(flagFilter, "f", defaultFilter, "select tables to process")
	flags.Bool(flagCaseSensitive, false, "whether the table names used in --filter should be case-sensitive")
}

// ParseFromFlags parses the TLS config from the flag set.
func (tls *TLSConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	tls.CA, tls.Cert, tls.Key, err = ParseTLSTripleFromFlags(flags)
	return err
}

// ParseTLSTripleFromFlags parses the (ca, cert, key) triple from flags.
func ParseTLSTripleFromFlags(flags *pflag.FlagSet) (ca, cert, key string, err error) {
	ca, err = flags.GetString(flagCA)
	if err != nil {
		return
	}
	cert, err = flags.GetString(flagCert)
	if err != nil {
		return
	}
	key, err = flags.GetString(flagKey)
	if err != nil {
		return
	}
	return
}

func (cfg *Config) normalizePDURLs() error {
	for i := range cfg.PD {
		var err error
		cfg.PD[i], err = normalizePDURL(cfg.PD[i], cfg.TLS.IsEnabled())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ParseFromFlags parses the config from the flag set.
func (cfg *Config) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	if cfg.Storage, err = flags.GetString(flagStorage); err != nil {
		return errors.Trace(err)
	}
	if cfg.SendCreds, err = flags.GetBool(flagSendCreds); err != nil {
		return errors.Trace(err)
	}
	if cfg.NoCreds, err = flags.GetBool(flagNoCreds); err != nil {
		return errors.Trace(err)
	}
	if cfg.Concurrency, err = flags.GetUint32(flagConcurrency); err != nil {
		return errors.Trace(err)
	}
	if cfg.Checksum, err = flags.GetBool(flagChecksum); err != nil {
		return errors.Trace(err)
	}
	if cfg.ChecksumConcurrency, err = flags.GetUint(flagChecksumConcurrency); err != nil {
		return errors.Trace(err)
	}

	var rateLimit, rateLimitUnit uint64
	if rateLimit, err = flags.GetUint64(flagRateLimit); err != nil {
		return errors.Trace(err)
	}
	if rateLimitUnit, err = flags.GetUint64(flagRateLimitUnit); err != nil {
		return errors.Trace(err)
	}
	cfg.RateLimit = rateLimit * rateLimitUnit

	cfg.Schemas = make(map[string]struct{})
	cfg.Tables = make(map[string]struct{})
	var caseSensitive bool
	if filterFlag := flags.Lookup(flagFilter); filterFlag != nil {
		var f filter.Filter
		f, err = filter.Parse(filterFlag.Value.(pflag.SliceValue).GetSlice())
		if err != nil {
			return errors.Trace(err)
		}
		cfg.TableFilter = f
		caseSensitive, err = flags.GetBool(flagCaseSensitive)
		if err != nil {
			return errors.Trace(err)
		}
	} else if dbFlag := flags.Lookup(flagDatabase); dbFlag != nil {
		db := dbFlag.Value.String()
		if len(db) == 0 {
			return errors.Annotate(berrors.ErrInvalidArgument, "empty database name is not allowed")
		}
		cfg.Schemas[utils.EncloseName(db)] = struct{}{}
		if tblFlag := flags.Lookup(flagTable); tblFlag != nil {
			tbl := tblFlag.Value.String()
			if len(tbl) == 0 {
				return errors.Annotate(berrors.ErrInvalidArgument, "empty table name is not allowed")
			}
			cfg.Tables[utils.EncloseDBAndTable(db, tbl)] = struct{}{}
			cfg.TableFilter = filter.NewTablesFilter(filter.Table{
				Schema: db,
				Name:   tbl,
			})
		} else {
			cfg.TableFilter = filter.NewSchemasFilter(db)
		}
	} else {
		cfg.TableFilter, _ = filter.Parse([]string{"*.*"})
	}
	if !caseSensitive {
		cfg.TableFilter = filter.CaseInsensitive(cfg.TableFilter)
	}
	checkRequirements, err := flags.GetBool(flagCheckRequirement)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CheckRequirements = checkRequirements

	cfg.SwitchModeInterval, err = flags.GetDuration(flagSwitchModeInterval)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.GRPCKeepaliveTime, err = flags.GetDuration(flagGrpcKeepaliveTime)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.GRPCKeepaliveTimeout, err = flags.GetDuration(flagGrpcKeepaliveTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.EnableOpenTracing, err = flags.GetBool(flagEnableOpenTracing)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.SwitchModeInterval <= 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--switch-mode-interval must be positive, %s is not allowed", cfg.SwitchModeInterval)
	}

	if err = cfg.BackendOptions.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	if err = cfg.TLS.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	cfg.PD, err = flags.GetStringSlice(flagPD)
	if err != nil {
		return errors.Trace(err)
	}
	if len(cfg.PD) == 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "must provide at least one PD server address")
	}
	if cfg.SkipCheckPath, err = flags.GetBool(flagSkipCheckPath); err != nil {
		return errors.Trace(err)
	}
	return cfg.normalizePDURLs()
}

// NewMgr creates a new mgr at the given PD address.
func NewMgr(ctx context.Context,
	g glue.Glue, pds []string,
	tlsConfig TLSConfig,
	keepalive keepalive.ClientParameters,
	checkRequirements bool,
	needDomain bool,
) (*conn.Mgr, error) {
	var (
		tlsConf *tls.Config
		err     error
	)
	pdAddress := strings.Join(pds, ",")
	if len(pdAddress) == 0 {
		return nil, errors.Annotate(berrors.ErrInvalidArgument, "pd address can not be empty")
	}

	securityOption := pd.SecurityOption{}
	if tlsConfig.IsEnabled() {
		securityOption.CAPath = tlsConfig.CA
		securityOption.CertPath = tlsConfig.Cert
		securityOption.KeyPath = tlsConfig.Key
		tlsConf, err = tlsConfig.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Disable GC because TiDB enables GC already.
	store, err := g.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddress), securityOption)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Is it necessary to remove `StoreBehavior`?
	return conn.NewMgr(
		ctx, g, pdAddress, store, tlsConf, securityOption, keepalive, conn.SkipTiFlash,
		checkRequirements, needDomain,
	)
}

// GetStorage gets the storage backend from the config.
func GetStorage(
	ctx context.Context,
	cfg *Config,
) (*backuppb.StorageBackend, storage.ExternalStorage, error) {
	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	s, err := storage.New(ctx, u, storageOpts(cfg))
	if err != nil {
		return nil, nil, errors.Annotate(err, "create storage failed")
	}
	return u, s, nil
}

func storageOpts(cfg *Config) *storage.ExternalStorageOptions {
	return &storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
		SkipCheckPath:   cfg.SkipCheckPath,
	}
}

// ReadBackupMeta reads the backupmeta file from the storage.
func ReadBackupMeta(
	ctx context.Context,
	fileName string,
	cfg *Config,
) (*backuppb.StorageBackend, storage.ExternalStorage, *backuppb.BackupMeta, error) {
	u, s, err := GetStorage(ctx, cfg)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	metaData, err := s.ReadFile(ctx, fileName)
	if err != nil {
		if gcsObjectNotFound(err) {
			// change gcs://bucket/abc/def to gcs://bucket/abc and read defbackupmeta
			oldPrefix := u.GetGcs().GetPrefix()
			newPrefix, file := path.Split(oldPrefix)
			newFileName := file + fileName
			u.GetGcs().Prefix = newPrefix
			s, err = storage.New(ctx, u, storageOpts(cfg))
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			log.Info("retry load metadata in gcs", zap.String("newPrefix", newPrefix), zap.String("newFileName", newFileName))
			metaData, err = s.ReadFile(ctx, newFileName)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			// reset prefix for tikv download sst file correctly.
			u.GetGcs().Prefix = oldPrefix
		} else {
			return nil, nil, nil, errors.Annotate(err, "load backupmeta failed")
		}
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = proto.Unmarshal(metaData, backupMeta); err != nil {
		return nil, nil, nil, errors.Annotate(err, "parse backupmeta failed")
	}
	return u, s, backupMeta, nil
}

// flagToZapField checks whether this flag can be logged,
// if need to log, return its zap field. Or return a field with hidden value.
func flagToZapField(f *pflag.Flag) zap.Field {
	if f.Name == flagStorage {
		hiddenQuery, err := url.Parse(f.Value.String())
		if err != nil {
			return zap.String(f.Name, "<invalid URI>")
		}
		// hide all query here.
		hiddenQuery.RawQuery = ""
		return zap.Stringer(f.Name, hiddenQuery)
	}
	return zap.Stringer(f.Name, f.Value)
}

// LogArguments prints origin command arguments.
func LogArguments(cmd *cobra.Command) {
	flags := cmd.Flags()
	fields := make([]zap.Field, 1, flags.NFlag()+1)
	fields[0] = zap.String("__command", cmd.CommandPath())
	flags.Visit(func(f *pflag.Flag) {
		fields = append(fields, flagToZapField(f))
	})
	log.Info("arguments", fields...)
}

// GetKeepalive get the keepalive info from the config.
func GetKeepalive(cfg *Config) keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:    cfg.GRPCKeepaliveTime,
		Timeout: cfg.GRPCKeepaliveTimeout,
	}
}

// adjust adjusts the abnormal config value in the current config.
// useful when not starting BR from CLI (e.g. from BRIE in SQL).
func (cfg *Config) adjust() {
	if cfg.GRPCKeepaliveTime == 0 {
		cfg.GRPCKeepaliveTime = defaultGRPCKeepaliveTime
	}
	if cfg.GRPCKeepaliveTimeout == 0 {
		cfg.GRPCKeepaliveTimeout = defaultGRPCKeepaliveTimeout
	}
	if cfg.ChecksumConcurrency == 0 {
		cfg.ChecksumConcurrency = variable.DefChecksumTableConcurrency
	}
}

func normalizePDURL(pd string, useTLS bool) (string, error) {
	if strings.HasPrefix(pd, "http://") {
		if useTLS {
			return "", errors.Annotate(berrors.ErrInvalidArgument, "pd url starts with http while TLS enabled")
		}
		return strings.TrimPrefix(pd, "http://"), nil
	}
	if strings.HasPrefix(pd, "https://") {
		if !useTLS {
			return "", errors.Annotate(berrors.ErrInvalidArgument, "pd url starts with https while TLS disabled")
		}
		return strings.TrimPrefix(pd, "https://"), nil
	}
	return pd, nil
}

// check whether it's a bug before #647, to solve case #1
// If the storage is set as gcs://bucket/prefix,
// the SSTs are written correctly to gcs://bucket/prefix/*.sst
// but the backupmeta is written wrongly to gcs://bucket/prefixbackupmeta.
// see details https://github.com/pingcap/br/issues/675#issuecomment-753780742
func gcsObjectNotFound(err error) bool {
	return errors.Cause(err) == gcs.ErrObjectNotExist // nolint:errorlint
}
