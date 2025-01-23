// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/docker/go-units"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/config"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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
	FlagChecksum            = "checksum"
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
	flagDryRun            = "dry-run"
	// TODO used for local test, should be removed later
	flagSkipAWS                       = "skip-aws"
	flagUseFSR                        = "use-fsr"
	flagCloudAPIConcurrency           = "cloud-api-concurrency"
	flagWithSysTable                  = "with-sys-table"
	flagOperatorPausedGCAndSchedulers = "operator-paused-gc-and-scheduler"

	defaultSwitchInterval       = 5 * time.Minute
	defaultGRPCKeepaliveTime    = 10 * time.Second
	defaultGRPCKeepaliveTimeout = 3 * time.Second
	defaultCloudAPIConcurrency  = 8

	flagFullBackupCipherType    = "crypter.method"
	flagFullBackupCipherKey     = "crypter.key"
	flagFullBackupCipherKeyFile = "crypter.key-file"

	flagLogBackupCipherType    = "log.crypter.method"
	flagLogBackupCipherKey     = "log.crypter.key"
	flagLogBackupCipherKeyFile = "log.crypter.key-file"

	flagMetadataDownloadBatchSize    = "metadata-download-batch-size"
	defaultMetadataDownloadBatchSize = 128

	unlimited           = 0
	crypterAES128KeyLen = 16
	crypterAES192KeyLen = 24
	crypterAES256KeyLen = 32

	flagFullBackupType = "type"

	masterKeysDelimiter     = ","
	flagMasterKeyConfig     = "master-key"
	flagMasterKeyCipherType = "master-key-crypter-method"
)

const (
	cipherKeyNonHexErrorMsg = "cipher key must be a valid hexadecimal string"
)

// FullBackupType type when doing full backup or restore
type FullBackupType string

const (
	FullBackupTypeKV  FullBackupType = "kv" // default type
	FullBackupTypeEBS FullBackupType = "aws-ebs"
)

// Valid whether the type is valid
func (t FullBackupType) Valid() bool {
	return t == FullBackupTypeKV || t == FullBackupTypeEBS
}

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

// Convert the TLS config to the PD security option.
func (tls *TLSConfig) ToPDSecurityOption() pd.SecurityOption {
	securityOption := pd.SecurityOption{}
	securityOption.CAPath = tls.CA
	securityOption.CertPath = tls.Cert
	securityOption.KeyPath = tls.Key
	return securityOption
}

// Convert the TLS config to the PD security option.
func (tls *TLSConfig) ToKVSecurity() config.Security {
	return config.Security{
		ClusterSSLCA:   tls.CA,
		ClusterSSLCert: tls.Cert,
		ClusterSSLKey:  tls.Key,
	}
}

// ParseFromFlags parses the TLS config from the flag set.
func (tls *TLSConfig) ParseFromFlags(flags *pflag.FlagSet) (err error) {
	tls.CA, tls.Cert, tls.Key, err = ParseTLSTripleFromFlags(flags)
	return
}

func dialEtcdWithCfg(ctx context.Context, cfg Config) (*clientv3.Client, error) {
	var (
		tlsConfig *tls.Config
		err       error
	)

	if cfg.TLS.IsEnabled() {
		tlsConfig, err = cfg.TLS.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	log.Info("trying to connect to etcd", zap.Strings("addr", cfg.PD))
	etcdCLI, err := clientv3.New(clientv3.Config{
		TLS:              tlsConfig,
		Endpoints:        cfg.PD,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                cfg.GRPCKeepaliveTime,
				Timeout:             cfg.GRPCKeepaliveTimeout,
				PermitWithoutStream: false,
			}),
			grpc.WithBlock(),
			grpc.WithReturnConnectionError(),
		},
		Context: ctx,
	})
	if err != nil {
		return nil, err
	}
	return etcdCLI, nil
}

// Config is the common configuration for all BRIE tasks.
type Config struct {
	storage.BackendOptions

	Storage             string    `json:"storage" toml:"storage"`
	PD                  []string  `json:"pd" toml:"pd"`
	TLS                 TLSConfig `json:"tls" toml:"tls"`
	RateLimit           uint64    `json:"rate-limit" toml:"rate-limit"`
	ChecksumConcurrency uint      `json:"checksum-concurrency" toml:"checksum-concurrency"`
	TableConcurrency    uint      `json:"table-concurrency" toml:"table-concurrency"`
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

	FilterStr          []string      `json:"filter-strings" toml:"filter-strings"`
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

	// Plaintext data key mainly used for full/snapshot backup and restore.
	CipherInfo backuppb.CipherInfo `json:"-" toml:"-"`

	// Could be used in log backup and restore but not recommended in a serious environment since data key is stored
	// in PD in plaintext.
	LogBackupCipherInfo backuppb.CipherInfo `json:"-" toml:"-"`

	// Master key based encryption for log restore.
	// More than one can be specified for log restore if master key rotated during log backup.
	MasterKeyConfig backuppb.MasterKeyConfig `json:"master-key-config" toml:"master-key-config"`

	// whether there's explicit filter
	ExplicitFilter bool `json:"-" toml:"-"`

	// KeyspaceName is the name of the keyspace of the task
	KeyspaceName string `json:"keyspace-name" toml:"keyspace-name"`

	// Metadata download batch size, such as metadata for log restore
	MetadataDownloadBatchSize uint `json:"metadata-download-batch-size" toml:"metadata-download-batch-size"`
}

// DefineCommonFlags defines the flags common to all BRIE commands.
func DefineCommonFlags(flags *pflag.FlagSet) {
	flags.BoolP(flagSendCreds, "c", true, "Whether send credentials to tikv")
	flags.StringP(flagStorage, "s", "", `specify the url where backup storage, eg, "s3://bucket/path/prefix"`)
	flags.StringSliceP(flagPD, "u", []string{"127.0.0.1:2379"}, "PD address")
	flags.String(flagCA, "", "CA certificate path for TLS connection")
	flags.String(flagCert, "", "Certificate path for TLS connection")
	flags.String(flagKey, "", "Private key path for TLS connection")
	flags.Uint(flagChecksumConcurrency, variable.DefChecksumTableConcurrency, "The concurrency of checksumming in one table")

	flags.Uint64(flagRateLimit, unlimited, "The rate limit of the task, MB/s per node")
	flags.Bool(FlagChecksum, true, "Run checksum at end of task")
	flags.Bool(flagRemoveTiFlash, true,
		"Remove TiFlash replicas before backup or restore, for unsupported versions of TiFlash")

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

	flags.String(flagFullBackupCipherType, "plaintext", "Encrypt/decrypt method, "+
		"be one of plaintext|aes128-ctr|aes192-ctr|aes256-ctr case-insensitively, "+
		"\"plaintext\" represents no encrypt/decrypt")
	flags.String(flagFullBackupCipherKey, "",
		"aes-crypter key, used to encrypt/decrypt the data "+
			"by the hexadecimal string, eg: \"0123456789abcdef0123456789abcdef\"")
	flags.String(flagFullBackupCipherKeyFile, "", "FilePath, its content is used as the cipher-key")

	flags.Uint(flagMetadataDownloadBatchSize, defaultMetadataDownloadBatchSize,
		"the batch size of downloading metadata, such as log restore metadata for truncate or restore")

	// log backup plaintext key flags
	flags.String(flagLogBackupCipherType, "plaintext", "Encrypt/decrypt method, "+
		"be one of plaintext|aes128-ctr|aes192-ctr|aes256-ctr case-insensitively, "+
		"\"plaintext\" represents no encrypt/decrypt")
	flags.String(flagLogBackupCipherKey, "",
		"aes-crypter key, used to encrypt/decrypt the data "+
			"by the hexadecimal string, eg: \"0123456789abcdef0123456789abcdef\"")
	flags.String(flagLogBackupCipherKeyFile, "", "FilePath, its content is used as the cipher-key")

	// master key config
	flags.String(flagMasterKeyCipherType, "plaintext", "Encrypt/decrypt method, "+
		"be one of plaintext|aes128-ctr|aes192-ctr|aes256-ctr case-insensitively, "+
		"\"plaintext\" represents no encrypt/decrypt")
	flags.String(flagMasterKeyConfig, "", "Master key config for point in time restore "+
		"examples: \"local:///path/to/master/key/file,"+
		"aws-kms:///{key-id}?AWS_ACCESS_KEY_ID={access-key}&AWS_SECRET_ACCESS_KEY={secret-key}&REGION={region},"+
		"gcp-kms:///projects/{project-id}/locations/{location}/keyRings/{keyring}/cryptoKeys/{key-name}?AUTH=specified&CREDENTIALS={credentials}\"")
	_ = flags.MarkHidden(flagMetadataDownloadBatchSize)

	storage.DefineFlags(flags)
}

// HiddenFlagsForStream temporary hidden flags that stream cmd not support.
func HiddenFlagsForStream(flags *pflag.FlagSet) {
	_ = flags.MarkHidden(FlagChecksum)
	_ = flags.MarkHidden(flagLoadStats)
	_ = flags.MarkHidden(flagChecksumConcurrency)
	_ = flags.MarkHidden(flagRateLimit)
	_ = flags.MarkHidden(flagRateLimitUnit)
	_ = flags.MarkHidden(flagRemoveTiFlash)
	_ = flags.MarkHidden(flagFullBackupCipherType)
	_ = flags.MarkHidden(flagFullBackupCipherKey)
	_ = flags.MarkHidden(flagFullBackupCipherKeyFile)
	_ = flags.MarkHidden(flagLogBackupCipherType)
	_ = flags.MarkHidden(flagLogBackupCipherKey)
	_ = flags.MarkHidden(flagLogBackupCipherKeyFile)
	_ = flags.MarkHidden(flagSwitchModeInterval)
	_ = flags.MarkHidden(flagMasterKeyConfig)
	_ = flags.MarkHidden(flagMasterKeyCipherType)

	storage.HiddenFlagsForStream(flags)
}

func DefaultConfig() Config {
	fs := pflag.NewFlagSet("dummy", pflag.ContinueOnError)
	DefineCommonFlags(fs)
	cfg := Config{}
	if err := cfg.ParseFromFlags(fs); err != nil {
		log.Panic("infallible operation failed.", zap.Error(err))
	}
	return cfg
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
func DefineFilterFlags(command *cobra.Command, defaultFilter []string, setHidden bool) {
	flags := command.Flags()
	flags.StringArrayP(flagFilter, "f", defaultFilter, "select tables to process")
	flags.Bool(flagCaseSensitive, false, "whether the table names used in --filter should be case-sensitive")

	if setHidden {
		_ = flags.MarkHidden(flagFilter)
		_ = flags.MarkHidden(flagCaseSensitive)
	}
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

func parseCipherType(t string) (encryptionpb.EncryptionMethod, error) {
	ct := encryptionpb.EncryptionMethod_UNKNOWN
	switch t {
	case "plaintext", "PLAINTEXT":
		ct = encryptionpb.EncryptionMethod_PLAINTEXT
	case "aes128-ctr", "AES128-CTR":
		ct = encryptionpb.EncryptionMethod_AES128_CTR
	case "aes192-ctr", "AES192-CTR":
		ct = encryptionpb.EncryptionMethod_AES192_CTR
	case "aes256-ctr", "AES256-CTR":
		ct = encryptionpb.EncryptionMethod_AES256_CTR
	default:
		return ct, errors.Annotatef(berrors.ErrInvalidArgument, "invalid crypter method '%s'", t)
	}

	return ct, nil
}

func checkCipherKey(cipherKey, cipherKeyFile string) error {
	if (len(cipherKey) == 0) == (len(cipherKeyFile) == 0) {
		return errors.Annotate(berrors.ErrInvalidArgument,
			"exactly one of cipher key or keyfile path should be provided")
	}
	return nil
}

func GetCipherKeyContent(cipherKey, cipherKeyFile string) ([]byte, error) {
	if err := checkCipherKey(cipherKey, cipherKeyFile); err != nil {
		return nil, errors.Trace(err)
	}

	var hexString string

	// Check if cipher-key is provided directly
	if len(cipherKey) > 0 {
		hexString = cipherKey
	} else {
		// Read content from cipher-file
		content, err := os.ReadFile(cipherKeyFile)
		if err != nil {
			return nil, errors.Annotate(err, "failed to read cipher file")
		}
		hexString = string(bytes.TrimSuffix(content, []byte("\n")))
	}

	// Attempt to decode the hex string
	decodedKey, err := hex.DecodeString(hexString)
	if err != nil {
		return nil, errors.Annotate(berrors.ErrInvalidArgument, cipherKeyNonHexErrorMsg)
	}

	return decodedKey, nil
}

func checkCipherKeyMatch(cipher *backuppb.CipherInfo) error {
	switch cipher.CipherType {
	case encryptionpb.EncryptionMethod_PLAINTEXT:
		return nil
	case encryptionpb.EncryptionMethod_AES128_CTR:
		if len(cipher.CipherKey) != crypterAES128KeyLen {
			return errors.Annotatef(berrors.ErrInvalidArgument, "AES-128 key length mismatch: expected %d, got %d",
				crypterAES128KeyLen, len(cipher.CipherKey))
		}
	case encryptionpb.EncryptionMethod_AES192_CTR:
		if len(cipher.CipherKey) != crypterAES192KeyLen {
			return errors.Annotatef(berrors.ErrInvalidArgument, "AES-192 key length mismatch: expected %d, got %d",
				crypterAES192KeyLen, len(cipher.CipherKey))
		}
	case encryptionpb.EncryptionMethod_AES256_CTR:
		if len(cipher.CipherKey) != crypterAES256KeyLen {
			return errors.Annotatef(berrors.ErrInvalidArgument, "AES-256 key length mismatch: expected %d, got %d",
				crypterAES256KeyLen, len(cipher.CipherKey))
		}
	default:
		return errors.Errorf("Unknown encryption method: %v", cipher.CipherType)
	}
	return nil
}

func (cfg *Config) parseCipherInfo(flags *pflag.FlagSet) error {
	crypterStr, err := flags.GetString(flagFullBackupCipherType)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.CipherInfo.CipherType, err = parseCipherType(crypterStr)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.CipherInfo.CipherType == encryptionpb.EncryptionMethod_PLAINTEXT {
		return nil
	}

	key, err := flags.GetString(flagFullBackupCipherKey)
	if err != nil {
		return errors.Trace(err)
	}

	keyFilePath, err := flags.GetString(flagFullBackupCipherKeyFile)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.CipherInfo.CipherKey, err = GetCipherKeyContent(key, keyFilePath)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkCipherKeyMatch(&cfg.CipherInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (cfg *Config) parseLogBackupCipherInfo(flags *pflag.FlagSet) (bool, error) {
	crypterStr, err := flags.GetString(flagLogBackupCipherType)
	if err != nil {
		return false, errors.Trace(err)
	}

	cfg.LogBackupCipherInfo.CipherType, err = parseCipherType(crypterStr)
	if err != nil {
		return false, errors.Trace(err)
	}

	if !utils.IsEffectiveEncryptionMethod(cfg.LogBackupCipherInfo.CipherType) {
		return false, nil
	}

	key, err := flags.GetString(flagLogBackupCipherKey)
	if err != nil {
		return false, errors.Trace(err)
	}

	keyFilePath, err := flags.GetString(flagLogBackupCipherKeyFile)
	if err != nil {
		return false, errors.Trace(err)
	}

	cfg.LogBackupCipherInfo.CipherKey, err = GetCipherKeyContent(key, keyFilePath)
	if err != nil {
		return false, errors.Trace(err)
	}

	err = checkCipherKeyMatch(&cfg.CipherInfo)
	if err != nil {
		return false, errors.Trace(err)
	}

	return true, nil
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

func (cfg *Config) UserFiltered() bool {
	return len(cfg.Schemas) != 0 || len(cfg.Tables) != 0 || len(cfg.FilterStr) != 0
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

	if cfg.Checksum, err = flags.GetBool(FlagChecksum); err != nil {
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
		cfg.ExplicitFilter = flags.Changed(flagFilter)
		cfg.FilterStr = filterFlag.Value.(pflag.SliceValue).GetSlice()
		cfg.TableFilter, err = filter.Parse(cfg.FilterStr)
		if err != nil {
			return errors.Trace(err)
		}
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
	if cfg.SkipCheckPath {
		log.L().Info("--skip-check-path is deprecated, need explicitly set it anymore")
	}

	err = cfg.parseCipherInfo(flags)
	if err != nil {
		return errors.Trace(err)
	}

	hasLogBackupPlaintextKey, err := cfg.parseLogBackupCipherInfo(flags)
	if err != nil {
		return errors.Trace(err)
	}

	if err = cfg.parseAndValidateMasterKeyInfo(hasLogBackupPlaintextKey, flags); err != nil {
		return errors.Trace(err)
	}

	if cfg.MetadataDownloadBatchSize, err = flags.GetUint(flagMetadataDownloadBatchSize); err != nil {
		return errors.Trace(err)
	}

	return cfg.normalizePDURLs()
}

func (cfg *Config) parseAndValidateMasterKeyInfo(hasPlaintextKey bool, flags *pflag.FlagSet) error {
	masterKeyString, err := flags.GetString(flagMasterKeyConfig)
	if err != nil {
		return errors.Errorf("master key flag '%s' is not defined: %v", flagMasterKeyConfig, err)
	}

	if masterKeyString == "" {
		return nil
	}

	if hasPlaintextKey {
		return errors.Errorf("invalid argument: both plaintext data key encryption and master key based encryption are set at the same time")
	}

	encryptionMethodString, err := flags.GetString(flagMasterKeyCipherType)
	if err != nil {
		return errors.Errorf("encryption method flag '%s' is not defined: %v", flagMasterKeyCipherType, err)
	}

	encryptionMethod, err := parseCipherType(encryptionMethodString)
	if err != nil {
		return errors.Errorf("failed to parse encryption method: %v", err)
	}

	if !utils.IsEffectiveEncryptionMethod(encryptionMethod) {
		return errors.Errorf("invalid encryption method: %s", encryptionMethodString)
	}

	masterKeyStrings := strings.Split(masterKeyString, masterKeysDelimiter)
	cfg.MasterKeyConfig = backuppb.MasterKeyConfig{
		EncryptionType: encryptionMethod,
		MasterKeys:     make([]*encryptionpb.MasterKey, 0, len(masterKeyStrings)),
	}

	for _, keyString := range masterKeyStrings {
		masterKey, err := validateAndParseMasterKeyString(strings.TrimSpace(keyString))
		if err != nil {
			return errors.Wrapf(err, "invalid master key configuration: %s", keyString)
		}
		cfg.MasterKeyConfig.MasterKeys = append(cfg.MasterKeyConfig.MasterKeys, &masterKey)
	}

	return nil
}

// OverrideDefaultForBackup override common config for backup tasks
func (cfg *Config) OverrideDefaultForBackup() {
	cfg.Checksum = false
}

// NewMgr creates a new mgr at the given PD address.
func NewMgr(ctx context.Context,
	g glue.Glue, pds []string,
	tlsConfig TLSConfig,
	keepalive keepalive.ClientParameters,
	checkRequirements bool,
	needDomain bool,
	versionCheckerType conn.VersionCheckerType,
) (*conn.Mgr, error) {
	var (
		tlsConf *tls.Config
		err     error
	)
	if len(pds) == 0 {
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

	// Is it necessary to remove `StoreBehavior`?
	return conn.NewMgr(
		ctx, g, pds, tlsConf, securityOption, keepalive, util.SkipTiFlash,
		checkRequirements, needDomain, versionCheckerType,
	)
}

// GetStorage gets the storage backend from the config.
func GetStorage(
	ctx context.Context,
	storageName string,
	cfg *Config,
) (*backuppb.StorageBackend, storage.ExternalStorage, error) {
	u, err := storage.ParseBackend(storageName, &cfg.BackendOptions)
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
	}
}

// ReadBackupMeta reads the backupmeta file from the storage.
func ReadBackupMeta(
	ctx context.Context,
	fileName string,
	cfg *Config,
) (*backuppb.StorageBackend, storage.ExternalStorage, *backuppb.BackupMeta, error) {
	u, s, err := GetStorage(ctx, cfg.Storage, cfg)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	metaData, err := s.ReadFile(ctx, fileName)
	if err != nil {
		if !gcsObjectNotFound(err) {
			return nil, nil, nil, errors.Annotate(err, "load backupmeta failed")
		}
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
	}

	// the prefix of backupmeta file is iv(16 bytes) if encryption method is valid
	var iv []byte
	if cfg.CipherInfo.CipherType != encryptionpb.EncryptionMethod_PLAINTEXT {
		iv = metaData[:metautil.CrypterIvLen]
	}
	decryptBackupMeta, err := utils.Decrypt(metaData[len(iv):], &cfg.CipherInfo, iv)
	if err != nil {
		return nil, nil, nil, errors.Annotate(err, "decrypt failed with wrong key")
	}

	backupMeta := &backuppb.BackupMeta{}
	if err = proto.Unmarshal(decryptBackupMeta, backupMeta); err != nil {
		return nil, nil, nil, errors.Annotate(err,
			"parse backupmeta failed because of wrong aes cipher")
	}
	return u, s, backupMeta, nil
}

// flagToZapField checks whether this flag can be logged,
// if need to log, return its zap field. Or return a field with hidden value.
func flagToZapField(f *pflag.Flag) zap.Field {
	switch f.Name {
	case flagStorage, FlagStreamFullBackupStorage:
		hiddenQuery, err := url.Parse(f.Value.String())
		if err != nil {
			return zap.String(f.Name, "<invalid URI>")
		}
		// hide all query here.
		hiddenQuery.RawQuery = ""
		return zap.Stringer(f.Name, hiddenQuery)
	case flagFullBackupCipherKey, flagLogBackupCipherKey, "azblob.encryption-key":
		return zap.String(f.Name, "<redacted>")
	case flagMasterKeyConfig:
		// TODO: we don't really need to hide the entirety of --master-key, consider parsing the URL here.
		return zap.String(f.Name, "<redacted>")
	default:
		return zap.Stringer(f.Name, f.Value)
	}
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
	if cfg.MetadataDownloadBatchSize == 0 {
		cfg.MetadataDownloadBatchSize = defaultMetadataDownloadBatchSize
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

// write progress in tmp file for tidb-operator, so tidb-operator can retrieve the
// progress of ebs backup. and user can get the progress through `kubectl get job`
// todo: maybe change to http api later
func progressFileWriterRoutine(ctx context.Context, progress glue.Progress, total int64, progressFile string) {
	// remove tmp file
	defer func() {
		_ = os.Remove(progressFile)
	}()

	for progress.GetCurrent() < total {
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
			break
		}
		cur := progress.GetCurrent()
		p := float64(cur) / float64(total)
		p *= 100
		err := os.WriteFile(progressFile, []byte(fmt.Sprintf("%.2f", p)), 0600)
		if err != nil {
			log.Warn("failed to update tmp progress file", zap.Error(err))
		}
	}
}
