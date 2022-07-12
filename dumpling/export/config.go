// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/util"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	flagDatabase                 = "database"
	flagTablesList               = "tables-list"
	flagHost                     = "host"
	flagUser                     = "user"
	flagPort                     = "port"
	flagPassword                 = "password"
	flagAllowCleartextPasswords  = "allow-cleartext-passwords"
	flagThreads                  = "threads"
	flagFilesize                 = "filesize"
	flagStatementSize            = "statement-size"
	flagOutput                   = "output"
	flagLoglevel                 = "loglevel"
	flagLogfile                  = "logfile"
	flagLogfmt                   = "logfmt"
	flagConsistency              = "consistency"
	flagSnapshot                 = "snapshot"
	flagNoViews                  = "no-views"
	flagNoSequences              = "no-sequences"
	flagSortByPk                 = "order-by-primary-key"
	flagStatusAddr               = "status-addr"
	flagRows                     = "rows"
	flagWhere                    = "where"
	flagEscapeBackslash          = "escape-backslash"
	flagFiletype                 = "filetype"
	flagNoHeader                 = "no-header"
	flagNoSchemas                = "no-schemas"
	flagNoData                   = "no-data"
	flagCsvNullValue             = "csv-null-value"
	flagSQL                      = "sql"
	flagFilter                   = "filter"
	flagCaseSensitive            = "case-sensitive"
	flagDumpEmptyDatabase        = "dump-empty-database"
	flagTidbMemQuotaQuery        = "tidb-mem-quota-query"
	flagCA                       = "ca"
	flagCert                     = "cert"
	flagKey                      = "key"
	flagCsvSeparator             = "csv-separator"
	flagCsvDelimiter             = "csv-delimiter"
	flagOutputFilenameTemplate   = "output-filename-template"
	flagCompleteInsert           = "complete-insert"
	flagParams                   = "params"
	flagReadTimeout              = "read-timeout"
	flagTransactionalConsistency = "transactional-consistency"
	flagCompress                 = "compress"

	// FlagHelp represents the help flag
	FlagHelp = "help"
)

// Config is the dump config for dumpling
type Config struct {
	storage.BackendOptions
	ExtStorage storage.ExternalStorage `json:"-"`

	specifiedTables          bool
	AllowCleartextPasswords  bool
	SortByPk                 bool
	NoViews                  bool
	NoSequences              bool
	NoHeader                 bool
	NoSchemas                bool
	NoData                   bool
	CompleteInsert           bool
	TransactionalConsistency bool
	EscapeBackslash          bool
	DumpEmptyDatabase        bool
	PosAfterConnect          bool
	CompressType             storage.CompressType

	Host     string
	Port     int
	Threads  int
	User     string
	Password string `json:"-"`
	Security struct {
		CAPath       string
		CertPath     string
		KeyPath      string
		SSLCABytes   []byte `json:"-"`
		SSLCertBytes []byte `json:"-"`
		SSLKEYBytes  []byte `json:"-"`
	}

	LogLevel      string
	LogFile       string
	LogFormat     string
	OutputDirPath string
	StatusAddr    string
	Snapshot      string
	Consistency   string
	CsvNullValue  string
	SQL           string
	CsvSeparator  string
	CsvDelimiter  string
	Databases     []string

	TableFilter         filter.Filter `json:"-"`
	Where               string
	FileType            string
	ServerInfo          version.ServerInfo
	Logger              *zap.Logger        `json:"-"`
	OutputFileTemplate  *template.Template `json:"-"`
	Rows                uint64
	ReadTimeout         time.Duration
	TiDBMemQuotaQuery   uint64
	FileSize            uint64
	StatementSize       uint64
	SessionParams       map[string]interface{}
	Labels              prometheus.Labels `json:"-"`
	Tables              DatabaseTables
	CollationCompatible string
}

// ServerInfoUnknown is the unknown database type to dumpling
var ServerInfoUnknown = version.ServerInfo{
	ServerType:    version.ServerTypeUnknown,
	ServerVersion: nil,
}

// DefaultConfig returns the default export Config for dumpling
func DefaultConfig() *Config {
	allFilter, _ := filter.Parse([]string{"*.*"})
	return &Config{
		Databases:           nil,
		Host:                "127.0.0.1",
		User:                "root",
		Port:                3306,
		Password:            "",
		Threads:             4,
		Logger:              nil,
		StatusAddr:          ":8281",
		FileSize:            UnspecifiedSize,
		StatementSize:       DefaultStatementSize,
		OutputDirPath:       ".",
		ServerInfo:          ServerInfoUnknown,
		SortByPk:            true,
		Tables:              nil,
		Snapshot:            "",
		Consistency:         consistencyTypeAuto,
		NoViews:             true,
		NoSequences:         true,
		Rows:                UnspecifiedSize,
		Where:               "",
		FileType:            "",
		NoHeader:            false,
		NoSchemas:           false,
		NoData:              false,
		CsvNullValue:        "\\N",
		SQL:                 "",
		TableFilter:         allFilter,
		DumpEmptyDatabase:   true,
		SessionParams:       make(map[string]interface{}),
		OutputFileTemplate:  DefaultOutputFileTemplate,
		PosAfterConnect:     false,
		CollationCompatible: LooseCollationCompatible,
		specifiedTables:     false,
	}
}

// String returns dumpling's config in json format
func (conf *Config) String() string {
	cfg, err := json.Marshal(conf)
	if err != nil && conf.Logger != nil {
		conf.Logger.Error("fail to marshal config to json", zap.Error(err))
	}
	return string(cfg)
}

// GetDSN generates DSN from Config
func (conf *Config) GetDSN(db string) string {
	// maxAllowedPacket=0 can be used to automatically fetch the max_allowed_packet variable from server on every connection.
	// https://github.com/go-sql-driver/mysql#maxallowedpacket
	hostPort := net.JoinHostPort(conf.Host, strconv.Itoa(conf.Port))
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?collation=utf8mb4_general_ci&readTimeout=%s&writeTimeout=30s&interpolateParams=true&maxAllowedPacket=0",
		conf.User, conf.Password, hostPort, db, conf.ReadTimeout)
	if len(conf.Security.CAPath) > 0 {
		dsn += "&tls=dumpling-tls-target"
	}
	if conf.AllowCleartextPasswords {
		dsn += "&allowCleartextPasswords=1"
	}
	return dsn
}

func timestampDirName() string {
	return fmt.Sprintf("./export-%s", time.Now().Format(time.RFC3339))
}

// DefineFlags defines flags of dumpling's configuration
func (conf *Config) DefineFlags(flags *pflag.FlagSet) {
	storage.DefineFlags(flags)
	flags.StringSliceP(flagDatabase, "B", nil, "Databases to dump")
	flags.StringSliceP(flagTablesList, "T", nil, "Comma delimited table list to dump; must be qualified table names")
	flags.StringP(flagHost, "h", "127.0.0.1", "The host to connect to")
	flags.StringP(flagUser, "u", "root", "Username with privileges to run the dump")
	flags.IntP(flagPort, "P", 4000, "TCP/IP port to connect to")
	flags.StringP(flagPassword, "p", "", "User password")
	flags.Bool(flagAllowCleartextPasswords, false, "Allow passwords to be sent in cleartext (warning: don't use without TLS)")
	flags.IntP(flagThreads, "t", 4, "Number of goroutines to use, default 4")
	flags.StringP(flagFilesize, "F", "", "The approximate size of output file")
	flags.Uint64P(flagStatementSize, "s", DefaultStatementSize, "Attempted size of INSERT statement in bytes")
	flags.StringP(flagOutput, "o", timestampDirName(), "Output directory")
	flags.String(flagLoglevel, "info", "Log level: {debug|info|warn|error|dpanic|panic|fatal}")
	flags.StringP(flagLogfile, "L", "", "Log file `path`, leave empty to write to console")
	flags.String(flagLogfmt, "text", "Log `format`: {text|json}")
	flags.String(flagConsistency, consistencyTypeAuto, "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	flags.String(flagSnapshot, "", "Snapshot position (uint64 or MySQL style string timestamp). Valid only when consistency=snapshot")
	flags.BoolP(flagNoViews, "W", true, "Do not dump views")
	flags.Bool(flagNoSequences, true, "Do not dump sequences")
	flags.Bool(flagSortByPk, true, "Sort dump results by primary key through order by sql")
	flags.String(flagStatusAddr, ":8281", "dumpling API server and pprof addr")
	flags.Uint64P(flagRows, "r", UnspecifiedSize, "If specified, dumpling will split table into chunks and concurrently dump them to different files to improve efficiency. For TiDB v3.0+, specify this will make dumpling split table with each file one TiDB region(no matter how many rows is).\n"+
		"If not specified, dumpling will dump table without inner-concurrency which could be relatively slow. default unlimited")
	flags.String(flagWhere, "", "Dump only selected records")
	flags.Bool(flagEscapeBackslash, true, "use backslash to escape special characters")
	flags.String(flagFiletype, "", "The type of export file (sql/csv)")
	flags.Bool(flagNoHeader, false, "whether not to dump CSV table header")
	flags.BoolP(flagNoSchemas, "m", false, "Do not dump table schemas with the data")
	flags.BoolP(flagNoData, "d", false, "Do not dump table data")
	flags.String(flagCsvNullValue, "\\N", "The null value used when export to csv")
	flags.StringP(flagSQL, "S", "", "Dump data with given sql. This argument doesn't support concurrent dump")
	_ = flags.MarkHidden(flagSQL)
	flags.StringSliceP(flagFilter, "f", []string{"*.*", DefaultTableFilter}, "filter to select which tables to dump")
	flags.Bool(flagCaseSensitive, false, "whether the filter should be case-sensitive")
	flags.Bool(flagDumpEmptyDatabase, true, "whether to dump empty database")
	flags.Uint64(flagTidbMemQuotaQuery, UnspecifiedSize, "The maximum memory limit for a single SQL statement, in bytes.")
	flags.String(flagCA, "", "The path name to the certificate authority file for TLS connection")
	flags.String(flagCert, "", "The path name to the client certificate file for TLS connection")
	flags.String(flagKey, "", "The path name to the client private key file for TLS connection")
	flags.String(flagCsvSeparator, ",", "The separator for csv files, default ','")
	flags.String(flagCsvDelimiter, "\"", "The delimiter for values in csv files, default '\"'")
	flags.String(flagOutputFilenameTemplate, "", "The output filename template (without file extension)")
	flags.Bool(flagCompleteInsert, false, "Use complete INSERT statements that include column names")
	flags.StringToString(flagParams, nil, `Extra session variables used while dumping, accepted format: --params "character_set_client=latin1,character_set_connection=latin1"`)
	flags.Bool(FlagHelp, false, "Print help message and quit")
	flags.Duration(flagReadTimeout, 15*time.Minute, "I/O read timeout for db connection.")
	_ = flags.MarkHidden(flagReadTimeout)
	flags.Bool(flagTransactionalConsistency, true, "Only support transactional consistency")
	_ = flags.MarkHidden(flagTransactionalConsistency)
	flags.StringP(flagCompress, "c", "", "Compress output file type, support 'gzip', 'no-compression' now")
}

// ParseFromFlags parses dumpling's export.Config from flags
// nolint: gocyclo
func (conf *Config) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	conf.Databases, err = flags.GetStringSlice(flagDatabase)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Host, err = flags.GetString(flagHost)
	if err != nil {
		return errors.Trace(err)
	}
	conf.User, err = flags.GetString(flagUser)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Port, err = flags.GetInt(flagPort)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Password, err = flags.GetString(flagPassword)
	if err != nil {
		return errors.Trace(err)
	}
	conf.AllowCleartextPasswords, err = flags.GetBool(flagAllowCleartextPasswords)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Threads, err = flags.GetInt(flagThreads)
	if err != nil {
		return errors.Trace(err)
	}
	conf.StatementSize, err = flags.GetUint64(flagStatementSize)
	if err != nil {
		return errors.Trace(err)
	}
	conf.OutputDirPath, err = flags.GetString(flagOutput)
	if err != nil {
		return errors.Trace(err)
	}
	conf.LogLevel, err = flags.GetString(flagLoglevel)
	if err != nil {
		return errors.Trace(err)
	}
	conf.LogFile, err = flags.GetString(flagLogfile)
	if err != nil {
		return errors.Trace(err)
	}
	conf.LogFormat, err = flags.GetString(flagLogfmt)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Consistency, err = flags.GetString(flagConsistency)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Snapshot, err = flags.GetString(flagSnapshot)
	if err != nil {
		return errors.Trace(err)
	}
	conf.NoViews, err = flags.GetBool(flagNoViews)
	if err != nil {
		return errors.Trace(err)
	}
	conf.NoSequences, err = flags.GetBool(flagNoSequences)
	if err != nil {
		return errors.Trace(err)
	}
	conf.SortByPk, err = flags.GetBool(flagSortByPk)
	if err != nil {
		return errors.Trace(err)
	}
	conf.StatusAddr, err = flags.GetString(flagStatusAddr)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Rows, err = flags.GetUint64(flagRows)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Where, err = flags.GetString(flagWhere)
	if err != nil {
		return errors.Trace(err)
	}
	conf.EscapeBackslash, err = flags.GetBool(flagEscapeBackslash)
	if err != nil {
		return errors.Trace(err)
	}
	conf.FileType, err = flags.GetString(flagFiletype)
	if err != nil {
		return errors.Trace(err)
	}
	conf.NoHeader, err = flags.GetBool(flagNoHeader)
	if err != nil {
		return errors.Trace(err)
	}
	conf.NoSchemas, err = flags.GetBool(flagNoSchemas)
	if err != nil {
		return errors.Trace(err)
	}
	conf.NoData, err = flags.GetBool(flagNoData)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CsvNullValue, err = flags.GetString(flagCsvNullValue)
	if err != nil {
		return errors.Trace(err)
	}
	conf.SQL, err = flags.GetString(flagSQL)
	if err != nil {
		return errors.Trace(err)
	}
	conf.DumpEmptyDatabase, err = flags.GetBool(flagDumpEmptyDatabase)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Security.CAPath, err = flags.GetString(flagCA)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Security.CertPath, err = flags.GetString(flagCert)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Security.KeyPath, err = flags.GetString(flagKey)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CsvSeparator, err = flags.GetString(flagCsvSeparator)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CsvDelimiter, err = flags.GetString(flagCsvDelimiter)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CompleteInsert, err = flags.GetBool(flagCompleteInsert)
	if err != nil {
		return errors.Trace(err)
	}
	conf.ReadTimeout, err = flags.GetDuration(flagReadTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	conf.TransactionalConsistency, err = flags.GetBool(flagTransactionalConsistency)
	if err != nil {
		return errors.Trace(err)
	}
	conf.TiDBMemQuotaQuery, err = flags.GetUint64(flagTidbMemQuotaQuery)
	if err != nil {
		return errors.Trace(err)
	}

	if conf.Threads <= 0 {
		return errors.Errorf("--threads is set to %d. It should be greater than 0", conf.Threads)
	}
	if len(conf.CsvSeparator) == 0 {
		return errors.New("--csv-separator is set to \"\". It must not be an empty string")
	}

	if conf.SessionParams == nil {
		conf.SessionParams = make(map[string]interface{})
	}

	tablesList, err := flags.GetStringSlice(flagTablesList)
	if err != nil {
		return errors.Trace(err)
	}
	fileSizeStr, err := flags.GetString(flagFilesize)
	if err != nil {
		return errors.Trace(err)
	}
	filters, err := flags.GetStringSlice(flagFilter)
	if err != nil {
		return errors.Trace(err)
	}
	caseSensitive, err := flags.GetBool(flagCaseSensitive)
	if err != nil {
		return errors.Trace(err)
	}
	outputFilenameFormat, err := flags.GetString(flagOutputFilenameTemplate)
	if err != nil {
		return errors.Trace(err)
	}
	params, err := flags.GetStringToString(flagParams)
	if err != nil {
		return errors.Trace(err)
	}

	conf.specifiedTables = len(tablesList) > 0
	conf.Tables, err = GetConfTables(tablesList)
	if err != nil {
		return errors.Trace(err)
	}

	conf.TableFilter, err = ParseTableFilter(tablesList, filters)
	if err != nil {
		return errors.Errorf("failed to parse filter: %s", err)
	}

	if !caseSensitive {
		conf.TableFilter = filter.CaseInsensitive(conf.TableFilter)
	}

	conf.FileSize, err = ParseFileSize(fileSizeStr)
	if err != nil {
		return errors.Trace(err)
	}

	if outputFilenameFormat == "" && conf.SQL != "" {
		outputFilenameFormat = DefaultAnonymousOutputFileTemplateText
	}
	tmpl, err := ParseOutputFileTemplate(outputFilenameFormat)
	if err != nil {
		return errors.Errorf("failed to parse output filename template (--output-filename-template '%s')\n", outputFilenameFormat)
	}
	conf.OutputFileTemplate = tmpl

	compressType, err := flags.GetString(flagCompress)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CompressType, err = ParseCompressType(compressType)
	if err != nil {
		return errors.Trace(err)
	}

	for k, v := range params {
		conf.SessionParams[k] = v
	}

	err = conf.BackendOptions.ParseFromFlags(pflag.CommandLine)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// ParseFileSize parses file size from tables-list and filter arguments
func ParseFileSize(fileSizeStr string) (uint64, error) {
	if len(fileSizeStr) == 0 {
		return UnspecifiedSize, nil
	} else if fileSizeMB, err := strconv.ParseUint(fileSizeStr, 10, 64); err == nil {
		fmt.Printf("Warning: -F without unit is not recommended, try using `-F '%dMiB'` in the future\n", fileSizeMB)
		return fileSizeMB * units.MiB, nil
	} else if size, err := units.RAMInBytes(fileSizeStr); err == nil {
		return uint64(size), nil
	}
	return 0, errors.Errorf("failed to parse filesize (-F '%s')", fileSizeStr)
}

// ParseTableFilter parses table filter from tables-list and filter arguments
func ParseTableFilter(tablesList, filters []string) (filter.Filter, error) {
	if len(tablesList) == 0 {
		return filter.Parse(filters)
	}

	// only parse -T when -f is default value. otherwise bail out.
	if !sameStringArray(filters, []string{"*.*", DefaultTableFilter}) {
		return nil, errors.New("cannot pass --tables-list and --filter together")
	}

	tableNames := make([]filter.Table, 0, len(tablesList))
	for _, table := range tablesList {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) < 2 {
			return nil, errors.Errorf("--tables-list only accepts qualified table names, but `%s` lacks a dot", table)
		}
		tableNames = append(tableNames, filter.Table{Schema: parts[0], Name: parts[1]})
	}

	return filter.NewTablesFilter(tableNames...), nil
}

func GetConfTables(tablesList []string) (DatabaseTables, error) {
	dbTables := DatabaseTables{}
	var (
		tablename    string
		avgRowLength uint64
	)
	avgRowLength = 0
	for _, tablename = range tablesList {
		parts := strings.SplitN(tablename, ".", 2)
		if len(parts) < 2 {
			return nil, errors.Errorf("--tables-list only accepts qualified table names, but `%s` lacks a dot", tablename)
		}
		dbName := parts[0]
		tbName := parts[1]
		dbTables[dbName] = append(dbTables[dbName], &TableInfo{tbName, avgRowLength, TableTypeBase})
	}
	return dbTables, nil
}

// ParseCompressType parses compressType string to storage.CompressType
func ParseCompressType(compressType string) (storage.CompressType, error) {
	switch compressType {
	case "", "no-compression":
		return storage.NoCompression, nil
	case "gzip", "gz":
		return storage.Gzip, nil
	default:
		return storage.NoCompression, errors.Errorf("unknown compress type %s", compressType)
	}
}

func (conf *Config) createExternalStorage(ctx context.Context) (storage.ExternalStorage, error) {
	if conf.ExtStorage != nil {
		return conf.ExtStorage, nil
	}
	b, err := storage.ParseBackend(conf.OutputDirPath, &conf.BackendOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// TODO: support setting httpClient with certification later
	return storage.New(ctx, b, &storage.ExternalStorageOptions{})
}

const (
	// UnspecifiedSize means the filesize/statement-size is unspecified
	UnspecifiedSize = 0
	// DefaultStatementSize is the default statement size
	DefaultStatementSize = 1000000
	// TiDBMemQuotaQueryName is the session variable TiDBMemQuotaQuery's name in TiDB
	TiDBMemQuotaQueryName = "tidb_mem_quota_query"
	// DefaultTableFilter is the default exclude table filter. It will exclude all system databases
	DefaultTableFilter = "!/^(mysql|sys|INFORMATION_SCHEMA|PERFORMANCE_SCHEMA|METRICS_SCHEMA|INSPECTION_SCHEMA)$/.*"

	defaultDumpThreads        = 128
	defaultDumpGCSafePointTTL = 5 * 60
	defaultEtcdDialTimeOut    = 3 * time.Second

	LooseCollationCompatible  = "loose"
	StrictCollationCompatible = "strict"

	dumplingServiceSafePointPrefix = "dumpling"
)

var (
	decodeRegionVersion = semver.New("3.0.0")
	gcSafePointVersion  = semver.New("4.0.0")
	tableSampleVersion  = semver.New("5.0.0-nightly")
)

func adjustConfig(conf *Config, fns ...func(*Config) error) error {
	for _, f := range fns {
		err := f(conf)
		if err != nil {
			return err
		}
	}
	return nil
}

func registerTLSConfig(conf *Config) error {
	if len(conf.Security.CAPath) > 0 {
		var err error
		var tlsConfig *tls.Config
		if len(conf.Security.SSLCABytes) == 0 {
			conf.Security.SSLCABytes, err = ioutil.ReadFile(conf.Security.CAPath)
			if err != nil {
				return errors.Trace(err)
			}
			if len(conf.Security.CertPath) > 0 {
				conf.Security.SSLCertBytes, err = ioutil.ReadFile(conf.Security.CertPath)
				if err != nil {
					return errors.Trace(err)
				}
			}
			if len(conf.Security.KeyPath) > 0 {
				conf.Security.SSLKEYBytes, err = ioutil.ReadFile(conf.Security.KeyPath)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		tlsConfig, err = util.ToTLSConfigWithVerifyByRawbytes(conf.Security.SSLCABytes,
			conf.Security.SSLCertBytes, conf.Security.SSLKEYBytes, []string{})
		if err != nil {
			return errors.Trace(err)
		}
		// NOTE for local test(use a self-signed or invalid certificate), we don't need to check CA file.
		// see more here https://github.com/go-sql-driver/mysql#tls
		if conf.Host == "127.0.0.1" || len(conf.Security.SSLCertBytes) == 0 || len(conf.Security.SSLKEYBytes) == 0 {
			tlsConfig.InsecureSkipVerify = true
		}
		err = mysql.RegisterTLSConfig("dumpling-tls-target", tlsConfig)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func validateSpecifiedSQL(conf *Config) error {
	if conf.SQL != "" && conf.Where != "" {
		return errors.New("can't specify both --sql and --where at the same time. Please try to combine them into --sql")
	}
	return nil
}

func adjustFileFormat(conf *Config) error {
	conf.FileType = strings.ToLower(conf.FileType)
	switch conf.FileType {
	case "":
		if conf.SQL != "" {
			conf.FileType = FileFormatCSVString
		} else {
			conf.FileType = FileFormatSQLTextString
		}
	case FileFormatSQLTextString:
		if conf.SQL != "" {
			return errors.Errorf("unsupported config.FileType '%s' when we specify --sql, please unset --filetype or set it to 'csv'", conf.FileType)
		}
	case FileFormatCSVString:
	default:
		return errors.Errorf("unknown config.FileType '%s'", conf.FileType)
	}
	return nil
}

func matchMysqlBugversion(info version.ServerInfo) bool {
	// if 8.0.3 <= mysql8 version < 8.0.23
	// FLUSH TABLES WITH READ LOCK could block other sessions from executing SHOW TABLE STATUS.
	// see more in https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-23.html
	if info.ServerType != version.ServerTypeMySQL {
		return false
	}
	currentVersion := info.ServerVersion
	bugVersionStart := semver.New("8.0.2")
	bugVersionEnd := semver.New("8.0.23")
	return bugVersionStart.LessThan(*currentVersion) && currentVersion.LessThan(*bugVersionEnd)
}
