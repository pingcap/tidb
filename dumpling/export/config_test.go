// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/dumpformat/parquetfile"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/spf13/pflag"
	"github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGetDriverConfigTLSCipherSuites(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(fmt.Sprintf("configured=%t", configured), func(t *testing.T) {
			conf := DefaultConfig()
			if configured {
				conf.Security.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "mysql.example"}
			}
			driverCfg := conf.GetDriverConfig("")
			require.Equal(t, !configured, driverCfg.AllowFallbackToPlaintext)
			require.Equal(t, !configured, driverCfg.TLS.InsecureSkipVerify)
			require.Equal(t, uint16(tls.VersionTLS12), driverCfg.TLS.MinVersion)
			for _, suite := range []uint16{
				tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_RSA_WITH_AES_128_CBC_SHA,
				tls.TLS_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			} {
				require.Contains(t, driverCfg.TLS.CipherSuites, suite)
			}
			if configured {
				require.Nil(t, conf.Security.TLS.CipherSuites)
				require.Equal(t, "mysql.example", driverCfg.TLS.ServerName)
				require.NotSame(t, conf.Security.TLS, driverCfg.TLS)
			}
		})
	}

	// An explicit cipher policy, including an empty list, must not be widened.
	for _, suites := range [][]uint16{{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}, {}} {
		conf := DefaultConfig()
		conf.Security.TLS = &tls.Config{MinVersion: tls.VersionTLS12, CipherSuites: suites}
		require.Equal(t, suites, conf.GetDriverConfig("").TLS.CipherSuites)
	}
}

func TestGetDriverConfigTLSHandshake(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	for _, withCA := range []bool{false, true} {
		for _, suite := range []uint16{
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		} {
			t.Run(fmt.Sprintf("CA=%t/%s", withCA, tls.CipherSuiteName(suite)), func(t *testing.T) {
				conf := DefaultConfig()
				if withCA {
					conf.Security.SSLCABytes = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
				}
				require.NoError(t, buildTLSConfig(conf))
				listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
					Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
					MinVersion:   tls.VersionTLS12,
					MaxVersion:   tls.VersionTLS12,
					CipherSuites: []uint16{suite},
				})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, listener.Close()) })
				serverErr := make(chan error, 1)
				go func() {
					conn, err := listener.Accept()
					if err != nil {
						serverErr <- err
						return
					}
					defer conn.Close()
					if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err == nil {
						err = conn.(*tls.Conn).Handshake()
					}
					serverErr <- err
				}()
				client, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", listener.Addr().String(), conf.GetDriverConfig("").TLS)
				require.NoError(t, err)
				defer client.Close()
				require.Equal(t, suite, client.ConnectionState().CipherSuite)
				require.NoError(t, <-serverErr)
			})
		}
	}
}

func TestSPIFFETLS(t *testing.T) {
	const clientID, serverID = "spiffe://example.org/dumpling", "spiffe://example.org/ns/prod/tidb"

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	issue := func(id string, serial int64) ([]byte, []byte) {
		uri, err := url.Parse(id)
		require.NoError(t, err)
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			NotBefore:    ca.NotBefore,
			NotAfter:     ca.NotAfter,
			KeyUsage:     x509.KeyUsageDigitalSignature,
			URIs:         []*url.URL{uri},
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
		require.NoError(t, err)
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)
		return der, keyDER
	}
	clientDER, clientKeyDER := issue(clientID, 2)
	serverDER, _ := issue(serverID, 3)

	socket := filepath.Join(t.TempDir(), "api.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	server := grpc.NewServer()
	workload.RegisterSpiffeWorkloadAPIServer(server, &testSPIFFEWorkloadAPI{
		response: &workload.X509SVIDResponse{Svids: []*workload.X509SVID{{
			SpiffeId:    clientID,
			X509Svid:    clientDER,
			X509SvidKey: clientKeyDER,
			Bundle:      caDER,
		}}},
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		<-done
	})

	for _, tc := range []struct {
		name string
		id   string
	}{
		{name: "exact", id: serverID},
		{name: "wildcard", id: "spiffe://example.org/ns/*/tidb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			conf := parseConfigFromArgsForTest(t, "--spiffe-workload-api-addr", "unix://"+socket, "--spiffe-server-id", tc.id)
			d := &Dumper{
				conf:      conf,
				tctx:      tcontext.Background().WithContext(ctx),
				cancelCtx: cancel,
				metrics:   newMetrics(conf.PromFactory, conf.Labels),
			}
			d.metrics.registerTo(conf.PromRegistry)
			require.NoError(t, d.initTLS())
			t.Cleanup(func() {
				require.NoError(t, d.Close())
				_, err := conf.Security.TLS.GetClientCertificate(nil)
				require.Error(t, err)
			})
			driverCfg := conf.GetDriverConfig("")
			require.False(t, driverCfg.AllowFallbackToPlaintext)
			require.NotNil(t, driverCfg.TLS.GetClientCertificate)
			require.NotNil(t, driverCfg.TLS.VerifyPeerCertificate)
			clientCert, err := driverCfg.TLS.GetClientCertificate(nil)
			require.NoError(t, err)
			require.Equal(t, [][]byte{clientDER}, clientCert.Certificate)
			require.NoError(t, driverCfg.TLS.VerifyPeerCertificate([][]byte{serverDER}, nil))
			require.Error(t, driverCfg.TLS.VerifyPeerCertificate([][]byte{clientDER}, nil))
		})
	}
}

type testSPIFFEWorkloadAPI struct {
	workload.UnimplementedSpiffeWorkloadAPIServer
	response *workload.X509SVIDResponse
}

func (s *testSPIFFEWorkloadAPI) FetchX509SVID(_ *workload.X509SVIDRequest, stream workload.SpiffeWorkloadAPI_FetchX509SVIDServer) error {
	if err := stream.Send(s.response); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestSPIFFETLSConfigValidation(t *testing.T) {
	const endpoint, serverID = "unix:///tmp/spiffe.sock", "spiffe://example.org/tidb"
	for _, tc := range []struct {
		name     string
		endpoint string
		serverID string
		caPath   string
		caBytes  []byte
		want     string
	}{
		{name: "TCP endpoint", endpoint: "tcp://127.0.0.1:8081", serverID: serverID, want: "unix:///"},
		{name: "relative Unix endpoint", endpoint: "unix://relative.sock", serverID: serverID, want: "unix:///"},
		{name: "missing server ID", endpoint: endpoint, want: "--spiffe-server-id"},
		{name: "missing endpoint", serverID: serverID, want: "--spiffe-workload-api-addr"},
		{name: "invalid server ID", endpoint: endpoint, serverID: "example.org", want: "--spiffe-server-id"},
		{name: "server ID without a workload path", endpoint: endpoint, serverID: "spiffe://example.org", want: "--spiffe-server-id"},
		{name: "wildcard trust domain", endpoint: endpoint, serverID: "spiffe://*.example.org/ns/tidb", want: "--spiffe-server-id"},
		{name: "partial wildcard segment", endpoint: endpoint, serverID: "spiffe://example.org/ns/tidb-*", want: "--spiffe-server-id"},
		{name: "globstar segment", endpoint: endpoint, serverID: "spiffe://example.org/ns/**/tidb", want: "--spiffe-server-id"},
		{name: "PEM path conflict", endpoint: endpoint, serverID: serverID, caPath: "ca.pem", want: "file or byte TLS credentials"},
		{name: "PEM content conflict", endpoint: endpoint, serverID: serverID, caBytes: []byte("ca"), want: "file or byte TLS credentials"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := DefaultConfig()
			conf.Security.SPIFFEWorkloadAPIAddr = tc.endpoint
			conf.Security.SPIFFEServerID = tc.serverID
			conf.Security.CAPath = tc.caPath
			conf.Security.SSLCABytes = tc.caBytes
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			d := &Dumper{conf: conf, tctx: tcontext.Background().WithContext(ctx)}
			require.ErrorContains(t, d.initTLS(), tc.want)
		})
	}
	_, err := parseConfigFromArgsForTestWithErr(t, "--dump-service", "unix:///tmp/dump.sock", "--spiffe-workload-api-addr", endpoint)
	require.ErrorContains(t, err, "--spiffe-workload-api-addr cannot be combined with --dump-service")
}

func TestSPIFFEServerMatcher(t *testing.T) {
	match, err := parseSPIFFEServerMatcher("spiffe://example.org/ns/*/tidb/*")
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		id    string
		match bool
	}{
		{name: "one segment per wildcard", id: "spiffe://example.org/ns/prod/tidb/server-1", match: true},
		{name: "different trust domain", id: "spiffe://other.org/ns/prod/tidb/server-1"},
		{name: "missing segment", id: "spiffe://example.org/ns/tidb/server-1"},
		{name: "extra segment", id: "spiffe://example.org/ns/prod/zone/tidb/server-1"},
		{name: "different literal segment", id: "spiffe://example.org/ns/prod/pd/server-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, err := spiffeid.FromString(tc.id)
			require.NoError(t, err)
			if tc.match {
				require.NoError(t, match(id))
			} else {
				require.Error(t, match(id))
			}
		})
	}
}

func TestCreateExternalStorage(t *testing.T) {
	mockConfig := defaultConfigForTest(t)
	loc, err := mockConfig.createExternalStorage(tcontext.Background())
	require.NoError(t, err)
	require.Regexp(t, "^file:", loc.URI())
	loc.Close()
}

func TestMatchMysqlBugVersion(t *testing.T) {
	cases := []struct {
		serverInfo version.ServerInfo
		expected   bool
	}{
		{version.ParseServerInfo("5.7.25-TiDB-3.0.6"), false},
		{version.ParseServerInfo("8.0.2"), false},
		{version.ParseServerInfo("8.0.3"), true},
		{version.ParseServerInfo("8.0.22"), true},
		{version.ParseServerInfo("8.0.23"), false},
	}
	for _, x := range cases {
		require.Equalf(t, x.expected, matchMysqlBugversion(x.serverInfo), "server info: %s", x.serverInfo)
	}
}

func TestGetConfTables(t *testing.T) {
	tablesList := []string{"db1t1", "db2.t1"}
	_, err := GetConfTables(tablesList)
	require.EqualError(t, err, fmt.Sprintf("--tables-list only accepts qualified table names, but `%s` lacks a dot", tablesList[0]))

	tablesList = []string{"db1.t1", "db2t1"}
	_, err = GetConfTables(tablesList)
	require.EqualError(t, err, fmt.Sprintf("--tables-list only accepts qualified table names, but `%s` lacks a dot", tablesList[1]))

	tablesList = []string{"db1.t1", "db2.t1"}
	expectedDBTables := NewDatabaseTables().
		AppendTables("db1", []string{"t1"}, []uint64{0}).
		AppendTables("db2", []string{"t1"}, []uint64{0})
	actualDBTables, err := GetConfTables(tablesList)
	require.NoError(t, err)
	require.Equal(t, expectedDBTables, actualDBTables)
}

func TestColumnFilters(t *testing.T) {
	columnFilter := columnFilterConfig{
		Filters: []columnFilterRule{
			{Matcher: []string{"db1.*"}, Columns: []string{"*", "!c*"}},
			{Matcher: []string{"db1.t1"}, Columns: []string{"c2"}},
			{Matcher: []string{"db1.t2"}, Columns: []string{"*", "!c3"}},
		},
	}
	require.NoError(t, columnFilter.compileForOption(false, flagColumnFilterFile))

	selectedFields, selectedIndexes, err := columnFilter.applyToColumns("DB1", "T1", []string{"c1", "C2", "c3", "d"})
	require.NoError(t, err)
	require.Equal(t, []string{"C2", "d"}, selectedFields)
	require.Equal(t, []int{1, 3}, selectedIndexes)

	selectedFields, selectedIndexes, err = columnFilter.applyToColumns("db2", "t1", []string{"c1"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, selectedFields)
	require.Equal(t, []int{0}, selectedIndexes)

	selectedFields, selectedIndexes, err = columnFilter.applyToColumns("db1", "t2", []string{"c1", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, selectedFields)
	require.Equal(t, []int{0}, selectedIndexes)

	columnFilter = columnFilterConfig{
		Filters: []columnFilterRule{
			{Matcher: []string{"db1.t1"}},
		},
	}
	err = columnFilter.compileForOption(false, flagColumnFilterFile)
	require.ErrorContains(t, err, "--column-filter-file filter 0 requires at least one column rule")
}

func TestParseColumnFilterFile(t *testing.T) {
	path := writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db1.t1", "db2.t2"]
columns = ["c1", "C2"]
`)
	columnFilter, err := parseColumnFilterConfig(path, false)
	require.NoError(t, err)
	selectedFields, selectedIndexes, err := columnFilter.applyToColumns("DB1", "T1", []string{"c1", "C2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1", "C2"}, selectedFields)
	require.Equal(t, []int{0, 1}, selectedIndexes)

	columnFilter, err = parseColumnFilterConfig(path, true)
	require.NoError(t, err)
	selectedFields, selectedIndexes, err = columnFilter.applyToColumns("DB1", "T1", []string{"c1", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1", "c3"}, selectedFields)
	require.Equal(t, []int{0, 1}, selectedIndexes)

	path = writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db.t"]
columns = ["/unterminated"]
`)
	_, err = parseColumnFilterConfig(path, false)
	require.ErrorContains(t, err, "failed to parse --column-filter-file filter 0 columns")

	path = writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db.t"]
colums = ["*"]
`)
	_, err = parseColumnFilterConfig(path, false)
	require.ErrorContains(t, err, "--column-filter-file contains unknown TOML keys: filters.colums")

	for _, content := range []string{
		`
[[filters]]
matcher = ["db.t"]
`,
		`
[[filters]]
matcher = ["db.t"]
columns = []
`,
	} {
		path = writeColumnFilterFileForTest(t, content)
		_, err = parseColumnFilterConfig(path, false)
		require.ErrorContains(t, err, "--column-filter-file filter 0 requires at least one column rule")
	}
}

func TestParseColumnFilterFileFlag(t *testing.T) {
	conf := parseConfigFromArgsForTest(t, "--no-schemas")
	require.Empty(t, conf.columnFilter.Filters)

	path := writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db1.t1"]
columns = ["*", "!c1", "!c2"]

[[filters]]
matcher = ["db2.t2"]
columns = ["*", "!c3"]
`)
	conf = parseConfigFromArgsForTest(t,
		"--no-schemas",
		"--column-filter-file", path,
	)
	selectedFields, selectedIndexes, err := conf.columnFilter.applyToColumns("db1", "t1", []string{"c1", "c2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c3"}, selectedFields)
	require.Equal(t, []int{2}, selectedIndexes)

	conf = parseConfigFromArgsForTest(t, "--column-filter-file", path)
	require.False(t, conf.NoSchemas)

	_, err = parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter-file", path,
		"--sql", "select * from t",
	)
	require.ErrorContains(t, err, "can't specify both --sql and --column-filter-file at the same time")
}

func TestSessionParamKeys(t *testing.T) {
	conf := parseConfigFromArgsForTest(t, "--params", "SQL_MODE=ANSI_QUOTES")
	require.Equal(t, "ANSI_QUOTES", conf.SessionParams["sql_mode"])
	require.NotContains(t, conf.SessionParams, "SQL_MODE")
}

func TestParseColumnFilterFlag(t *testing.T) {
	conf := parseConfigFromArgsForTest(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db1.t1"], columns = ["*", "!c1", "!c2"] }`,
		"--column-filter", `{ matcher = ["db2.t2"], columns = ["c3"] }`,
	)
	selectedFields, selectedIndexes, err := conf.columnFilter.applyToColumns("db1", "t1", []string{"c1", "c2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c3"}, selectedFields)
	require.Equal(t, []int{2}, selectedIndexes)

	selectedFields, selectedIndexes, err = conf.columnFilter.applyToColumns("db2", "t2", []string{"c1", "c2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c3"}, selectedFields)
	require.Equal(t, []int{2}, selectedIndexes)

	_, err = parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db.t"], columns = ["/unterminated"] }`,
	)
	require.ErrorContains(t, err, "failed to parse --column-filter filter 0 columns")

	_, err = parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db.t"], colums = ["*"] }`,
	)
	require.ErrorContains(t, err, "--column-filter contains unknown TOML keys: filter.colums")

	for _, arg := range []string{
		`{ matcher = ["db.t"] }`,
		`{ matcher = ["db.t"], columns = [] }`,
	} {
		_, err = parseConfigFromArgsForTestWithErr(t,
			"--no-schemas",
			"--column-filter", arg,
		)
		require.ErrorContains(t, err, "--column-filter filter 0 requires at least one column rule")
	}

	conf = parseConfigFromArgsForTest(t,
		"--column-filter", `{ matcher = ["db.t"], columns = ["*"] }`,
	)
	require.False(t, conf.NoSchemas)

	_, err = parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db.t"], columns = ["*"] }`,
		"--sql", "select * from t",
	)
	require.ErrorContains(t, err, "can't specify both --sql and --column-filter at the same time")
}

func TestColumnFilterConflict(t *testing.T) {
	path := writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db.t"]
columns = ["*"]
`)
	_, err := parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db.t"], columns = ["*"] }`,
		"--column-filter-file", path,
	)
	require.ErrorContains(t, err, "can't specify both --column-filter and --column-filter-file at the same time")
}

func TestColumnFilterOptions(t *testing.T) {
	conf := DefaultConfig()
	conf.NoSchemas = true
	require.NoError(t, validateColumnFilterOptions(conf, flagColumnFilterFile))

	conf = DefaultConfig()
	conf.NoSchemas = true
	conf.SQL = "select * from t"
	require.ErrorContains(
		t,
		validateColumnFilterOptions(conf, flagColumnFilterFile),
		"can't specify both --sql and --column-filter-file at the same time",
	)

	conf = DefaultConfig()
	require.NoError(t, validateColumnFilterOptions(conf, flagColumnFilterFile))
}

func writeColumnFilterFileForTest(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "column-filter.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func newColumnFilterConfigForTest(t *testing.T, filters ...columnFilterRule) columnFilterConfig {
	t.Helper()
	columnFilter := columnFilterConfig{
		Filters: filters,
	}
	require.NoError(t, columnFilter.compileForOption(false, flagColumnFilterFile))
	return columnFilter
}

func TestParseParquetDefaultFlags(t *testing.T) {
	defaultConf := DefaultConfig()
	require.Equal(t, parquetfile.DefaultCompressionType, defaultConf.ParquetCompressType)
	require.EqualValues(t, units.MiB, defaultConf.ParquetPageSize)
	require.EqualValues(t, 120*units.MiB, defaultConf.ParquetRowGroupSize)

	conf := parseConfigFromArgsForTest(t)
	require.EqualValues(t, units.MiB, conf.ParquetPageSize)
	require.EqualValues(t, 120*units.MiB, conf.ParquetRowGroupSize)
	require.Equal(t, parquetfile.DefaultCompressionType, conf.ParquetCompressType)

	t.Run("parseParquetCompressType uses parquetfile policy", func(t *testing.T) {
		tp, err := parseParquetCompressType("")
		require.NoError(t, err)
		require.Equal(t, parquetfile.DefaultCompressionType, tp)

		tp, err = parseParquetCompressType("zstd")
		require.NoError(t, err)
		require.Equal(t, compressedio.Zstd, tp)
	})
}

func TestParseParquetSizeFlags(t *testing.T) {
	conf := parseConfigFromArgsForTest(t,
		"--filetype", "parquet",
		"--parquet-page-size", "2MiB",
		"--parquet-row-group-size", "128MiB",
	)
	require.EqualValues(t, 2*units.MiB, conf.ParquetPageSize)
	require.EqualValues(t, 128*units.MiB, conf.ParquetRowGroupSize)
}

func TestParseCSVOutputDialectAcceptsUppercaseFileTypeAndDialect(t *testing.T) {
	conf := parseConfigFromArgsForTest(t,
		"--filetype", "CSV",
		"--csv-output-dialect", "SNOWFLAKE",
	)
	require.Equal(t, CSVDialectSnowflake, conf.CsvOutputDialect)
}

func TestOutputFilenameTemplateWithRowsValidation(t *testing.T) {
	t.Run("reject template without index when rows split mode and output template are both specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r, --filesize/-F, or --dump-service")
	})

	t.Run("accept template with index when rows and output template are both specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{.DB}}.{{.Table}}.{{.Index}}",
		)
		require.NoError(t, err)
	})

	t.Run("reject template when index only appears in a conditional guard", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{if .Index}}{{end}}{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r, --filesize/-F, or --dump-service")
	})

	t.Run("reject template when index is only conditionally rendered", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{if lt .Index 2}}{{.Index}}{{end}}{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r, --filesize/-F, or --dump-service")
	})

	t.Run("accept template when standalone index exists outside conditionals", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{if lt .Index 2}}prefix.{{end}}{{.DB}}.{{.Table}}.{{.Index}}",
		)
		require.NoError(t, err)
	})

	t.Run("accept template without index when rows is explicitly set to zero", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "0",
			"--output-filename-template", "{{.DB}}.{{.Table}}",
		)
		require.NoError(t, err)
	})

	t.Run("reject template without index when filesize split mode and output template are both specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--filesize", "1MiB",
			"--output-filename-template", "{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r, --filesize/-F, or --dump-service")
	})

	t.Run("accept template with index when filesize and output template are both specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--filesize", "1MiB",
			"--output-filename-template", "{{.DB}}.{{.Table}}.{{.Index}}",
		)
		require.NoError(t, err)
	})

	t.Run("accept template without index when rows is not specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--output-filename-template", "{{.DB}}.{{.Table}}",
		)
		require.NoError(t, err)
	})
}

func TestDumpServiceFlags(t *testing.T) {
	conf := parseConfigFromArgsForTest(t, "--dump-service", "unix:///tmp/dumper.sock", "--threads", "32", "--filetype", "csv", "--filter", "test.*")
	require.Equal(t, "unix:///tmp/dumper.sock", conf.DumpService)
	require.Equal(t, 32, conf.Threads)
	require.NoError(t, validateDumpService(conf))
	for _, fileSize := range []string{"", "0", "1MiB"} {
		for _, testCase := range []struct {
			name     string
			template string
			wantErr  bool
		}{
			{name: "default template"},
			{name: "missing index", template: "{{.DB}}.{{.Table}}", wantErr: true},
			{name: "conditional guard", template: "{{if .Index}}{{end}}{{.DB}}.{{.Table}}", wantErr: true},
			{name: "conditional index", template: "{{if .Table}}{{.Index}}{{end}}{{.DB}}.{{.Table}}", wantErr: true},
			{name: "standalone index", template: "{{.DB}}.{{.Table}}.{{.Index}}"},
		} {
			t.Run("output template/filesize="+fileSize+"/"+testCase.name, func(t *testing.T) {
				args := []string{"--dump-service", "unix:///tmp/dumper.sock"}
				if fileSize != "" {
					args = append(args, "--filesize", fileSize)
				}
				if testCase.template != "" {
					args = append(args, "--output-filename-template", testCase.template)
				}
				_, err := parseConfigFromArgsForTestWithErr(t, args...)
				if testCase.wantErr {
					require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks")
					return
				}
				require.NoError(t, err)
			})
		}
	}
	for _, option := range []string{flagColumnFilter, flagColumnFilterFile} {
		t.Run(option, func(t *testing.T) {
			value := `{ matcher = ["test.*"], columns = ["*", "!secret"] }`
			if option == flagColumnFilterFile {
				value = writeColumnFilterFileForTest(t, "[[filters]]\nmatcher = [\"test.*\"]\ncolumns = [\"*\", \"!secret\"]\n")
			}
			conf := parseConfigFromArgsForTest(t, "--dump-service", "unix:///tmp/dumper.sock", "--"+option, value)
			columns, indexes, err := conf.columnFilter.applyToColumns("test", "t", []string{"id", "secret", "name"})
			require.NoError(t, err)
			require.Equal(t, []string{"id", "name"}, columns)
			require.Equal(t, []int{0, 2}, indexes)
		})
	}
	invalidConf := DefaultConfig()
	invalidConf.DumpService = "http://localhost/dumper.sock"
	require.Error(t, validateDumpService(invalidConf))

	for _, flagName := range dumpServiceIncompatibleFlags {
		t.Run(flagName, func(t *testing.T) {
			flags := pflag.NewFlagSet("dumpling", pflag.ContinueOnError)
			DefaultConfig().DefineFlags(flags)
			require.NoError(t, flags.Set(flagDumpService, "unix:///tmp/dumper.sock"))
			flags.Lookup(flagName).Changed = true
			require.EqualError(t, validateDumpServiceFlags(flags), "--"+flagName+" cannot be combined with --dump-service")
		})
	}
	_, err := parseConfigFromArgsForTestWithErr(t, "--dump-service", "")
	require.Error(t, err)
	require.Empty(t, parseConfigFromArgsForTest(t).DumpService)
}

func parseConfigFromArgsForTest(t *testing.T, args ...string) *Config {
	t.Helper()
	conf, err := parseConfigFromArgsForTestWithErr(t, args...)
	require.NoError(t, err)
	return conf
}

func parseConfigFromArgsForTestWithErr(t *testing.T, args ...string) (*Config, error) {
	t.Helper()
	conf := DefaultConfig()
	flags := pflag.NewFlagSet("dumpling", pflag.ContinueOnError)
	conf.DefineFlags(flags)
	oldCommandLine := pflag.CommandLine
	pflag.CommandLine = flags
	t.Cleanup(func() {
		pflag.CommandLine = oldCommandLine
	})
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	return conf, conf.ParseFromFlags(flags)
}

func TestParseIncludeGeneratedColumns(t *testing.T) {
	conf := parseConfigFromArgsForTest(t)
	require.Equal(t, GeneratedColumnsNone, conf.IncludeGeneratedColumns)

	for _, value := range []string{"none", "NONE", ""} {
		conf = parseConfigFromArgsForTest(t, "--include-generated-columns", value)
		require.Equal(t, GeneratedColumnsNone, conf.IncludeGeneratedColumns)
	}

	// The option only changes data output, so it doesn't require --no-schemas.
	conf = parseConfigFromArgsForTest(t, "--include-generated-columns", "Stored", "--filetype", "csv")
	require.Equal(t, GeneratedColumnsStored, conf.IncludeGeneratedColumns)
	require.False(t, conf.NoSchemas)

	for _, value := range []string{"virtual", "all"} {
		_, err := parseConfigFromArgsForTestWithErr(t, "--include-generated-columns", value)
		require.ErrorContains(t, err, "--include-generated-columns="+value+" is not supported yet")
	}
	_, err := parseConfigFromArgsForTestWithErr(t, "--include-generated-columns=true")
	require.ErrorContains(t, err, "invalid --include-generated-columns value 'true'")
}

func TestAskPasswordFlags(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		args     []string
		password string
		wantAsk  bool
		wantErr  string
	}{
		{name: "default"},
		{name: "long flag", args: []string{"--ask-password"}, wantAsk: true},
		{name: "short flag", args: []string{"-a"}, wantAsk: true},
		{name: "disabled", args: []string{"--ask-password=false", "-p", "secret"}, password: "secret"},
		{name: "existing password", args: []string{"-p", "secret"}, password: "secret"},
		{name: "explicit empty password", args: []string{"--password="}},
		{name: "conflicting password", args: []string{"--ask-password", "-p", "secret"}, wantErr: "--ask-password cannot be combined with --password"},
		{name: "conflicting empty password", args: []string{"--password=", "--ask-password"}, wantErr: "--ask-password cannot be combined with --password"},
		{name: "dump service", args: []string{"--dump-service=unix:///tmp/dumper.sock", "--ask-password"}, wantErr: "--ask-password cannot be combined with --dump-service"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			conf, err := parseConfigFromArgsForTestWithErr(t, testCase.args...)
			if testCase.wantErr != "" {
				require.EqualError(t, err, testCase.wantErr)
				require.NotContains(t, err.Error(), "secret")
				return
			}
			require.NoError(t, err)
			askPassword, err := pflag.CommandLine.GetBool(flagAskPassword)
			require.NoError(t, err)
			require.Equal(t, testCase.wantAsk, askPassword)
			require.Equal(t, testCase.password, conf.Password)
		})
	}
}

func TestConfigStringRedactsCredentials(t *testing.T) {
	conf := defaultConfigForTest(t)
	conf.Password = "SQL_PASSWORD_SECRET"
	conf.OutputDirPath = "s3://bucket/path?access-key=AKID&secret_access_key=SKEY&session-token=TOKEN&region=us-east-1"
	conf.S3.AccessKey = "AKID"
	conf.S3.SecretAccessKey = "SKEY"
	conf.S3.SessionToken = "TOKEN"
	conf.Azblob.AccountKey = "AZKEY"
	conf.Azblob.SASToken = "SAS"
	conf.Azblob.EncryptionKey = "ENC"
	conf.Azblob.Endpoint = "https://account.blob.core.windows.net/?sig=SASQUERY"

	str := conf.String()
	for _, secret := range []string{"SQL_PASSWORD_SECRET", "AKID", "SKEY", "TOKEN", "AZKEY", "SAS", "ENC", "SASQUERY"} {
		require.NotContains(t, str, secret)
	}
	require.Contains(t, str, "region=us-east-1")
	require.Equal(t, "AKID", conf.S3.AccessKey)
	require.Contains(t, conf.OutputDirPath, "AKID")

	conf.OutputDirPath = "s3://bucket:port/path?access-key=AKID&secret-access-key=SKEY"
	str = conf.String()
	require.NotContains(t, str, "AKID")
	require.NotContains(t, str, "SKEY")
}
