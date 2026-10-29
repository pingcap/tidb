// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
<<<<<<< HEAD
=======
	"math/big"
	"net"
	"os"
	"path/filepath"
>>>>>>> 02dffa3bcf3 (dumpling: retain TLS compatibility with legacy MySQL servers (#72441))
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
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

func TestCreateExternalStorage(t *testing.T) {
	mockConfig := defaultConfigForTest(t)
	loc, err := mockConfig.createExternalStorage(tcontext.Background())
	require.NoError(t, err)
	require.Regexp(t, "^file:", loc.URI())
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

func TestOutputFilenameTemplateWithRowsValidation(t *testing.T) {
	t.Run("reject template without index when rows split mode and output template are both specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r or --filesize/-F")
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
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r or --filesize/-F")
	})

	t.Run("reject template when index is only conditionally rendered", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{if lt .Index 2}}{{.Index}}{{end}}{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r or --filesize/-F")
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
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r or --filesize/-F")
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
