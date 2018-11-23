// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2016 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"
)

var testDSNs = []struct {
	in  string
	out *Config
}{{
	"username:password@protocol(address)/dbname?param=value",
	&Config{User: "username", Passwd: "password", Net: "protocol", Addr: "address", DBName: "dbname", Params: map[string]string{"param": "value"}, Collation: "utf8_general_ci", Loc: time.UTC},
}, {
	"username:password@protocol(address)/dbname?param=value&columnsWithAlias=true",
	&Config{User: "username", Passwd: "password", Net: "protocol", Addr: "address", DBName: "dbname", Params: map[string]string{"param": "value"}, Collation: "utf8_general_ci", Loc: time.UTC, ColumnsWithAlias: true},
}, {
	"username:password@protocol(address)/dbname?param=value&columnsWithAlias=true&multiStatements=true",
	&Config{User: "username", Passwd: "password", Net: "protocol", Addr: "address", DBName: "dbname", Params: map[string]string{"param": "value"}, Collation: "utf8_general_ci", Loc: time.UTC, ColumnsWithAlias: true, MultiStatements: true},
}, {
	"user@unix(/path/to/socket)/dbname?charset=utf8",
	&Config{User: "user", Net: "unix", Addr: "/path/to/socket", DBName: "dbname", Params: map[string]string{"charset": "utf8"}, Collation: "utf8_general_ci", Loc: time.UTC},
}, {
	"user:password@tcp(localhost:5555)/dbname?charset=utf8&tls=true",
	&Config{User: "user", Passwd: "password", Net: "tcp", Addr: "localhost:5555", DBName: "dbname", Params: map[string]string{"charset": "utf8"}, Collation: "utf8_general_ci", Loc: time.UTC, TLSConfig: "true"},
}, {
	"user:password@tcp(localhost:5555)/dbname?charset=utf8mb4,utf8&tls=skip-verify",
	&Config{User: "user", Passwd: "password", Net: "tcp", Addr: "localhost:5555", DBName: "dbname", Params: map[string]string{"charset": "utf8mb4,utf8"}, Collation: "utf8_general_ci", Loc: time.UTC, TLSConfig: "skip-verify"},
}, {
	"user:password@/dbname?loc=UTC&timeout=30s&readTimeout=1s&writeTimeout=1s&allowAllFiles=1&clientFoundRows=true&allowOldPasswords=TRUE&collation=utf8mb4_unicode_ci&maxAllowedPacket=16777216",
	&Config{User: "user", Passwd: "password", Net: "tcp", Addr: "127.0.0.1:3306", DBName: "dbname", Collation: "utf8mb4_unicode_ci", Loc: time.UTC, Timeout: 30 * time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, AllowAllFiles: true, AllowOldPasswords: true, ClientFoundRows: true, MaxAllowedPacket: 16777216},
}, {
	"user:p@ss(word)@tcp([de:ad:be:ef::ca:fe]:80)/dbname?loc=Local",
	&Config{User: "user", Passwd: "p@ss(word)", Net: "tcp", Addr: "[de:ad:be:ef::ca:fe]:80", DBName: "dbname", Collation: "utf8_general_ci", Loc: time.Local},
}, {
	"/dbname",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", DBName: "dbname", Collation: "utf8_general_ci", Loc: time.UTC},
}, {
	"@/",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", Collation: "utf8_general_ci", Loc: time.UTC},
}, {
	"/",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", Collation: "utf8_general_ci", Loc: time.UTC},
}, {
	"",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", Collation: "utf8_general_ci", Loc: time.UTC},
}, {
	"user:p@/ssword@/",
	&Config{User: "user", Passwd: "p@/ssword", Net: "tcp", Addr: "127.0.0.1:3306", Collation: "utf8_general_ci", Loc: time.UTC},
}, {
	"unix/?arg=%2Fsome%2Fpath.ext",
	&Config{Net: "unix", Addr: "/tmp/mysql.sock", Params: map[string]string{"arg": "/some/path.ext"}, Collation: "utf8_general_ci", Loc: time.UTC},
}}

func TestDSNParser(t *testing.T) {
	for i, tst := range testDSNs {
		cfg, err := ParseDSN(tst.in)
		if err != nil {
			t.Error(err.Error())
		}

		// pointer not static
		cfg.tls = nil

		if !reflect.DeepEqual(cfg, tst.out) {
			t.Errorf("%d. ParseDSN(%q) mismatch:\ngot  %+v\nwant %+v", i, tst.in, cfg, tst.out)
		}
	}
}

func TestDSNParserInvalid(t *testing.T) {
	var invalidDSNs = []string{
		"@net(addr/",                  // no closing brace
		"@tcp(/",                      // no closing brace
		"tcp(/",                       // no closing brace
		"(/",                          // no closing brace
		"net(addr)//",                 // unescaped
		"User:pass@tcp(1.2.3.4:3306)", // no trailing slash
		//"/dbname?arg=/some/unescaped/path",
	}

	for i, tst := range invalidDSNs {
		if _, err := ParseDSN(tst); err == nil {
			t.Errorf("invalid DSN #%d. (%s) didn't error!", i, tst)
		}
	}
}

func TestDSNReformat(t *testing.T) {
	for i, tst := range testDSNs {
		dsn1 := tst.in
		cfg1, err := ParseDSN(dsn1)
		if err != nil {
			t.Error(err.Error())
			continue
		}
		cfg1.tls = nil // pointer not static
		res1 := fmt.Sprintf("%+v", cfg1)

		dsn2 := cfg1.FormatDSN()
		cfg2, err := ParseDSN(dsn2)
		if err != nil {
			t.Error(err.Error())
			continue
		}
		cfg2.tls = nil // pointer not static
		res2 := fmt.Sprintf("%+v", cfg2)

		if res1 != res2 {
			t.Errorf("%d. %q does not match %q", i, res2, res1)
		}
	}
}

func TestDSNWithCustomTLS(t *testing.T) {
	baseDSN := "User:password@tcp(localhost:5555)/dbname?tls="
	tlsCfg := tls.Config{}

	RegisterTLSConfig("utils_test", &tlsCfg)

	// Custom TLS is missing
	tst := baseDSN + "invalid_tls"
	cfg, err := ParseDSN(tst)
	if err == nil {
		t.Errorf("invalid custom TLS in DSN (%s) but did not error. Got config: %#v", tst, cfg)
	}

	tst = baseDSN + "utils_test"

	// Custom TLS with a server name
	name := "foohost"
	tlsCfg.ServerName = name
	cfg, err = ParseDSN(tst)

	if err != nil {
		t.Error(err.Error())
	} else if cfg.tls.ServerName != name {
		t.Errorf("did not get the correct TLS ServerName (%s) parsing DSN (%s).", name, tst)
	}

	// Custom TLS without a server name
	name = "localhost"
	tlsCfg.ServerName = ""
	cfg, err = ParseDSN(tst)

	if err != nil {
		t.Error(err.Error())
	} else if cfg.tls.ServerName != name {
		t.Errorf("did not get the correct ServerName (%s) parsing DSN (%s).", name, tst)
	} else if tlsCfg.ServerName != "" {
		t.Errorf("tlsCfg was mutated ServerName (%s) should be empty parsing DSN (%s).", name, tst)
	}

	DeregisterTLSConfig("utils_test")
}

func TestDSNWithCustomTLSQueryEscape(t *testing.T) {
	const configKey = "&%!:"
	dsn := "User:password@tcp(localhost:5555)/dbname?tls=" + url.QueryEscape(configKey)
	name := "foohost"
	tlsCfg := tls.Config{ServerName: name}

	RegisterTLSConfig(configKey, &tlsCfg)

	cfg, err := ParseDSN(dsn)

	if err != nil {
		t.Error(err.Error())
	} else if cfg.tls.ServerName != name {
		t.Errorf("did not get the correct TLS ServerName (%s) parsing DSN (%s).", name, dsn)
	}
}

func TestDSNUnsafeCollation(t *testing.T) {
	_, err := ParseDSN("/dbname?collation=gbk_chinese_ci&interpolateParams=true")
	if err != errInvalidDSNUnsafeCollation {
		t.Errorf("expected %v, got %v", errInvalidDSNUnsafeCollation, err)
	}

	_, err = ParseDSN("/dbname?collation=gbk_chinese_ci&interpolateParams=false")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=gbk_chinese_ci")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=ascii_bin&interpolateParams=true")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=latin1_german1_ci&interpolateParams=true")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=utf8_general_ci&interpolateParams=true")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=utf8mb4_general_ci&interpolateParams=true")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}
}

func TestParamsAreSorted(t *testing.T) {
	expected := "/dbname?interpolateParams=true&foobar=baz&quux=loo"
	dsn := &Config{
		DBName:            "dbname",
		InterpolateParams: true,
		Params: map[string]string{
			"quux":   "loo",
			"foobar": "baz",
		},
	}
	actual := dsn.FormatDSN()
	if actual != expected {
		t.Errorf("generic Config.Params were not sorted: want %#v, got %#v", expected, actual)
	}
}

func BenchmarkParseDSN(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, tst := range testDSNs {
			if _, err := ParseDSN(tst.in); err != nil {
				b.Error(err.Error())
			}
		}
	}
}
