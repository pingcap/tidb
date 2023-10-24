//go:build boringcrypto
// +build boringcrypto

package main

import _ "crypto/tls/fipsonly"

import "github.com/pingcap/tidb/pkg/parser/mysql"

func init() {
	mysql.TiDBReleaseVersion += "-fips"
}
