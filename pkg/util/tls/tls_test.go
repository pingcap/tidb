package tls

import (
	"crypto/tls"
	"testing"
)

func TestVersionName(t *testing.T) {
	tests := []struct {
		version uint16
		name    string
	}{
		{tls.VersionSSL30, "SSLv3"},
		{tls.VersionTLS10, "TLS 1.0"},
		{tls.VersionTLS11, "TLS 1.1"},
		{tls.VersionTLS12, "TLSv1.2"},
		{tls.VersionTLS13, "TLSv1.3"},
		{tls.VersionTLS13 + 1, "0x0305"},
	}

	for _, tc := range tests {
		if n := VersionName(tc.version); n != tc.name {
			t.Fatalf("VersionName(%d) expected %s, but got %s", tc.version, tc.name, n)
		}
	}
}
