// Copyright 2024 PingCAP, Inc.
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

package privileges

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseHostIPNet(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *net.IPNet
	}{
		{
			name:  "valid subnet mask notation",
			input: "192.168.1.0/255.255.255.0",
			expected: &net.IPNet{
				IP:   net.ParseIP("192.168.1.0").To4(),
				Mask: net.IPv4Mask(255, 255, 255, 0),
			},
		},
		{
			name:  "valid CIDR notation /24",
			input: "192.168.1.0/24",
			expected: &net.IPNet{
				IP:   net.ParseIP("192.168.1.0").To4(),
				Mask: net.CIDRMask(24, 32),
			},
		},
		{
			name:  "valid CIDR notation /16",
			input: "10.0.0.0/16",
			expected: &net.IPNet{
				IP:   net.ParseIP("10.0.0.0").To4(),
				Mask: net.CIDRMask(16, 32),
			},
		},
		{
			name:  "valid CIDR notation /8",
			input: "127.0.0.0/8",
			expected: &net.IPNet{
				IP:   net.ParseIP("127.0.0.0").To4(),
				Mask: net.CIDRMask(8, 32),
			},
		},
		{
			name:  "valid CIDR notation /32",
			input: "192.168.1.100/32",
			expected: &net.IPNet{
				IP:   net.ParseIP("192.168.1.100").To4(),
				Mask: net.CIDRMask(32, 32),
			},
		},
		{
			name:  "valid CIDR notation /0",
			input: "0.0.0.0/0",
			expected: &net.IPNet{
				IP:   net.ParseIP("0.0.0.0").To4(),
				Mask: net.CIDRMask(0, 32),
			},
		},
		{
			name:     "no slash - should return nil",
			input:    "192.168.1.0",
			expected: nil,
		},
		{
			name:     "invalid IP address",
			input:    "invalid.ip/24",
			expected: nil,
		},
		{
			name:     "invalid CIDR number",
			input:    "192.168.1.0/abc",
			expected: nil,
		},
		{
			name:     "CIDR out of range - negative",
			input:    "192.168.1.0/-1",
			expected: nil,
		},
		{
			name:     "CIDR out of range - too large",
			input:    "192.168.1.0/33",
			expected: nil,
		},
		{
			name:     "invalid subnet mask",
			input:    "192.168.1.0/255.255.255.1",
			expected: nil,
		},
		{
			name:     "invalid subnet mask IP",
			input:    "192.168.1.0/invalid.mask",
			expected: nil,
		},
		{
			name:     "host IP not aligned with subnet mask",
			input:    "192.168.1.1/255.255.255.0",
			expected: nil,
		},
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "only slash",
			input:    "/",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseHostIPNet(tt.input)
			if tt.expected == nil {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Equal(t, tt.expected.IP, result.IP)
				require.Equal(t, tt.expected.Mask, result.Mask)
			}
		})
	}
}

func TestHostIPNetContains(t *testing.T) {
	tests := []struct {
		name      string
		network   string
		testIP    string
		shouldMatch bool
	}{
		{
			name:      "subnet mask - IP in range",
			network:   "192.168.1.0/255.255.255.0",
			testIP:    "192.168.1.100",
			shouldMatch: true,
		},
		{
			name:      "subnet mask - IP out of range",
			network:   "192.168.1.0/255.255.255.0",
			testIP:    "192.168.2.100",
			shouldMatch: false,
		},
		{
			name:      "CIDR /24 - IP in range",
			network:   "192.168.1.0/24",
			testIP:    "192.168.1.50",
			shouldMatch: true,
		},
		{
			name:      "CIDR /24 - IP out of range",
			network:   "192.168.1.0/24",
			testIP:    "192.168.2.50",
			shouldMatch: false,
		},
		{
			name:      "CIDR /16 - IP in range",
			network:   "10.0.0.0/16",
			testIP:    "10.0.255.255",
			shouldMatch: true,
		},
		{
			name:      "CIDR /16 - IP out of range",
			network:   "10.0.0.0/16",
			testIP:    "10.1.0.0",
			shouldMatch: false,
		},
		{
			name:      "CIDR /32 - exact match",
			network:   "192.168.1.100/32",
			testIP:    "192.168.1.100",
			shouldMatch: true,
		},
		{
			name:      "CIDR /32 - different IP",
			network:   "192.168.1.100/32",
			testIP:    "192.168.1.101",
			shouldMatch: false,
		},
		{
			name:      "CIDR /0 - any IP",
			network:   "0.0.0.0/0",
			testIP:    "255.255.255.255",
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipNet := parseHostIPNet(tt.network)
			require.NotNil(t, ipNet, "Failed to parse network: %s", tt.network)
			
			testIP := net.ParseIP(tt.testIP)
			require.NotNil(t, testIP, "Failed to parse test IP: %s", tt.testIP)
			
			result := ipNet.Contains(testIP)
			require.Equal(t, tt.shouldMatch, result)
		})
	}
}

func TestIsValidHostPattern(t *testing.T) {
	tests := []struct {
		name        string
		host        string
		shouldValid bool
	}{
		{
			name:        "any host wildcard",
			host:        "%",
			shouldValid: true,
		},
		{
			name:        "empty string",
			host:        "",
			shouldValid: true,
		},
		{
			name:        "valid subnet mask",
			host:        "192.168.1.0/255.255.255.0",
			shouldValid: true,
		},
		{
			name:        "valid CIDR /24",
			host:        "192.168.1.0/24",
			shouldValid: true,
		},
		{
			name:        "valid CIDR /16",
			host:        "10.0.0.0/16",
			shouldValid: true,
		},
		{
			name:        "valid wildcard pattern",
			host:        "192.168.1.%",
			shouldValid: true,
		},
		{
			name:        "valid wildcard with domain",
			host:        "%.example.com",
			shouldValid: true,
		},
		{
			name:        "valid exact IP",
			host:        "192.168.1.100",
			shouldValid: true,
		},
		{
			name:        "valid localhost",
			host:        "localhost",
			shouldValid: true,
		},
		{
			name:        "valid hostname",
			host:        "example.com",
			shouldValid: true,
		},
		{
			name:        "invalid subnet mask",
			host:        "192.168.1.0/255.255.255.1",
			shouldValid: false,
		},
		{
			name:        "invalid CIDR",
			host:        "192.168.1.0/33",
			shouldValid: false,
		},
		{
			name:        "invalid IP in CIDR",
			host:        "invalid.ip/24",
			shouldValid: false,
		},
		{
			name:        "invalid wildcard pattern",
			host:        "192.168.%.%",
			shouldValid: false,
		},
		{
			name:        "multiple slashes",
			host:        "192.168.1.0/24/32",
			shouldValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsValidHostPattern(tt.host)
			require.Equal(t, tt.shouldValid, result)
		})
	}
}

func TestCIDRNetworkNormalization(t *testing.T) {
	// Test that CIDR networks are properly normalized
	// For example, 192.168.1.100/24 should become 192.168.1.0/24
	ipNet := parseHostIPNet("192.168.1.100/24")
	require.NotNil(t, ipNet)
	require.Equal(t, net.ParseIP("192.168.1.0").To4(), ipNet.IP)
	require.Equal(t, net.CIDRMask(24, 32), ipNet.Mask)
	
	// Test that 10.0.255.255/16 becomes 10.0.0.0/16
	ipNet = parseHostIPNet("10.0.255.255/16")
	require.NotNil(t, ipNet)
	require.Equal(t, net.ParseIP("10.0.0.0").To4(), ipNet.IP)
	require.Equal(t, net.CIDRMask(16, 32), ipNet.Mask)
}

func TestBackwardCompatibility(t *testing.T) {
	// Test that existing subnet mask notation still works
	ipNet := parseHostIPNet("127.0.0.0/255.255.255.0")
	require.NotNil(t, ipNet)
	require.Equal(t, net.ParseIP("127.0.0.0").To4(), ipNet.IP)
	require.Equal(t, net.IPv4Mask(255, 255, 255, 0), ipNet.Mask)
	
	// Test that it contains the expected IPs
	require.True(t, ipNet.Contains(net.ParseIP("127.0.0.1")))
	require.True(t, ipNet.Contains(net.ParseIP("127.0.0.255")))
	require.False(t, ipNet.Contains(net.ParseIP("127.0.1.1")))
}
