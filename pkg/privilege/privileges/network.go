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
	"strconv"
	"strings"
)

// parseHostIPNet parses an IPv4 address and its subnet mask (e.g. `127.0.0.0/255.255.255.0`),
// or CIDR notation (e.g. `127.0.0.0/24`), return the `IPNet` struct which represent the IP range info.
// `IPNet` is used to check if a giving IP (e.g. `127.0.0.1`) is in its IP range by call `IPNet.Contains(ip)`.
func parseHostIPNet(s string) *net.IPNet {
	i := strings.IndexByte(s, '/')
	if i < 0 {
		return nil
	}
	
	hostIP := net.ParseIP(s[:i]).To4()
	if hostIP == nil {
		return nil
	}
	
	maskPart := s[i+1:]
	
	// Try to parse as CIDR notation first (e.g., /24)
	if cidr, err := strconv.Atoi(maskPart); err == nil {
		if cidr < 0 || cidr > 32 {
			return nil
		}
		mask := net.CIDRMask(cidr, 32)
		// For CIDR notation, we need to normalize the IP to the network address
		networkIP := hostIP.Mask(mask)
		return &net.IPNet{
			IP:   networkIP,
			Mask: mask,
		}
	}
	
	// Try to parse as subnet mask notation (e.g., /255.255.255.0)
	maskIP := net.ParseIP(maskPart).To4()
	if maskIP == nil {
		return nil
	}
	
	mask := net.IPv4Mask(maskIP[0], maskIP[1], maskIP[2], maskIP[3])
	// Validate that the subnet mask is contiguous (like 255.255.255.0)
	ones, bits := mask.Size()
	if bits != 32 || ones > 32 {
		return nil
	}
	
	// We must ensure that: <host_ip> & <netmask> == <host_ip>
	// e.g. `127.0.0.1/255.0.0.0` is an illegal string,
	// because `127.0.0.1` & `255.0.0.0` == `127.0.0.0`, but != `127.0.0.1`
	// see https://dev.mysql.com/doc/refman/5.7/en/account-names.html
	if !hostIP.Equal(hostIP.Mask(mask)) {
		return nil
	}
	
	return &net.IPNet{
		IP:   hostIP,
		Mask: mask,
	}
}

// IsValidHostPattern checks if the host pattern is valid for user creation.
// It supports wildcards (%), exact hosts, CIDR notation, and subnet mask notation.
func IsValidHostPattern(host string) bool {
	if host == "%" || host == "" {
		return true
	}
	
	// Check for CIDR or subnet mask notation
	if strings.Contains(host, "/") {
		return parseHostIPNet(host) != nil
	}
	
	// Check for wildcard pattern
	if strings.Contains(host, "%") {
		// Allow patterns like "%.example.com", "192.168.1.%", etc.
		// But don't allow multiple % signs in invalid positions
		parts := strings.Split(host, "%")
		if len(parts) > 2 {
			return false // Multiple % signs
		}
		// Check if the non-% parts are valid
		for _, part := range parts {
			if part != "" && !isHostname(part) {
				return false
			}
		}
		return true
	}
	
	// Check for exact host, localhost, or hostname
	return net.ParseIP(host) != nil || host == "localhost" || isHostname(host)
}

// isHostname performs basic hostname validation
func isHostname(host string) bool {
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	// Basic check for valid hostname characters
	for _, ch := range host {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || 
		     (ch >= '0' && ch <= '9') || ch == '.' || ch == '-') {
			return false
		}
	}
	return true
}
