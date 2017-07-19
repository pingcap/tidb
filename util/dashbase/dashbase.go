// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dashbase

import (
	"regexp"
	"strconv"
)

const (
	// DefaultFirehosePort is the default firehose port
	DefaultFirehosePort int = 7888

	// DefaultProxyPort is the default proxy port
	DefaultProxyPort int = 9876
)

// ConnectionOption holds database connection options.
type ConnectionOption struct {
	FirehoseHostname string `json:"firehose_host"`
	FirehosePort     int    `json:"firehose_port"`
	ProxyHostname    string `json:"proxy_host"`
	ProxyPort        int    `json:"proxy_port"`
}

var connectionOptionRegexp *regexp.Regexp

func init() {
	// m[1] == firehose_hostname
	// m[3] == firehose_port
	// m[5] == proxy_hostname
	// m[7] == proxy_port
	connectionOptionRegexp = regexp.MustCompile(`^([^:;]+)(:(\d+))?(;([^:;]+)(:(\d+))?)?$`)
}

// ParseConnectionOption parses a connection string into ConnectionOption.
func ParseConnectionOption(option string) (*ConnectionOption, bool) {
	var opt ConnectionOption
	matches := connectionOptionRegexp.FindStringSubmatch(option)
	if matches == nil {
		return nil, false
	}
	opt.FirehoseHostname = matches[1]
	if len(matches[3]) > 0 {
		val, err := strconv.Atoi(matches[3])
		if err != nil {
			return nil, false
		}
		opt.FirehosePort = val
	} else {
		opt.FirehosePort = DefaultFirehosePort
	}
	if len(matches[5]) > 0 {
		opt.ProxyHostname = matches[5]
		if len(matches[7]) > 0 {
			val, err := strconv.Atoi(matches[7])
			if err != nil {
				return nil, false
			}
			opt.ProxyPort = val
		} else {
			opt.ProxyPort = DefaultProxyPort
		}
	} else {
		opt.ProxyHostname = opt.FirehoseHostname
		opt.ProxyPort = DefaultProxyPort
	}
	return &opt, true
}
