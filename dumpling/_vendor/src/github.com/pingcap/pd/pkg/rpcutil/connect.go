// Copyright 2016 PingCAP, Inc.
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

package rpcutil

import (
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/juju/errors"
)

const (
	rpcPrefix = "/pd/rpc"
)

// connectURL returns a rpc connection to the url.
func connectURL(u url.URL, timeout time.Duration) (net.Conn, error) {
	req, err := http.NewRequest("GET", rpcPrefix, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var conn net.Conn
	switch u.Scheme {
	case "unix", "unixs":
		conn, err = net.DialTimeout("unix", u.Host, timeout)
	default:
		conn, err = net.DialTimeout("tcp", u.Host, timeout)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = req.Write(conn); err != nil {
		conn.Close()
		return nil, errors.Trace(err)
	}

	return conn, nil
}

// ConnectUrls returns a rpc connection to any one of the urls.
func ConnectUrls(urls string, timeout time.Duration) (net.Conn, error) {
	us, err := ParseUrls(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, u := range us {
		conn, err := connectURL(u, timeout)
		if err == nil {
			return conn, nil
		}
	}
	return nil, errors.Errorf("failed to connect to %s", urls)
}
