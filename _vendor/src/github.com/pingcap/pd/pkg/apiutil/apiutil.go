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

package apiutil

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/juju/errors"
)

// ReadJSON reads a JSON data from r and then close it.
func ReadJSON(r io.ReadCloser, data interface{}) error {
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Trace(err)
	}

	err = json.Unmarshal(b, data)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// NewHTTPClient returns a HTTP client according to the scheme.
func NewHTTPClient(scheme string, timeout time.Duration) *http.Client {
	tr := NewHTTPTransport(scheme)
	return &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}
}

// NewHTTPTransport returns a proper http.RoundTripper.
func NewHTTPTransport(scheme string) *http.Transport {
	tr := &http.Transport{}
	if scheme == "unix" || scheme == "unixs" {
		tr.Dial = unixDial
	}
	return tr
}

func unixDial(_, addr string) (net.Conn, error) {
	return net.Dial("unix", addr)
}
