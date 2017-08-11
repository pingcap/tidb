// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mux

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func (r *Route) GoString() string {
	matchers := make([]string, len(r.matchers))
	for i, m := range r.matchers {
		matchers[i] = fmt.Sprintf("%#v", m)
	}
	return fmt.Sprintf("&Route{matchers:[]matcher{%s}}", strings.Join(matchers, ", "))
}

func (r *routeRegexp) GoString() string {
	return fmt.Sprintf("&routeRegexp{template: %q, matchHost: %t, matchQuery: %t, strictSlash: %t, regexp: regexp.MustCompile(%q), reverse: %q, varsN: %v, varsR: %v", r.template, r.matchHost, r.matchQuery, r.strictSlash, r.regexp.String(), r.reverse, r.varsN, r.varsR)
}

type routeTest struct {
	title          string            // title of the test
	route          *Route            // the route being tested
	request        *http.Request     // a request to test the route
	vars           map[string]string // the expected vars of the match
	host           string            // the expected host of the match
	path           string            // the expected path of the match
	pathTemplate   string            // the expected path template to match
	hostTemplate   string            // the expected host template to match
	shouldMatch    bool              // whether the request is expected to match the route at all
	shouldRedirect bool              // whether the request should result in a redirect
}

func TestHost(t *testing.T) {
	// newRequestHost a new request with a method, url, and host header
	newRequestHost := func(method, url, host string) *http.Request {
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			panic(err)
		}
		req.Host = host
		return req
	}

	tests := []routeTest{
		{
			title:       "Host route match",
			route:       new(Route).Host("aaa.bbb.ccc"),
			request:     newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:        map[string]string{},
			host:        "aaa.bbb.ccc",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Host route, wrong host in request URL",
			route:       new(Route).Host("aaa.bbb.ccc"),
			request:     newRequest("GET", "http://aaa.222.ccc/111/222/333"),
			vars:        map[string]string{},
			host:        "aaa.bbb.ccc",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Host route with port, match",
			route:       new(Route).Host("aaa.bbb.ccc:1234"),
			request:     newRequest("GET", "http://aaa.bbb.ccc:1234/111/222/333"),
			vars:        map[string]string{},
			host:        "aaa.bbb.ccc:1234",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Host route with port, wrong port in request URL",
			route:       new(Route).Host("aaa.bbb.ccc:1234"),
			request:     newRequest("GET", "http://aaa.bbb.ccc:9999/111/222/333"),
			vars:        map[string]string{},
			host:        "aaa.bbb.ccc:1234",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Host route, match with host in request header",
			route:       new(Route).Host("aaa.bbb.ccc"),
			request:     newRequestHost("GET", "/111/222/333", "aaa.bbb.ccc"),
			vars:        map[string]string{},
			host:        "aaa.bbb.ccc",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Host route, wrong host in request header",
			route:       new(Route).Host("aaa.bbb.ccc"),
			request:     newRequestHost("GET", "/111/222/333", "aaa.222.ccc"),
			vars:        map[string]string{},
			host:        "aaa.bbb.ccc",
			path:        "",
			shouldMatch: false,
		},
		// BUG {new(Route).Host("aaa.bbb.ccc:1234"), newRequestHost("GET", "/111/222/333", "aaa.bbb.ccc:1234"), map[string]string{}, "aaa.bbb.ccc:1234", "", true},
		{
			title:       "Host route with port, wrong host in request header",
			route:       new(Route).Host("aaa.bbb.ccc:1234"),
			request:     newRequestHost("GET", "/111/222/333", "aaa.bbb.ccc:9999"),
			vars:        map[string]string{},
			host:        "aaa.bbb.ccc:1234",
			path:        "",
			shouldMatch: false,
		},
		{
			title:        "Host route with pattern, match",
			route:        new(Route).Host("aaa.{v1:[a-z]{3}}.ccc"),
			request:      newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:         map[string]string{"v1": "bbb"},
			host:         "aaa.bbb.ccc",
			path:         "",
			hostTemplate: `aaa.{v1:[a-z]{3}}.ccc`,
			shouldMatch:  true,
		},
		{
			title:        "Host route with pattern, additional capturing group, match",
			route:        new(Route).Host("aaa.{v1:[a-z]{2}(?:b|c)}.ccc"),
			request:      newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:         map[string]string{"v1": "bbb"},
			host:         "aaa.bbb.ccc",
			path:         "",
			hostTemplate: `aaa.{v1:[a-z]{2}(?:b|c)}.ccc`,
			shouldMatch:  true,
		},
		{
			title:        "Host route with pattern, wrong host in request URL",
			route:        new(Route).Host("aaa.{v1:[a-z]{3}}.ccc"),
			request:      newRequest("GET", "http://aaa.222.ccc/111/222/333"),
			vars:         map[string]string{"v1": "bbb"},
			host:         "aaa.bbb.ccc",
			path:         "",
			hostTemplate: `aaa.{v1:[a-z]{3}}.ccc`,
			shouldMatch:  false,
		},
		{
			title:        "Host route with multiple patterns, match",
			route:        new(Route).Host("{v1:[a-z]{3}}.{v2:[a-z]{3}}.{v3:[a-z]{3}}"),
			request:      newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:         map[string]string{"v1": "aaa", "v2": "bbb", "v3": "ccc"},
			host:         "aaa.bbb.ccc",
			path:         "",
			hostTemplate: `{v1:[a-z]{3}}.{v2:[a-z]{3}}.{v3:[a-z]{3}}`,
			shouldMatch:  true,
		},
		{
			title:        "Host route with multiple patterns, wrong host in request URL",
			route:        new(Route).Host("{v1:[a-z]{3}}.{v2:[a-z]{3}}.{v3:[a-z]{3}}"),
			request:      newRequest("GET", "http://aaa.222.ccc/111/222/333"),
			vars:         map[string]string{"v1": "aaa", "v2": "bbb", "v3": "ccc"},
			host:         "aaa.bbb.ccc",
			path:         "",
			hostTemplate: `{v1:[a-z]{3}}.{v2:[a-z]{3}}.{v3:[a-z]{3}}`,
			shouldMatch:  false,
		},
		{
			title:        "Host route with hyphenated name and pattern, match",
			route:        new(Route).Host("aaa.{v-1:[a-z]{3}}.ccc"),
			request:      newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:         map[string]string{"v-1": "bbb"},
			host:         "aaa.bbb.ccc",
			path:         "",
			hostTemplate: `aaa.{v-1:[a-z]{3}}.ccc`,
			shouldMatch:  true,
		},
		{
			title:        "Host route with hyphenated name and pattern, additional capturing group, match",
			route:        new(Route).Host("aaa.{v-1:[a-z]{2}(?:b|c)}.ccc"),
			request:      newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:         map[string]string{"v-1": "bbb"},
			host:         "aaa.bbb.ccc",
			path:         "",
			hostTemplate: `aaa.{v-1:[a-z]{2}(?:b|c)}.ccc`,
			shouldMatch:  true,
		},
		{
			title:        "Host route with multiple hyphenated names and patterns, match",
			route:        new(Route).Host("{v-1:[a-z]{3}}.{v-2:[a-z]{3}}.{v-3:[a-z]{3}}"),
			request:      newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:         map[string]string{"v-1": "aaa", "v-2": "bbb", "v-3": "ccc"},
			host:         "aaa.bbb.ccc",
			path:         "",
			hostTemplate: `{v-1:[a-z]{3}}.{v-2:[a-z]{3}}.{v-3:[a-z]{3}}`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with single pattern with pipe, match",
			route:        new(Route).Path("/{category:a|b/c}"),
			request:      newRequest("GET", "http://localhost/a"),
			vars:         map[string]string{"category": "a"},
			host:         "",
			path:         "/a",
			pathTemplate: `/{category:a|b/c}`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with single pattern with pipe, match",
			route:        new(Route).Path("/{category:a|b/c}"),
			request:      newRequest("GET", "http://localhost/b/c"),
			vars:         map[string]string{"category": "b/c"},
			host:         "",
			path:         "/b/c",
			pathTemplate: `/{category:a|b/c}`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with multiple patterns with pipe, match",
			route:        new(Route).Path("/{category:a|b/c}/{product}/{id:[0-9]+}"),
			request:      newRequest("GET", "http://localhost/a/product_name/1"),
			vars:         map[string]string{"category": "a", "product": "product_name", "id": "1"},
			host:         "",
			path:         "/a/product_name/1",
			pathTemplate: `/{category:a|b/c}/{product}/{id:[0-9]+}`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with multiple patterns with pipe, match",
			route:        new(Route).Path("/{category:a|b/c}/{product}/{id:[0-9]+}"),
			request:      newRequest("GET", "http://localhost/b/c/product_name/1"),
			vars:         map[string]string{"category": "b/c", "product": "product_name", "id": "1"},
			host:         "",
			path:         "/b/c/product_name/1",
			pathTemplate: `/{category:a|b/c}/{product}/{id:[0-9]+}`,
			shouldMatch:  true,
		},
	}
	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
	}
}

func TestPath(t *testing.T) {
	tests := []routeTest{
		{
			title:       "Path route, match",
			route:       new(Route).Path("/111/222/333"),
			request:     newRequest("GET", "http://localhost/111/222/333"),
			vars:        map[string]string{},
			host:        "",
			path:        "/111/222/333",
			shouldMatch: true,
		},
		{
			title:       "Path route, match with trailing slash in request and path",
			route:       new(Route).Path("/111/"),
			request:     newRequest("GET", "http://localhost/111/"),
			vars:        map[string]string{},
			host:        "",
			path:        "/111/",
			shouldMatch: true,
		},
		{
			title:        "Path route, do not match with trailing slash in path",
			route:        new(Route).Path("/111/"),
			request:      newRequest("GET", "http://localhost/111"),
			vars:         map[string]string{},
			host:         "",
			path:         "/111",
			pathTemplate: `/111/`,
			shouldMatch:  false,
		},
		{
			title:        "Path route, do not match with trailing slash in request",
			route:        new(Route).Path("/111"),
			request:      newRequest("GET", "http://localhost/111/"),
			vars:         map[string]string{},
			host:         "",
			path:         "/111/",
			pathTemplate: `/111`,
			shouldMatch:  false,
		},
		{
			title:        "Path route, match root with no host",
			route:        new(Route).Path("/"),
			request:      newRequest("GET", "/"),
			vars:         map[string]string{},
			host:         "",
			path:         "/",
			pathTemplate: `/`,
			shouldMatch:  true,
		},
		{
			title: "Path route, match root with no host, App Engine format",
			route: new(Route).Path("/"),
			request: func() *http.Request {
				r := newRequest("GET", "http://localhost/")
				r.RequestURI = "/"
				return r
			}(),
			vars:         map[string]string{},
			host:         "",
			path:         "/",
			pathTemplate: `/`,
			shouldMatch:  true,
		},
		{
			title:       "Path route, wrong path in request in request URL",
			route:       new(Route).Path("/111/222/333"),
			request:     newRequest("GET", "http://localhost/1/2/3"),
			vars:        map[string]string{},
			host:        "",
			path:        "/111/222/333",
			shouldMatch: false,
		},
		{
			title:        "Path route with pattern, match",
			route:        new(Route).Path("/111/{v1:[0-9]{3}}/333"),
			request:      newRequest("GET", "http://localhost/111/222/333"),
			vars:         map[string]string{"v1": "222"},
			host:         "",
			path:         "/111/222/333",
			pathTemplate: `/111/{v1:[0-9]{3}}/333`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with pattern, URL in request does not match",
			route:        new(Route).Path("/111/{v1:[0-9]{3}}/333"),
			request:      newRequest("GET", "http://localhost/111/aaa/333"),
			vars:         map[string]string{"v1": "222"},
			host:         "",
			path:         "/111/222/333",
			pathTemplate: `/111/{v1:[0-9]{3}}/333`,
			shouldMatch:  false,
		},
		{
			title:        "Path route with multiple patterns, match",
			route:        new(Route).Path("/{v1:[0-9]{3}}/{v2:[0-9]{3}}/{v3:[0-9]{3}}"),
			request:      newRequest("GET", "http://localhost/111/222/333"),
			vars:         map[string]string{"v1": "111", "v2": "222", "v3": "333"},
			host:         "",
			path:         "/111/222/333",
			pathTemplate: `/{v1:[0-9]{3}}/{v2:[0-9]{3}}/{v3:[0-9]{3}}`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with multiple patterns, URL in request does not match",
			route:        new(Route).Path("/{v1:[0-9]{3}}/{v2:[0-9]{3}}/{v3:[0-9]{3}}"),
			request:      newRequest("GET", "http://localhost/111/aaa/333"),
			vars:         map[string]string{"v1": "111", "v2": "222", "v3": "333"},
			host:         "",
			path:         "/111/222/333",
			pathTemplate: `/{v1:[0-9]{3}}/{v2:[0-9]{3}}/{v3:[0-9]{3}}`,
			shouldMatch:  false,
		},
		{
			title:        "Path route with multiple patterns with pipe, match",
			route:        new(Route).Path("/{category:a|(?:b/c)}/{product}/{id:[0-9]+}"),
			request:      newRequest("GET", "http://localhost/a/product_name/1"),
			vars:         map[string]string{"category": "a", "product": "product_name", "id": "1"},
			host:         "",
			path:         "/a/product_name/1",
			pathTemplate: `/{category:a|(?:b/c)}/{product}/{id:[0-9]+}`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with hyphenated name and pattern, match",
			route:        new(Route).Path("/111/{v-1:[0-9]{3}}/333"),
			request:      newRequest("GET", "http://localhost/111/222/333"),
			vars:         map[string]string{"v-1": "222"},
			host:         "",
			path:         "/111/222/333",
			pathTemplate: `/111/{v-1:[0-9]{3}}/333`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with multiple hyphenated names and patterns, match",
			route:        new(Route).Path("/{v-1:[0-9]{3}}/{v-2:[0-9]{3}}/{v-3:[0-9]{3}}"),
			request:      newRequest("GET", "http://localhost/111/222/333"),
			vars:         map[string]string{"v-1": "111", "v-2": "222", "v-3": "333"},
			host:         "",
			path:         "/111/222/333",
			pathTemplate: `/{v-1:[0-9]{3}}/{v-2:[0-9]{3}}/{v-3:[0-9]{3}}`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with multiple hyphenated names and patterns with pipe, match",
			route:        new(Route).Path("/{product-category:a|(?:b/c)}/{product-name}/{product-id:[0-9]+}"),
			request:      newRequest("GET", "http://localhost/a/product_name/1"),
			vars:         map[string]string{"product-category": "a", "product-name": "product_name", "product-id": "1"},
			host:         "",
			path:         "/a/product_name/1",
			pathTemplate: `/{product-category:a|(?:b/c)}/{product-name}/{product-id:[0-9]+}`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with multiple hyphenated names and patterns with pipe and case insensitive, match",
			route:        new(Route).Path("/{type:(?i:daily|mini|variety)}-{date:\\d{4,4}-\\d{2,2}-\\d{2,2}}"),
			request:      newRequest("GET", "http://localhost/daily-2016-01-01"),
			vars:         map[string]string{"type": "daily", "date": "2016-01-01"},
			host:         "",
			path:         "/daily-2016-01-01",
			pathTemplate: `/{type:(?i:daily|mini|variety)}-{date:\d{4,4}-\d{2,2}-\d{2,2}}`,
			shouldMatch:  true,
		},
		{
			title:        "Path route with empty match right after other match",
			route:        new(Route).Path(`/{v1:[0-9]*}{v2:[a-z]*}/{v3:[0-9]*}`),
			request:      newRequest("GET", "http://localhost/111/222"),
			vars:         map[string]string{"v1": "111", "v2": "", "v3": "222"},
			host:         "",
			path:         "/111/222",
			pathTemplate: `/{v1:[0-9]*}{v2:[a-z]*}/{v3:[0-9]*}`,
			shouldMatch:  true,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
		testUseEscapedRoute(t, test)
	}
}

func TestPathPrefix(t *testing.T) {
	tests := []routeTest{
		{
			title:       "PathPrefix route, match",
			route:       new(Route).PathPrefix("/111"),
			request:     newRequest("GET", "http://localhost/111/222/333"),
			vars:        map[string]string{},
			host:        "",
			path:        "/111",
			shouldMatch: true,
		},
		{
			title:       "PathPrefix route, match substring",
			route:       new(Route).PathPrefix("/1"),
			request:     newRequest("GET", "http://localhost/111/222/333"),
			vars:        map[string]string{},
			host:        "",
			path:        "/1",
			shouldMatch: true,
		},
		{
			title:       "PathPrefix route, URL prefix in request does not match",
			route:       new(Route).PathPrefix("/111"),
			request:     newRequest("GET", "http://localhost/1/2/3"),
			vars:        map[string]string{},
			host:        "",
			path:        "/111",
			shouldMatch: false,
		},
		{
			title:        "PathPrefix route with pattern, match",
			route:        new(Route).PathPrefix("/111/{v1:[0-9]{3}}"),
			request:      newRequest("GET", "http://localhost/111/222/333"),
			vars:         map[string]string{"v1": "222"},
			host:         "",
			path:         "/111/222",
			pathTemplate: `/111/{v1:[0-9]{3}}`,
			shouldMatch:  true,
		},
		{
			title:        "PathPrefix route with pattern, URL prefix in request does not match",
			route:        new(Route).PathPrefix("/111/{v1:[0-9]{3}}"),
			request:      newRequest("GET", "http://localhost/111/aaa/333"),
			vars:         map[string]string{"v1": "222"},
			host:         "",
			path:         "/111/222",
			pathTemplate: `/111/{v1:[0-9]{3}}`,
			shouldMatch:  false,
		},
		{
			title:        "PathPrefix route with multiple patterns, match",
			route:        new(Route).PathPrefix("/{v1:[0-9]{3}}/{v2:[0-9]{3}}"),
			request:      newRequest("GET", "http://localhost/111/222/333"),
			vars:         map[string]string{"v1": "111", "v2": "222"},
			host:         "",
			path:         "/111/222",
			pathTemplate: `/{v1:[0-9]{3}}/{v2:[0-9]{3}}`,
			shouldMatch:  true,
		},
		{
			title:        "PathPrefix route with multiple patterns, URL prefix in request does not match",
			route:        new(Route).PathPrefix("/{v1:[0-9]{3}}/{v2:[0-9]{3}}"),
			request:      newRequest("GET", "http://localhost/111/aaa/333"),
			vars:         map[string]string{"v1": "111", "v2": "222"},
			host:         "",
			path:         "/111/222",
			pathTemplate: `/{v1:[0-9]{3}}/{v2:[0-9]{3}}`,
			shouldMatch:  false,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
		testUseEscapedRoute(t, test)
	}
}

func TestHostPath(t *testing.T) {
	tests := []routeTest{
		{
			title:        "Host and Path route, match",
			route:        new(Route).Host("aaa.bbb.ccc").Path("/111/222/333"),
			request:      newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:         map[string]string{},
			host:         "",
			path:         "",
			pathTemplate: `/111/222/333`,
			hostTemplate: `aaa.bbb.ccc`,
			shouldMatch:  true,
		},
		{
			title:        "Host and Path route, wrong host in request URL",
			route:        new(Route).Host("aaa.bbb.ccc").Path("/111/222/333"),
			request:      newRequest("GET", "http://aaa.222.ccc/111/222/333"),
			vars:         map[string]string{},
			host:         "",
			path:         "",
			pathTemplate: `/111/222/333`,
			hostTemplate: `aaa.bbb.ccc`,
			shouldMatch:  false,
		},
		{
			title:        "Host and Path route with pattern, match",
			route:        new(Route).Host("aaa.{v1:[a-z]{3}}.ccc").Path("/111/{v2:[0-9]{3}}/333"),
			request:      newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:         map[string]string{"v1": "bbb", "v2": "222"},
			host:         "aaa.bbb.ccc",
			path:         "/111/222/333",
			pathTemplate: `/111/{v2:[0-9]{3}}/333`,
			hostTemplate: `aaa.{v1:[a-z]{3}}.ccc`,
			shouldMatch:  true,
		},
		{
			title:        "Host and Path route with pattern, URL in request does not match",
			route:        new(Route).Host("aaa.{v1:[a-z]{3}}.ccc").Path("/111/{v2:[0-9]{3}}/333"),
			request:      newRequest("GET", "http://aaa.222.ccc/111/222/333"),
			vars:         map[string]string{"v1": "bbb", "v2": "222"},
			host:         "aaa.bbb.ccc",
			path:         "/111/222/333",
			pathTemplate: `/111/{v2:[0-9]{3}}/333`,
			hostTemplate: `aaa.{v1:[a-z]{3}}.ccc`,
			shouldMatch:  false,
		},
		{
			title:        "Host and Path route with multiple patterns, match",
			route:        new(Route).Host("{v1:[a-z]{3}}.{v2:[a-z]{3}}.{v3:[a-z]{3}}").Path("/{v4:[0-9]{3}}/{v5:[0-9]{3}}/{v6:[0-9]{3}}"),
			request:      newRequest("GET", "http://aaa.bbb.ccc/111/222/333"),
			vars:         map[string]string{"v1": "aaa", "v2": "bbb", "v3": "ccc", "v4": "111", "v5": "222", "v6": "333"},
			host:         "aaa.bbb.ccc",
			path:         "/111/222/333",
			pathTemplate: `/{v4:[0-9]{3}}/{v5:[0-9]{3}}/{v6:[0-9]{3}}`,
			hostTemplate: `{v1:[a-z]{3}}.{v2:[a-z]{3}}.{v3:[a-z]{3}}`,
			shouldMatch:  true,
		},
		{
			title:        "Host and Path route with multiple patterns, URL in request does not match",
			route:        new(Route).Host("{v1:[a-z]{3}}.{v2:[a-z]{3}}.{v3:[a-z]{3}}").Path("/{v4:[0-9]{3}}/{v5:[0-9]{3}}/{v6:[0-9]{3}}"),
			request:      newRequest("GET", "http://aaa.222.ccc/111/222/333"),
			vars:         map[string]string{"v1": "aaa", "v2": "bbb", "v3": "ccc", "v4": "111", "v5": "222", "v6": "333"},
			host:         "aaa.bbb.ccc",
			path:         "/111/222/333",
			pathTemplate: `/{v4:[0-9]{3}}/{v5:[0-9]{3}}/{v6:[0-9]{3}}`,
			hostTemplate: `{v1:[a-z]{3}}.{v2:[a-z]{3}}.{v3:[a-z]{3}}`,
			shouldMatch:  false,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
		testUseEscapedRoute(t, test)
	}
}

func TestHeaders(t *testing.T) {
	// newRequestHeaders creates a new request with a method, url, and headers
	newRequestHeaders := func(method, url string, headers map[string]string) *http.Request {
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			panic(err)
		}
		for k, v := range headers {
			req.Header.Add(k, v)
		}
		return req
	}

	tests := []routeTest{
		{
			title:       "Headers route, match",
			route:       new(Route).Headers("foo", "bar", "baz", "ding"),
			request:     newRequestHeaders("GET", "http://localhost", map[string]string{"foo": "bar", "baz": "ding"}),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Headers route, bad header values",
			route:       new(Route).Headers("foo", "bar", "baz", "ding"),
			request:     newRequestHeaders("GET", "http://localhost", map[string]string{"foo": "bar", "baz": "dong"}),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Headers route, regex header values to match",
			route:       new(Route).Headers("foo", "ba[zr]"),
			request:     newRequestHeaders("GET", "http://localhost", map[string]string{"foo": "bar"}),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Headers route, regex header values to match",
			route:       new(Route).HeadersRegexp("foo", "ba[zr]"),
			request:     newRequestHeaders("GET", "http://localhost", map[string]string{"foo": "baz"}),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
	}

}

func TestMethods(t *testing.T) {
	tests := []routeTest{
		{
			title:       "Methods route, match GET",
			route:       new(Route).Methods("GET", "POST"),
			request:     newRequest("GET", "http://localhost"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Methods route, match POST",
			route:       new(Route).Methods("GET", "POST"),
			request:     newRequest("POST", "http://localhost"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Methods route, bad method",
			route:       new(Route).Methods("GET", "POST"),
			request:     newRequest("PUT", "http://localhost"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
	}
}

func TestQueries(t *testing.T) {
	tests := []routeTest{
		{
			title:       "Queries route, match",
			route:       new(Route).Queries("foo", "bar", "baz", "ding"),
			request:     newRequest("GET", "http://localhost?foo=bar&baz=ding"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:        "Queries route, match with a query string",
			route:        new(Route).Host("www.example.com").Path("/api").Queries("foo", "bar", "baz", "ding"),
			request:      newRequest("GET", "http://www.example.com/api?foo=bar&baz=ding"),
			vars:         map[string]string{},
			host:         "",
			path:         "",
			pathTemplate: `/api`,
			hostTemplate: `www.example.com`,
			shouldMatch:  true,
		},
		{
			title:        "Queries route, match with a query string out of order",
			route:        new(Route).Host("www.example.com").Path("/api").Queries("foo", "bar", "baz", "ding"),
			request:      newRequest("GET", "http://www.example.com/api?baz=ding&foo=bar"),
			vars:         map[string]string{},
			host:         "",
			path:         "",
			pathTemplate: `/api`,
			hostTemplate: `www.example.com`,
			shouldMatch:  true,
		},
		{
			title:       "Queries route, bad query",
			route:       new(Route).Queries("foo", "bar", "baz", "ding"),
			request:     newRequest("GET", "http://localhost?foo=bar&baz=dong"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Queries route with pattern, match",
			route:       new(Route).Queries("foo", "{v1}"),
			request:     newRequest("GET", "http://localhost?foo=bar"),
			vars:        map[string]string{"v1": "bar"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with multiple patterns, match",
			route:       new(Route).Queries("foo", "{v1}", "baz", "{v2}"),
			request:     newRequest("GET", "http://localhost?foo=bar&baz=ding"),
			vars:        map[string]string{"v1": "bar", "v2": "ding"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with regexp pattern, match",
			route:       new(Route).Queries("foo", "{v1:[0-9]+}"),
			request:     newRequest("GET", "http://localhost?foo=10"),
			vars:        map[string]string{"v1": "10"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with regexp pattern, regexp does not match",
			route:       new(Route).Queries("foo", "{v1:[0-9]+}"),
			request:     newRequest("GET", "http://localhost?foo=a"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Queries route with regexp pattern with quantifier, match",
			route:       new(Route).Queries("foo", "{v1:[0-9]{1}}"),
			request:     newRequest("GET", "http://localhost?foo=1"),
			vars:        map[string]string{"v1": "1"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with regexp pattern with quantifier, additional variable in query string, match",
			route:       new(Route).Queries("foo", "{v1:[0-9]{1}}"),
			request:     newRequest("GET", "http://localhost?bar=2&foo=1"),
			vars:        map[string]string{"v1": "1"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with regexp pattern with quantifier, regexp does not match",
			route:       new(Route).Queries("foo", "{v1:[0-9]{1}}"),
			request:     newRequest("GET", "http://localhost?foo=12"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Queries route with regexp pattern with quantifier, additional capturing group",
			route:       new(Route).Queries("foo", "{v1:[0-9]{1}(?:a|b)}"),
			request:     newRequest("GET", "http://localhost?foo=1a"),
			vars:        map[string]string{"v1": "1a"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with regexp pattern with quantifier, additional variable in query string, regexp does not match",
			route:       new(Route).Queries("foo", "{v1:[0-9]{1}}"),
			request:     newRequest("GET", "http://localhost?foo=12"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Queries route with hyphenated name, match",
			route:       new(Route).Queries("foo", "{v-1}"),
			request:     newRequest("GET", "http://localhost?foo=bar"),
			vars:        map[string]string{"v-1": "bar"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with multiple hyphenated names, match",
			route:       new(Route).Queries("foo", "{v-1}", "baz", "{v-2}"),
			request:     newRequest("GET", "http://localhost?foo=bar&baz=ding"),
			vars:        map[string]string{"v-1": "bar", "v-2": "ding"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with hyphenate name and pattern, match",
			route:       new(Route).Queries("foo", "{v-1:[0-9]+}"),
			request:     newRequest("GET", "http://localhost?foo=10"),
			vars:        map[string]string{"v-1": "10"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with hyphenated name and pattern with quantifier, additional capturing group",
			route:       new(Route).Queries("foo", "{v-1:[0-9]{1}(?:a|b)}"),
			request:     newRequest("GET", "http://localhost?foo=1a"),
			vars:        map[string]string{"v-1": "1a"},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with empty value, should match",
			route:       new(Route).Queries("foo", ""),
			request:     newRequest("GET", "http://localhost?foo=bar"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with empty value and no parameter in request, should not match",
			route:       new(Route).Queries("foo", ""),
			request:     newRequest("GET", "http://localhost"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Queries route with empty value and empty parameter in request, should match",
			route:       new(Route).Queries("foo", ""),
			request:     newRequest("GET", "http://localhost?foo="),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route with overlapping value, should not match",
			route:       new(Route).Queries("foo", "bar"),
			request:     newRequest("GET", "http://localhost?foo=barfoo"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Queries route with no parameter in request, should not match",
			route:       new(Route).Queries("foo", "{bar}"),
			request:     newRequest("GET", "http://localhost"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
		{
			title:       "Queries route with empty parameter in request, should match",
			route:       new(Route).Queries("foo", "{bar}"),
			request:     newRequest("GET", "http://localhost?foo="),
			vars:        map[string]string{"foo": ""},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Queries route, bad submatch",
			route:       new(Route).Queries("foo", "bar", "baz", "ding"),
			request:     newRequest("GET", "http://localhost?fffoo=bar&baz=dingggg"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
		testUseEscapedRoute(t, test)
	}
}

func TestSchemes(t *testing.T) {
	tests := []routeTest{
		// Schemes
		{
			title:       "Schemes route, match https",
			route:       new(Route).Schemes("https", "ftp"),
			request:     newRequest("GET", "https://localhost"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Schemes route, match ftp",
			route:       new(Route).Schemes("https", "ftp"),
			request:     newRequest("GET", "ftp://localhost"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "Schemes route, bad scheme",
			route:       new(Route).Schemes("https", "ftp"),
			request:     newRequest("GET", "http://localhost"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
	}
	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
	}
}

func TestMatcherFunc(t *testing.T) {
	m := func(r *http.Request, m *RouteMatch) bool {
		if r.URL.Host == "aaa.bbb.ccc" {
			return true
		}
		return false
	}

	tests := []routeTest{
		{
			title:       "MatchFunc route, match",
			route:       new(Route).MatcherFunc(m),
			request:     newRequest("GET", "http://aaa.bbb.ccc"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: true,
		},
		{
			title:       "MatchFunc route, non-match",
			route:       new(Route).MatcherFunc(m),
			request:     newRequest("GET", "http://aaa.222.ccc"),
			vars:        map[string]string{},
			host:        "",
			path:        "",
			shouldMatch: false,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
	}
}

func TestBuildVarsFunc(t *testing.T) {
	tests := []routeTest{
		{
			title: "BuildVarsFunc set on route",
			route: new(Route).Path(`/111/{v1:\d}{v2:.*}`).BuildVarsFunc(func(vars map[string]string) map[string]string {
				vars["v1"] = "3"
				vars["v2"] = "a"
				return vars
			}),
			request:      newRequest("GET", "http://localhost/111/2"),
			path:         "/111/3a",
			pathTemplate: `/111/{v1:\d}{v2:.*}`,
			shouldMatch:  true,
		},
		{
			title: "BuildVarsFunc set on route and parent route",
			route: new(Route).PathPrefix(`/{v1:\d}`).BuildVarsFunc(func(vars map[string]string) map[string]string {
				vars["v1"] = "2"
				return vars
			}).Subrouter().Path(`/{v2:\w}`).BuildVarsFunc(func(vars map[string]string) map[string]string {
				vars["v2"] = "b"
				return vars
			}),
			request:      newRequest("GET", "http://localhost/1/a"),
			path:         "/2/b",
			pathTemplate: `/{v1:\d}/{v2:\w}`,
			shouldMatch:  true,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
	}
}

func TestSubRouter(t *testing.T) {
	subrouter1 := new(Route).Host("{v1:[a-z]+}.google.com").Subrouter()
	subrouter2 := new(Route).PathPrefix("/foo/{v1}").Subrouter()
	subrouter3 := new(Route).PathPrefix("/foo").Subrouter()
	subrouter4 := new(Route).PathPrefix("/foo/bar").Subrouter()
	subrouter5 := new(Route).PathPrefix("/{category}").Subrouter()

	tests := []routeTest{
		{
			route:        subrouter1.Path("/{v2:[a-z]+}"),
			request:      newRequest("GET", "http://aaa.google.com/bbb"),
			vars:         map[string]string{"v1": "aaa", "v2": "bbb"},
			host:         "aaa.google.com",
			path:         "/bbb",
			pathTemplate: `/{v2:[a-z]+}`,
			hostTemplate: `{v1:[a-z]+}.google.com`,
			shouldMatch:  true,
		},
		{
			route:        subrouter1.Path("/{v2:[a-z]+}"),
			request:      newRequest("GET", "http://111.google.com/111"),
			vars:         map[string]string{"v1": "aaa", "v2": "bbb"},
			host:         "aaa.google.com",
			path:         "/bbb",
			pathTemplate: `/{v2:[a-z]+}`,
			hostTemplate: `{v1:[a-z]+}.google.com`,
			shouldMatch:  false,
		},
		{
			route:        subrouter2.Path("/baz/{v2}"),
			request:      newRequest("GET", "http://localhost/foo/bar/baz/ding"),
			vars:         map[string]string{"v1": "bar", "v2": "ding"},
			host:         "",
			path:         "/foo/bar/baz/ding",
			pathTemplate: `/foo/{v1}/baz/{v2}`,
			shouldMatch:  true,
		},
		{
			route:        subrouter2.Path("/baz/{v2}"),
			request:      newRequest("GET", "http://localhost/foo/bar"),
			vars:         map[string]string{"v1": "bar", "v2": "ding"},
			host:         "",
			path:         "/foo/bar/baz/ding",
			pathTemplate: `/foo/{v1}/baz/{v2}`,
			shouldMatch:  false,
		},
		{
			route:        subrouter3.Path("/"),
			request:      newRequest("GET", "http://localhost/foo/"),
			vars:         map[string]string{},
			host:         "",
			path:         "/foo/",
			pathTemplate: `/foo/`,
			shouldMatch:  true,
		},
		{
			route:        subrouter3.Path(""),
			request:      newRequest("GET", "http://localhost/foo"),
			vars:         map[string]string{},
			host:         "",
			path:         "/foo",
			pathTemplate: `/foo`,
			shouldMatch:  true,
		},

		{
			route:        subrouter4.Path("/"),
			request:      newRequest("GET", "http://localhost/foo/bar/"),
			vars:         map[string]string{},
			host:         "",
			path:         "/foo/bar/",
			pathTemplate: `/foo/bar/`,
			shouldMatch:  true,
		},
		{
			route:        subrouter4.Path(""),
			request:      newRequest("GET", "http://localhost/foo/bar"),
			vars:         map[string]string{},
			host:         "",
			path:         "/foo/bar",
			pathTemplate: `/foo/bar`,
			shouldMatch:  true,
		},
		{
			route:        subrouter5.Path("/"),
			request:      newRequest("GET", "http://localhost/baz/"),
			vars:         map[string]string{"category": "baz"},
			host:         "",
			path:         "/baz/",
			pathTemplate: `/{category}/`,
			shouldMatch:  true,
		},
		{
			route:        subrouter5.Path(""),
			request:      newRequest("GET", "http://localhost/baz"),
			vars:         map[string]string{"category": "baz"},
			host:         "",
			path:         "/baz",
			pathTemplate: `/{category}`,
			shouldMatch:  true,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
		testUseEscapedRoute(t, test)
	}
}

func TestNamedRoutes(t *testing.T) {
	r1 := NewRouter()
	r1.NewRoute().Name("a")
	r1.NewRoute().Name("b")
	r1.NewRoute().Name("c")

	r2 := r1.NewRoute().Subrouter()
	r2.NewRoute().Name("d")
	r2.NewRoute().Name("e")
	r2.NewRoute().Name("f")

	r3 := r2.NewRoute().Subrouter()
	r3.NewRoute().Name("g")
	r3.NewRoute().Name("h")
	r3.NewRoute().Name("i")

	if r1.namedRoutes == nil || len(r1.namedRoutes) != 9 {
		t.Errorf("Expected 9 named routes, got %v", r1.namedRoutes)
	} else if r1.Get("i") == nil {
		t.Errorf("Subroute name not registered")
	}
}

func TestStrictSlash(t *testing.T) {
	r := NewRouter()
	r.StrictSlash(true)

	tests := []routeTest{
		{
			title:          "Redirect path without slash",
			route:          r.NewRoute().Path("/111/"),
			request:        newRequest("GET", "http://localhost/111"),
			vars:           map[string]string{},
			host:           "",
			path:           "/111/",
			shouldMatch:    true,
			shouldRedirect: true,
		},
		{
			title:          "Do not redirect path with slash",
			route:          r.NewRoute().Path("/111/"),
			request:        newRequest("GET", "http://localhost/111/"),
			vars:           map[string]string{},
			host:           "",
			path:           "/111/",
			shouldMatch:    true,
			shouldRedirect: false,
		},
		{
			title:          "Redirect path with slash",
			route:          r.NewRoute().Path("/111"),
			request:        newRequest("GET", "http://localhost/111/"),
			vars:           map[string]string{},
			host:           "",
			path:           "/111",
			shouldMatch:    true,
			shouldRedirect: true,
		},
		{
			title:          "Do not redirect path without slash",
			route:          r.NewRoute().Path("/111"),
			request:        newRequest("GET", "http://localhost/111"),
			vars:           map[string]string{},
			host:           "",
			path:           "/111",
			shouldMatch:    true,
			shouldRedirect: false,
		},
		{
			title:          "Propagate StrictSlash to subrouters",
			route:          r.NewRoute().PathPrefix("/static/").Subrouter().Path("/images/"),
			request:        newRequest("GET", "http://localhost/static/images"),
			vars:           map[string]string{},
			host:           "",
			path:           "/static/images/",
			shouldMatch:    true,
			shouldRedirect: true,
		},
		{
			title:          "Ignore StrictSlash for path prefix",
			route:          r.NewRoute().PathPrefix("/static/"),
			request:        newRequest("GET", "http://localhost/static/logo.png"),
			vars:           map[string]string{},
			host:           "",
			path:           "/static/",
			shouldMatch:    true,
			shouldRedirect: false,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
		testUseEscapedRoute(t, test)
	}
}

func TestUseEncodedPath(t *testing.T) {
	r := NewRouter()
	r.UseEncodedPath()

	tests := []routeTest{
		{
			title:        "Router with useEncodedPath, URL with encoded slash does match",
			route:        r.NewRoute().Path("/v1/{v1}/v2"),
			request:      newRequest("GET", "http://localhost/v1/1%2F2/v2"),
			vars:         map[string]string{"v1": "1%2F2"},
			host:         "",
			path:         "/v1/1%2F2/v2",
			pathTemplate: `/v1/{v1}/v2`,
			shouldMatch:  true,
		},
		{
			title:        "Router with useEncodedPath, URL with encoded slash doesn't match",
			route:        r.NewRoute().Path("/v1/1/2/v2"),
			request:      newRequest("GET", "http://localhost/v1/1%2F2/v2"),
			vars:         map[string]string{"v1": "1%2F2"},
			host:         "",
			path:         "/v1/1%2F2/v2",
			pathTemplate: `/v1/1/2/v2`,
			shouldMatch:  false,
		},
	}

	for _, test := range tests {
		testRoute(t, test)
		testTemplate(t, test)
	}
}

func TestWalkSingleDepth(t *testing.T) {
	r0 := NewRouter()
	r1 := NewRouter()
	r2 := NewRouter()

	r0.Path("/g")
	r0.Path("/o")
	r0.Path("/d").Handler(r1)
	r0.Path("/r").Handler(r2)
	r0.Path("/a")

	r1.Path("/z")
	r1.Path("/i")
	r1.Path("/l")
	r1.Path("/l")

	r2.Path("/i")
	r2.Path("/l")
	r2.Path("/l")

	paths := []string{"g", "o", "r", "i", "l", "l", "a"}
	depths := []int{0, 0, 0, 1, 1, 1, 0}
	i := 0
	err := r0.Walk(func(route *Route, router *Router, ancestors []*Route) error {
		matcher := route.matchers[0].(*routeRegexp)
		if matcher.template == "/d" {
			return SkipRouter
		}
		if len(ancestors) != depths[i] {
			t.Errorf(`Expected depth of %d at i = %d; got "%d"`, depths[i], i, len(ancestors))
		}
		if matcher.template != "/"+paths[i] {
			t.Errorf(`Expected "/%s" at i = %d; got "%s"`, paths[i], i, matcher.template)
		}
		i++
		return nil
	})
	if err != nil {
		panic(err)
	}
	if i != len(paths) {
		t.Errorf("Expected %d routes, found %d", len(paths), i)
	}
}

func TestWalkNested(t *testing.T) {
	router := NewRouter()

	g := router.Path("/g").Subrouter()
	o := g.PathPrefix("/o").Subrouter()
	r := o.PathPrefix("/r").Subrouter()
	i := r.PathPrefix("/i").Subrouter()
	l1 := i.PathPrefix("/l").Subrouter()
	l2 := l1.PathPrefix("/l").Subrouter()
	l2.Path("/a")

	paths := []string{"/g", "/g/o", "/g/o/r", "/g/o/r/i", "/g/o/r/i/l", "/g/o/r/i/l/l", "/g/o/r/i/l/l/a"}
	idx := 0
	err := router.Walk(func(route *Route, router *Router, ancestors []*Route) error {
		path := paths[idx]
		tpl := route.regexp.path.template
		if tpl != path {
			t.Errorf(`Expected %s got %s`, path, tpl)
		}
		idx++
		return nil
	})
	if err != nil {
		panic(err)
	}
	if idx != len(paths) {
		t.Errorf("Expected %d routes, found %d", len(paths), idx)
	}
}

func TestWalkErrorRoute(t *testing.T) {
	router := NewRouter()
	router.Path("/g")
	expectedError := errors.New("error")
	err := router.Walk(func(route *Route, router *Router, ancestors []*Route) error {
		return expectedError
	})
	if err != expectedError {
		t.Errorf("Expected %v routes, found %v", expectedError, err)
	}
}

func TestWalkErrorMatcher(t *testing.T) {
	router := NewRouter()
	expectedError := router.Path("/g").Subrouter().Path("").GetError()
	err := router.Walk(func(route *Route, router *Router, ancestors []*Route) error {
		return route.GetError()
	})
	if err != expectedError {
		t.Errorf("Expected %v routes, found %v", expectedError, err)
	}
}

func TestWalkErrorHandler(t *testing.T) {
	handler := NewRouter()
	expectedError := handler.Path("/path").Subrouter().Path("").GetError()
	router := NewRouter()
	router.Path("/g").Handler(handler)
	err := router.Walk(func(route *Route, router *Router, ancestors []*Route) error {
		return route.GetError()
	})
	if err != expectedError {
		t.Errorf("Expected %v routes, found %v", expectedError, err)
	}
}

func TestSubrouterErrorHandling(t *testing.T) {
	superRouterCalled := false
	subRouterCalled := false

	router := NewRouter()
	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		superRouterCalled = true
	})
	subRouter := router.PathPrefix("/bign8").Subrouter()
	subRouter.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subRouterCalled = true
	})

	req, _ := http.NewRequest("GET", "http://localhost/bign8/was/here", nil)
	router.ServeHTTP(NewRecorder(), req)

	if superRouterCalled {
		t.Error("Super router 404 handler called when sub-router 404 handler is available.")
	}
	if !subRouterCalled {
		t.Error("Sub-router 404 handler was not called.")
	}
}

// See: https://github.com/gorilla/mux/issues/200
func TestPanicOnCapturingGroups(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("(Test that capturing groups now fail fast) Expected panic, however test completed sucessfully.\n")
		}
	}()
	NewRouter().NewRoute().Path("/{type:(promo|special)}/{promoId}.json")
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

func getRouteTemplate(route *Route) string {
	host, err := route.GetHostTemplate()
	if err != nil {
		host = "none"
	}
	path, err := route.GetPathTemplate()
	if err != nil {
		path = "none"
	}
	return fmt.Sprintf("Host: %v, Path: %v", host, path)
}

func testRoute(t *testing.T, test routeTest) {
	request := test.request
	route := test.route
	vars := test.vars
	shouldMatch := test.shouldMatch
	host := test.host
	path := test.path
	url := test.host + test.path
	shouldRedirect := test.shouldRedirect

	var match RouteMatch
	ok := route.Match(request, &match)
	if ok != shouldMatch {
		msg := "Should match"
		if !shouldMatch {
			msg = "Should not match"
		}
		t.Errorf("(%v) %v:\nRoute: %#v\nRequest: %#v\nVars: %v\n", test.title, msg, route, request, vars)
		return
	}
	if shouldMatch {
		if test.vars != nil && !stringMapEqual(test.vars, match.Vars) {
			t.Errorf("(%v) Vars not equal: expected %v, got %v", test.title, vars, match.Vars)
			return
		}
		if host != "" {
			u, _ := test.route.URLHost(mapToPairs(match.Vars)...)
			if host != u.Host {
				t.Errorf("(%v) URLHost not equal: expected %v, got %v -- %v", test.title, host, u.Host, getRouteTemplate(route))
				return
			}
		}
		if path != "" {
			u, _ := route.URLPath(mapToPairs(match.Vars)...)
			if path != u.Path {
				t.Errorf("(%v) URLPath not equal: expected %v, got %v -- %v", test.title, path, u.Path, getRouteTemplate(route))
				return
			}
		}
		if url != "" {
			u, _ := route.URL(mapToPairs(match.Vars)...)
			if url != u.Host+u.Path {
				t.Errorf("(%v) URL not equal: expected %v, got %v -- %v", test.title, url, u.Host+u.Path, getRouteTemplate(route))
				return
			}
		}
		if shouldRedirect && match.Handler == nil {
			t.Errorf("(%v) Did not redirect", test.title)
			return
		}
		if !shouldRedirect && match.Handler != nil {
			t.Errorf("(%v) Unexpected redirect", test.title)
			return
		}
	}
}

func testUseEscapedRoute(t *testing.T, test routeTest) {
	test.route.useEncodedPath = true
	testRoute(t, test)
}

func testTemplate(t *testing.T, test routeTest) {
	route := test.route
	pathTemplate := test.pathTemplate
	if len(pathTemplate) == 0 {
		pathTemplate = test.path
	}
	hostTemplate := test.hostTemplate
	if len(hostTemplate) == 0 {
		hostTemplate = test.host
	}

	routePathTemplate, pathErr := route.GetPathTemplate()
	if pathErr == nil && routePathTemplate != pathTemplate {
		t.Errorf("(%v) GetPathTemplate not equal: expected %v, got %v", test.title, pathTemplate, routePathTemplate)
	}

	routeHostTemplate, hostErr := route.GetHostTemplate()
	if hostErr == nil && routeHostTemplate != hostTemplate {
		t.Errorf("(%v) GetHostTemplate not equal: expected %v, got %v", test.title, hostTemplate, routeHostTemplate)
	}
}

type TestA301ResponseWriter struct {
	hh     http.Header
	status int
}

func (ho TestA301ResponseWriter) Header() http.Header {
	return http.Header(ho.hh)
}

func (ho TestA301ResponseWriter) Write(b []byte) (int, error) {
	return 0, nil
}

func (ho TestA301ResponseWriter) WriteHeader(code int) {
	ho.status = code
}

func Test301Redirect(t *testing.T) {
	m := make(http.Header)

	func1 := func(w http.ResponseWriter, r *http.Request) {}
	func2 := func(w http.ResponseWriter, r *http.Request) {}

	r := NewRouter()
	r.HandleFunc("/api/", func2).Name("func2")
	r.HandleFunc("/", func1).Name("func1")

	req, _ := http.NewRequest("GET", "http://localhost//api/?abc=def", nil)

	res := TestA301ResponseWriter{
		hh:     m,
		status: 0,
	}
	r.ServeHTTP(&res, req)

	if "http://localhost/api/?abc=def" != res.hh["Location"][0] {
		t.Errorf("Should have complete URL with query string")
	}
}

func TestSkipClean(t *testing.T) {
	func1 := func(w http.ResponseWriter, r *http.Request) {}
	func2 := func(w http.ResponseWriter, r *http.Request) {}

	r := NewRouter()
	r.SkipClean(true)
	r.HandleFunc("/api/", func2).Name("func2")
	r.HandleFunc("/", func1).Name("func1")

	req, _ := http.NewRequest("GET", "http://localhost//api/?abc=def", nil)
	res := NewRecorder()
	r.ServeHTTP(res, req)

	if len(res.HeaderMap["Location"]) != 0 {
		t.Errorf("Shouldn't redirect since skip clean is disabled")
	}
}

// https://plus.google.com/101022900381697718949/posts/eWy6DjFJ6uW
func TestSubrouterHeader(t *testing.T) {
	expected := "func1 response"
	func1 := func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, expected)
	}
	func2 := func(http.ResponseWriter, *http.Request) {}

	r := NewRouter()
	s := r.Headers("SomeSpecialHeader", "").Subrouter()
	s.HandleFunc("/", func1).Name("func1")
	r.HandleFunc("/", func2).Name("func2")

	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	req.Header.Add("SomeSpecialHeader", "foo")
	match := new(RouteMatch)
	matched := r.Match(req, match)
	if !matched {
		t.Errorf("Should match request")
	}
	if match.Route.GetName() != "func1" {
		t.Errorf("Expecting func1 handler, got %s", match.Route.GetName())
	}
	resp := NewRecorder()
	match.Handler.ServeHTTP(resp, req)
	if resp.Body.String() != expected {
		t.Errorf("Expecting %q", expected)
	}
}

// mapToPairs converts a string map to a slice of string pairs
func mapToPairs(m map[string]string) []string {
	var i int
	p := make([]string, len(m)*2)
	for k, v := range m {
		p[i] = k
		p[i+1] = v
		i += 2
	}
	return p
}

// stringMapEqual checks the equality of two string maps
func stringMapEqual(m1, m2 map[string]string) bool {
	nil1 := m1 == nil
	nil2 := m2 == nil
	if nil1 != nil2 || len(m1) != len(m2) {
		return false
	}
	for k, v := range m1 {
		if v != m2[k] {
			return false
		}
	}
	return true
}

// newRequest is a helper function to create a new request with a method and url.
// The request returned is a 'server' request as opposed to a 'client' one through
// simulated write onto the wire and read off of the wire.
// The differences between requests are detailed in the net/http package.
func newRequest(method, url string) *http.Request {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		panic(err)
	}
	// extract the escaped original host+path from url
	// http://localhost/path/here?v=1#frag -> //localhost/path/here
	opaque := ""
	if i := len(req.URL.Scheme); i > 0 {
		opaque = url[i+1:]
	}

	if i := strings.LastIndex(opaque, "?"); i > -1 {
		opaque = opaque[:i]
	}
	if i := strings.LastIndex(opaque, "#"); i > -1 {
		opaque = opaque[:i]
	}

	// Escaped host+path workaround as detailed in https://golang.org/pkg/net/url/#URL
	// for < 1.5 client side workaround
	req.URL.Opaque = opaque

	// Simulate writing to wire
	var buff bytes.Buffer
	req.Write(&buff)
	ioreader := bufio.NewReader(&buff)

	// Parse request off of 'wire'
	req, err = http.ReadRequest(ioreader)
	if err != nil {
		panic(err)
	}
	return req
}
