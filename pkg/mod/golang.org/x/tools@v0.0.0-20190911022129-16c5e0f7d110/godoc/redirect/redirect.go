// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package redirect provides hooks to register HTTP handlers that redirect old
// godoc paths to their new equivalents and assist in accessing the issue
// tracker, wiki, code review system, etc.
package redirect // import "golang.org/x/tools/godoc/redirect"

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context/ctxhttp"
)

// Register registers HTTP handlers that redirect old godoc paths to their new
// equivalents and assist in accessing the issue tracker, wiki, code review
// system, etc. If mux is nil it uses http.DefaultServeMux.
func Register(mux *http.ServeMux) {
	if mux == nil {
		mux = http.DefaultServeMux
	}
	handlePathRedirects(mux, pkgRedirects, "/pkg/")
	handlePathRedirects(mux, cmdRedirects, "/cmd/")
	for prefix, redirect := range prefixHelpers {
		p := "/" + prefix + "/"
		mux.Handle(p, PrefixHandler(p, redirect))
	}
	for path, redirect := range redirects {
		mux.Handle(path, Handler(redirect))
	}
	// NB: /src/pkg (sans trailing slash) is the index of packages.
	mux.HandleFunc("/src/pkg/", srcPkgHandler)
	mux.HandleFunc("/cl/", clHandler)
	mux.HandleFunc("/change/", changeHandler)
	mux.HandleFunc("/design/", designHandler)
}

func handlePathRedirects(mux *http.ServeMux, redirects map[string]string, prefix string) {
	for source, target := range redirects {
		h := Handler(prefix + target + "/")
		p := prefix + source
		mux.Handle(p, h)
		mux.Handle(p+"/", h)
	}
}

// Packages that were renamed between r60 and go1.
var pkgRedirects = map[string]string{
	"asn1":              "encoding/asn1",
	"big":               "math/big",
	"cmath":             "math/cmplx",
	"csv":               "encoding/csv",
	"exec":              "os/exec",
	"exp/template/html": "html/template",
	"gob":               "encoding/gob",
	"http":              "net/http",
	"http/cgi":          "net/http/cgi",
	"http/fcgi":         "net/http/fcgi",
	"http/httptest":     "net/http/httptest",
	"http/pprof":        "net/http/pprof",
	"json":              "encoding/json",
	"mail":              "net/mail",
	"rand":              "math/rand",
	"rpc":               "net/rpc",
	"rpc/jsonrpc":       "net/rpc/jsonrpc",
	"scanner":           "text/scanner",
	"smtp":              "net/smtp",
	"tabwriter":         "text/tabwriter",
	"template":          "text/template",
	"template/parse":    "text/template/parse",
	"url":               "net/url",
	"utf16":             "unicode/utf16",
	"utf8":              "unicode/utf8",
	"xml":               "encoding/xml",
}

// Commands that were renamed between r60 and go1.
var cmdRedirects = map[string]string{
	"gofix":     "fix",
	"goinstall": "go",
	"gopack":    "pack",
	"gotest":    "go",
	"govet":     "vet",
	"goyacc":    "yacc",
}

var redirects = map[string]string{
	"/blog":       "/blog/",
	"/build":      "http://build.golang.org",
	"/change":     "https://go.googlesource.com/go",
	"/cl":         "https://go-review.googlesource.com",
	"/cmd/godoc/": "http://godoc.org/golang.org/x/tools/cmd/godoc/",
	"/issue":      "https://github.com/golang/go/issues",
	"/issue/new":  "https://github.com/golang/go/issues/new",
	"/issues":     "https://github.com/golang/go/issues",
	"/issues/new": "https://github.com/golang/go/issues/new",
	"/play":       "http://play.golang.org",
	"/design":     "https://go.googlesource.com/proposal/+/master/design",

	// In Go 1.2 the references page is part of /doc/.
	"/ref": "/doc/#references",
	// This next rule clobbers /ref/spec and /ref/mem.
	// TODO(adg): figure out what to do here, if anything.
	// "/ref/": "/doc/#references",

	// Be nice to people who are looking in the wrong place.
	"/doc/mem":  "/ref/mem",
	"/doc/spec": "/ref/spec",

	"/talks": "http://talks.golang.org",
	"/tour":  "http://tour.golang.org",
	"/wiki":  "https://github.com/golang/go/wiki",

	"/doc/articles/c_go_cgo.html":                    "/blog/c-go-cgo",
	"/doc/articles/concurrency_patterns.html":        "/blog/go-concurrency-patterns-timing-out-and",
	"/doc/articles/defer_panic_recover.html":         "/blog/defer-panic-and-recover",
	"/doc/articles/error_handling.html":              "/blog/error-handling-and-go",
	"/doc/articles/gobs_of_data.html":                "/blog/gobs-of-data",
	"/doc/articles/godoc_documenting_go_code.html":   "/blog/godoc-documenting-go-code",
	"/doc/articles/gos_declaration_syntax.html":      "/blog/gos-declaration-syntax",
	"/doc/articles/image_draw.html":                  "/blog/go-imagedraw-package",
	"/doc/articles/image_package.html":               "/blog/go-image-package",
	"/doc/articles/json_and_go.html":                 "/blog/json-and-go",
	"/doc/articles/json_rpc_tale_of_interfaces.html": "/blog/json-rpc-tale-of-interfaces",
	"/doc/articles/laws_of_reflection.html":          "/blog/laws-of-reflection",
	"/doc/articles/slices_usage_and_internals.html":  "/blog/go-slices-usage-and-internals",
	"/doc/go_for_cpp_programmers.html":               "/wiki/GoForCPPProgrammers",
	"/doc/go_tutorial.html":                          "http://tour.golang.org/",
}

var prefixHelpers = map[string]string{
	"issue":  "https://github.com/golang/go/issues/",
	"issues": "https://github.com/golang/go/issues/",
	"play":   "http://play.golang.org/",
	"talks":  "http://talks.golang.org/",
	"wiki":   "https://github.com/golang/go/wiki/",
}

func Handler(target string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		url := target
		if qs := r.URL.RawQuery; qs != "" {
			url += "?" + qs
		}
		http.Redirect(w, r, url, http.StatusMovedPermanently)
	})
}

var validId = regexp.MustCompile(`^[A-Za-z0-9-]*/?$`)

func PrefixHandler(prefix, baseURL string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p := r.URL.Path; p == prefix {
			// redirect /prefix/ to /prefix
			http.Redirect(w, r, p[:len(p)-1], http.StatusFound)
			return
		}
		id := r.URL.Path[len(prefix):]
		if !validId.MatchString(id) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		target := baseURL + id
		http.Redirect(w, r, target, http.StatusFound)
	})
}

// Redirect requests from the old "/src/pkg/foo" to the new "/src/foo".
// See http://golang.org/s/go14nopkg
func srcPkgHandler(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/src/" + r.URL.Path[len("/src/pkg/"):]
	http.Redirect(w, r, r.URL.String(), http.StatusMovedPermanently)
}

func clHandler(w http.ResponseWriter, r *http.Request) {
	const prefix = "/cl/"
	if p := r.URL.Path; p == prefix {
		// redirect /prefix/ to /prefix
		http.Redirect(w, r, p[:len(p)-1], http.StatusFound)
		return
	}
	id := r.URL.Path[len(prefix):]
	// support /cl/152700045/, which is used in commit 0edafefc36.
	id = strings.TrimSuffix(id, "/")
	if !validId.MatchString(id) {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	target := ""

	if n, err := strconv.Atoi(id); err == nil && isRietveldCL(n) {
		// Issue 28836: if this Rietveld CL happens to
		// also be a Gerrit CL, render a disambiguation HTML
		// page with two links instead. We need to make a
		// Gerrit API call to figure that out, but we cache
		// known Gerrit CLs so it's done at most once per CL.
		if ok, err := isGerritCL(r.Context(), n); err == nil && ok {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			clDisambiguationHTML.Execute(w, n)
			return
		}

		target = "https://codereview.appspot.com/" + id
	} else {
		target = "https://go-review.googlesource.com/" + id
	}
	http.Redirect(w, r, target, http.StatusFound)
}

var clDisambiguationHTML = template.Must(template.New("").Parse(`<!DOCTYPE html>
<html lang="en">
	<head>
		<title>Go CL {{.}} Disambiguation</title>
		<meta name="viewport" content="width=device-width">
	</head>
	<body>
		CL number {{.}} exists in both Gerrit (the current code review system)
		and Rietveld (the previous code review system). Please make a choice:

		<ul>
			<li><a href="https://go-review.googlesource.com/{{.}}">Gerrit CL {{.}}</a></li>
			<li><a href="https://codereview.appspot.com/{{.}}">Rietveld CL {{.}}</a></li>
		</ul>
	</body>
</html>`))

// isGerritCL reports whether a Gerrit CL with the specified numeric change ID (e.g., "4247")
// is known to exist by querying the Gerrit API at https://go-review.googlesource.com.
// isGerritCL uses gerritCLCache as a cache of Gerrit CL IDs that exist.
func isGerritCL(ctx context.Context, id int) (bool, error) {
	// Check cache first.
	gerritCLCache.Lock()
	ok := gerritCLCache.exist[id]
	gerritCLCache.Unlock()
	if ok {
		return true, nil
	}

	// Query the Gerrit API Get Change endpoint, as documented at
	// https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#get-change.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := ctxhttp.Get(ctx, nil, fmt.Sprintf("https://go-review.googlesource.com/changes/%d", id))
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		// A Gerrit CL with this ID exists. Add it to cache.
		gerritCLCache.Lock()
		gerritCLCache.exist[id] = true
		gerritCLCache.Unlock()
		return true, nil
	case http.StatusNotFound:
		// A Gerrit CL with this ID doesn't exist. It may get created in the future.
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status code: %v", resp.Status)
	}
}

var gerritCLCache = struct {
	sync.Mutex
	exist map[int]bool // exist is a set of Gerrit CL IDs that are known to exist.
}{exist: make(map[int]bool)}

var changeMap *hashMap

// LoadChangeMap loads the specified map of Mercurial to Git revisions,
// which is used by the /change/ handler to intelligently map old hg
// revisions to their new git equivalents.
// It should be called before calling Register.
// The file should remain open as long as the process is running.
// See the implementation of this package for details.
func LoadChangeMap(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	m, err := newHashMap(f)
	if err != nil {
		return err
	}
	changeMap = m
	return nil
}

func changeHandler(w http.ResponseWriter, r *http.Request) {
	const prefix = "/change/"
	if p := r.URL.Path; p == prefix {
		// redirect /prefix/ to /prefix
		http.Redirect(w, r, p[:len(p)-1], http.StatusFound)
		return
	}
	hash := r.URL.Path[len(prefix):]
	target := "https://go.googlesource.com/go/+/" + hash
	if git := changeMap.Lookup(hash); git > 0 {
		target = fmt.Sprintf("https://go.googlesource.com/%v/+/%v", git.Repo(), git.Hash())
	}
	http.Redirect(w, r, target, http.StatusFound)
}

func designHandler(w http.ResponseWriter, r *http.Request) {
	const prefix = "/design/"
	if p := r.URL.Path; p == prefix {
		// redirect /prefix/ to /prefix
		http.Redirect(w, r, p[:len(p)-1], http.StatusFound)
		return
	}
	name := r.URL.Path[len(prefix):]
	target := "https://go.googlesource.com/proposal/+/master/design/" + name + ".md"
	http.Redirect(w, r, target, http.StatusFound)
}
