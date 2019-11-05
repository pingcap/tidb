// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package godoc

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/tools/godoc/golangorgenv"
)

// Page describes the contents of the top-level godoc webpage.
type Page struct {
	Title    string
	Tabtitle string
	Subtitle string
	SrcPath  string
	Query    string
	Body     []byte
	GoogleCN bool // page is being served from golang.google.cn
	TreeView bool // page needs to contain treeview related js and css

	// filled in by ServePage
	SearchBox       bool
	Playground      bool
	Version         string
	GoogleAnalytics string
}

func (p *Presentation) ServePage(w http.ResponseWriter, page Page) {
	if page.Tabtitle == "" {
		page.Tabtitle = page.Title
	}
	page.SearchBox = p.Corpus.IndexEnabled
	page.Playground = p.ShowPlayground
	page.Version = runtime.Version()
	page.GoogleAnalytics = p.GoogleAnalytics
	applyTemplateToResponseWriter(w, p.GodocHTML, page)
}

func (p *Presentation) ServeError(w http.ResponseWriter, r *http.Request, relpath string, err error) {
	w.WriteHeader(http.StatusNotFound)
	if perr, ok := err.(*os.PathError); ok {
		rel, err := filepath.Rel(runtime.GOROOT(), perr.Path)
		if err != nil {
			perr.Path = "REDACTED"
		} else {
			perr.Path = filepath.Join("$GOROOT", rel)
		}
	}
	p.ServePage(w, Page{
		Title:           "File " + relpath,
		Subtitle:        relpath,
		Body:            applyTemplate(p.ErrorHTML, "errorHTML", err),
		GoogleCN:        googleCN(r),
		GoogleAnalytics: p.GoogleAnalytics,
	})
}

// googleCN reports whether request r is considered
// to be served from golang.google.cn.
func googleCN(r *http.Request) bool {
	if r.FormValue("googlecn") != "" {
		return true
	}
	if strings.HasSuffix(r.Host, ".cn") {
		return true
	}
	if !golangorgenv.CheckCountry() {
		return false
	}
	switch r.Header.Get("X-Appengine-Country") {
	case "", "ZZ", "CN":
		return true
	}
	return false
}
