// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package source

import (
	"context"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/internal/imports"
	"golang.org/x/tools/internal/lsp/protocol"
	"golang.org/x/tools/internal/span"
)

// FileIdentity uniquely identifies a file at a version from a FileSystem.
type FileIdentity struct {
	URI     span.URI
	Version string
}

func (identity FileIdentity) String() string {
	return fmt.Sprintf("%s%s", identity.URI, identity.Version)
}

// FileHandle represents a handle to a specific version of a single file from
// a specific file system.
type FileHandle interface {
	// FileSystem returns the file system this handle was acquired from.
	FileSystem() FileSystem

	// Identity returns the FileIdentity for the file.
	Identity() FileIdentity

	// Kind returns the FileKind for the file.
	Kind() FileKind

	// Read reads the contents of a file and returns it along with its hash value.
	// If the file is not available, returns a nil slice and an error.
	Read(ctx context.Context) ([]byte, string, error)
}

// FileSystem is the interface to something that provides file contents.
type FileSystem interface {
	// GetFile returns a handle for the specified file.
	GetFile(uri span.URI) FileHandle
}

// FileKind describes the kind of the file in question.
// It can be one of Go, mod, or sum.
type FileKind int

const (
	Go = FileKind(iota)
	Mod
	Sum
)

// TokenHandle represents a handle to the *token.File for a file.
type TokenHandle interface {
	// File returns a file handle for which to get the *token.File.
	File() FileHandle

	// Token returns the *token.File for the file.
	Token(ctx context.Context) (*token.File, error)
}

// ParseGoHandle represents a handle to the AST for a file.
type ParseGoHandle interface {
	// File returns a file handle for which to get the AST.
	File() FileHandle

	// Mode returns the parse mode of this handle.
	Mode() ParseMode

	// Parse returns the parsed AST for the file.
	// If the file is not available, returns nil and an error.
	Parse(ctx context.Context) (*ast.File, error)

	// Cached returns the AST for this handle, if it has already been stored.
	Cached(ctx context.Context) (*ast.File, error)
}

// ParseMode controls the content of the AST produced when parsing a source file.
type ParseMode int

const (
	// ParseHeader specifies that the main package declaration and imports are needed.
	// This is the mode used when attempting to examine the package graph structure.
	ParseHeader = ParseMode(iota)

	// ParseExported specifies that the public symbols are needed, but things like
	// private symbols and function bodies are not.
	// This mode is used for things where a package is being consumed only as a
	// dependency.
	ParseExported

	// ParseFull specifies the full AST is needed.
	// This is used for files of direct interest where the entire contents must
	// be considered.
	ParseFull
)

// CheckPackageHandle represents a handle to a specific version of a package.
// It is uniquely defined by the file handles that make up the package.
type CheckPackageHandle interface {
	// ID returns the ID of the package associated with the CheckPackageHandle.
	ID() string

	// ParseGoHandle returns a ParseGoHandle for which to get the package.
	Files() []ParseGoHandle

	// Config is the *packages.Config that the package metadata was loaded with.
	Config() *packages.Config

	// Check returns the type-checked Package for the CheckPackageHandle.
	Check(ctx context.Context) (Package, error)

	// Cached returns the Package for the CheckPackageHandle if it has already been stored.
	Cached(ctx context.Context) (Package, error)
}

// Cache abstracts the core logic of dealing with the environment from the
// higher level logic that processes the information to produce results.
// The cache provides access to files and their contents, so the source
// package does not directly access the file system.
// A single cache is intended to be process wide, and is the primary point of
// sharing between all consumers.
// A cache may have many active sessions at any given time.
type Cache interface {
	// A FileSystem that reads file contents from external storage.
	FileSystem

	// NewSession creates a new Session manager and returns it.
	NewSession(ctx context.Context) Session

	// FileSet returns the shared fileset used by all files in the system.
	FileSet() *token.FileSet

	// TokenHandle returns a TokenHandle for the given file handle.
	TokenHandle(fh FileHandle) TokenHandle

	// ParseGoHandle returns a ParseGoHandle for the given file handle.
	ParseGoHandle(fh FileHandle, mode ParseMode) ParseGoHandle
}

// Session represents a single connection from a client.
// This is the level at which things like open files are maintained on behalf
// of the client.
// A session may have many active views at any given time.
type Session interface {
	// NewView creates a new View and returns it.
	NewView(ctx context.Context, name string, folder span.URI, options ViewOptions) View

	// Cache returns the cache that created this session.
	Cache() Cache

	// View returns a view with a mathing name, if the session has one.
	View(name string) View

	// ViewOf returns a view corresponding to the given URI.
	ViewOf(uri span.URI) View

	// Views returns the set of active views built by this session.
	Views() []View

	// Shutdown the session and all views it has created.
	Shutdown(ctx context.Context)

	// A FileSystem prefers the contents from overlays, and falls back to the
	// content from the underlying cache if no overlay is present.
	FileSystem

	// DidOpen is invoked each time a file is opened in the editor.
	DidOpen(ctx context.Context, uri span.URI, kind FileKind, text []byte)

	// DidSave is invoked each time an open file is saved in the editor.
	DidSave(uri span.URI)

	// DidClose is invoked each time an open file is closed in the editor.
	DidClose(uri span.URI)

	// IsOpen returns whether the editor currently has a file open.
	IsOpen(uri span.URI) bool

	// Called to set the effective contents of a file from this session.
	SetOverlay(uri span.URI, data []byte) (wasFirstChange bool)

	// DidChangeOutOfBand is called when a file under the root folder
	// changes. The file is not necessarily open in the editor.
	DidChangeOutOfBand(ctx context.Context, f GoFile, change protocol.FileChangeType)

	// Options returns a copy of the SessionOptions for this session.
	Options() SessionOptions

	// SetOptions sets the options of this session to new values.
	SetOptions(SessionOptions)
}

// View represents a single workspace.
// This is the level at which we maintain configuration like working directory
// and build tags.
type View interface {
	// Session returns the session that created this view.
	Session() Session

	// Name returns the name this view was constructed with.
	Name() string

	// Folder returns the root folder for this view.
	Folder() span.URI

	// BuiltinPackage returns the ast for the special "builtin" package.
	BuiltinPackage() *ast.Package

	// GetFile returns the file object for a given URI, initializing it
	// if it is not already part of the view.
	GetFile(ctx context.Context, uri span.URI) (File, error)

	// FindFile returns the file object for a given URI if it is
	// already part of the view.
	FindFile(ctx context.Context, uri span.URI) File

	// Called to set the effective contents of a file from this view.
	SetContent(ctx context.Context, uri span.URI, content []byte) (wasFirstChange bool, err error)

	// BackgroundContext returns a context used for all background processing
	// on behalf of this view.
	BackgroundContext() context.Context

	// Shutdown closes this view, and detaches it from it's session.
	Shutdown(ctx context.Context)

	// Ignore returns true if this file should be ignored by this view.
	Ignore(span.URI) bool

	Config(ctx context.Context) *packages.Config

	// RunProcessEnvFunc runs fn with the process env for this view inserted into opts.
	// Note: the process env contains cached module and filesystem state.
	RunProcessEnvFunc(ctx context.Context, fn func(*imports.Options) error, opts *imports.Options) error

	// Options returns a copy of the ViewOptions for this view.
	Options() ViewOptions
}

// File represents a source file of any type.
type File interface {
	URI() span.URI
	View() View
	Handle(ctx context.Context) FileHandle
}

// GoFile represents a Go source file that has been type-checked.
type GoFile interface {
	File

	Builtin() (*ast.File, bool)

	// GetCachedPackage returns the cached package for the file, if any.
	GetCachedPackage(ctx context.Context) (Package, error)

	// GetCachedPackage returns the cached package for the file, if any.
	GetCachedPackages(ctx context.Context) ([]Package, error)

	// GetPackage returns the CheckPackageHandle for the package that this file belongs to.
	GetCheckPackageHandle(ctx context.Context) (CheckPackageHandle, error)

	// GetPackages returns the CheckPackageHandles of the packages that this file belongs to.
	GetCheckPackageHandles(ctx context.Context) ([]CheckPackageHandle, error)

	// GetPackage returns the Package that this file belongs to.
	GetPackage(ctx context.Context) (Package, error)

	// GetPackages returns the Packages that this file belongs to.
	GetPackages(ctx context.Context) ([]Package, error)

	// GetActiveReverseDeps returns the active files belonging to the reverse
	// dependencies of this file's package.
	GetActiveReverseDeps(ctx context.Context) []GoFile
}

type ModFile interface {
	File
}

type SumFile interface {
	File
}

// Package represents a Go package that has been type-checked. It maintains
// only the relevant fields of a *go/packages.Package.
type Package interface {
	ID() string
	PkgPath() string
	GetHandles() []ParseGoHandle
	GetSyntax(context.Context) []*ast.File
	GetErrors() []packages.Error
	GetTypes() *types.Package
	GetTypesInfo() *types.Info
	GetTypesSizes() types.Sizes
	IsIllTyped() bool
	GetDiagnostics() []Diagnostic
	SetDiagnostics(a *analysis.Analyzer, diag []Diagnostic)

	// GetImport returns the CheckPackageHandle for a package imported by this package.
	GetImport(ctx context.Context, pkgPath string) (Package, error)

	// GetActionGraph returns the action graph for the given package.
	GetActionGraph(ctx context.Context, a *analysis.Analyzer) (*Action, error)

	// FindFile returns the AST and type information for a file that may
	// belong to or be part of a dependency of the given package.
	FindFile(ctx context.Context, uri span.URI, pos token.Pos) (ParseGoHandle, *ast.File, Package, error)
}
