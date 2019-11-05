// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lsp

import (
	"bytes"
	"context"
	"os"
	"path"

	"golang.org/x/tools/internal/jsonrpc2"
	"golang.org/x/tools/internal/lsp/debug"
	"golang.org/x/tools/internal/lsp/protocol"
	"golang.org/x/tools/internal/lsp/source"
	"golang.org/x/tools/internal/span"
	"golang.org/x/tools/internal/telemetry/log"
	errors "golang.org/x/xerrors"
)

func (s *Server) initialize(ctx context.Context, params *protocol.ParamInitia) (*protocol.InitializeResult, error) {
	s.stateMu.Lock()
	state := s.state
	s.stateMu.Unlock()
	if state >= serverInitializing {
		return nil, jsonrpc2.NewErrorf(jsonrpc2.CodeInvalidRequest, "server already initialized")
	}
	s.stateMu.Lock()
	s.state = serverInitializing
	s.stateMu.Unlock()

	options := s.session.Options()
	defer func() { s.session.SetOptions(options) }()

	//TODO: handle the options results
	source.SetOptions(&options, params.InitializationOptions)
	options.ForClientCapabilities(params.Capabilities)

	s.pendingFolders = params.WorkspaceFolders
	if len(s.pendingFolders) == 0 {
		if params.RootURI != "" {
			s.pendingFolders = []protocol.WorkspaceFolder{{
				URI:  params.RootURI,
				Name: path.Base(params.RootURI),
			}}
		} else {
			// no folders and no root, single file mode
			//TODO(iancottrell): not sure how to do single file mode yet
			//issue: golang.org/issue/31168
			return nil, errors.Errorf("single file mode not supported yet")
		}
	}

	var codeActionProvider interface{}
	if params.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport != nil &&
		len(params.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet) > 0 {
		// If the client has specified CodeActionLiteralSupport,
		// send the code actions we support.
		//
		// Using CodeActionOptions is only valid if codeActionLiteralSupport is set.
		codeActionProvider = &protocol.CodeActionOptions{
			CodeActionKinds: s.getSupportedCodeActions(),
		}
	} else {
		codeActionProvider = true
	}
	var renameOpts interface{}
	if params.Capabilities.TextDocument.Rename.PrepareSupport {
		renameOpts = &protocol.RenameOptions{
			PrepareProvider: true,
		}
	} else {
		renameOpts = true
	}
	return &protocol.InitializeResult{
		Capabilities: protocol.ServerCapabilities{
			CodeActionProvider: codeActionProvider,
			CompletionProvider: &protocol.CompletionOptions{
				TriggerCharacters: []string{"."},
			},
			DefinitionProvider:         true,
			DocumentFormattingProvider: true,
			DocumentSymbolProvider:     true,
			FoldingRangeProvider:       true,
			HoverProvider:              true,
			DocumentHighlightProvider:  true,
			DocumentLinkProvider:       &protocol.DocumentLinkOptions{},
			ReferencesProvider:         true,
			RenameProvider:             renameOpts,
			SignatureHelpProvider: &protocol.SignatureHelpOptions{
				TriggerCharacters: []string{"(", ","},
			},
			TextDocumentSync: &protocol.TextDocumentSyncOptions{
				Change:    options.TextDocumentSyncKind,
				OpenClose: true,
				Save: &protocol.SaveOptions{
					IncludeText: false,
				},
			},
			TypeDefinitionProvider: true,
			Workspace: &struct {
				WorkspaceFolders *struct {
					Supported           bool   "json:\"supported,omitempty\""
					ChangeNotifications string "json:\"changeNotifications,omitempty\""
				} "json:\"workspaceFolders,omitempty\""
			}{
				WorkspaceFolders: &struct {
					Supported           bool   "json:\"supported,omitempty\""
					ChangeNotifications string "json:\"changeNotifications,omitempty\""
				}{
					Supported:           true,
					ChangeNotifications: "workspace/didChangeWorkspaceFolders",
				},
			},
		},
	}, nil
}

func (s *Server) initialized(ctx context.Context, params *protocol.InitializedParams) error {
	s.stateMu.Lock()
	s.state = serverInitialized
	s.stateMu.Unlock()

	options := s.session.Options()
	defer func() { s.session.SetOptions(options) }()

	var registrations []protocol.Registration
	if options.ConfigurationSupported && options.DynamicConfigurationSupported {
		registrations = append(registrations,
			protocol.Registration{
				ID:     "workspace/didChangeConfiguration",
				Method: "workspace/didChangeConfiguration",
			},
			protocol.Registration{
				ID:     "workspace/didChangeWorkspaceFolders",
				Method: "workspace/didChangeWorkspaceFolders",
			},
		)
	}

	if options.WatchFileChanges && options.DynamicWatchedFilesSupported {
		registrations = append(registrations, protocol.Registration{
			ID:     "workspace/didChangeWatchedFiles",
			Method: "workspace/didChangeWatchedFiles",
			RegisterOptions: protocol.DidChangeWatchedFilesRegistrationOptions{
				Watchers: []protocol.FileSystemWatcher{{
					GlobPattern: "**/*.go",
					Kind:        float64(protocol.WatchChange),
				}},
			},
		})
	}

	if len(registrations) > 0 {
		s.client.RegisterCapability(ctx, &protocol.RegistrationParams{
			Registrations: registrations,
		})
	}

	buf := &bytes.Buffer{}
	debug.PrintVersionInfo(buf, true, debug.PlainText)
	log.Print(ctx, buf.String())

	for _, folder := range s.pendingFolders {
		if err := s.addView(ctx, folder.Name, span.NewURI(folder.URI)); err != nil {
			return err
		}
	}
	s.pendingFolders = nil

	return nil
}

func (s *Server) fetchConfig(ctx context.Context, name string, folder span.URI, vo *source.ViewOptions) error {
	if !s.session.Options().ConfigurationSupported {
		return nil
	}
	v := protocol.ParamConfig{
		protocol.ConfigurationParams{
			Items: []protocol.ConfigurationItem{{
				ScopeURI: protocol.NewURI(folder),
				Section:  "gopls",
			}, {
				ScopeURI: protocol.NewURI(folder),
				Section:  name,
			},
			},
		}, protocol.PartialResultParams{},
	}
	configs, err := s.client.Configuration(ctx, &v)
	if err != nil {
		return err
	}
	for _, config := range configs {
		//TODO: handle the options results
		source.SetOptions(vo, config)
	}
	return nil
}

func (s *Server) shutdown(ctx context.Context) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.state < serverInitialized {
		return jsonrpc2.NewErrorf(jsonrpc2.CodeInvalidRequest, "server not initialized")
	}
	// drop all the active views
	s.session.Shutdown(ctx)
	s.state = serverShutDown
	return nil
}

func (s *Server) exit(ctx context.Context) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.state != serverShutDown {
		os.Exit(1)
	}
	os.Exit(0)
	return nil
}

func setBool(b *bool, m map[string]interface{}, name string) {
	if v, ok := m[name].(bool); ok {
		*b = v
	}
}

func setNotBool(b *bool, m map[string]interface{}, name string) {
	if v, ok := m[name].(bool); ok {
		*b = !v
	}
}
