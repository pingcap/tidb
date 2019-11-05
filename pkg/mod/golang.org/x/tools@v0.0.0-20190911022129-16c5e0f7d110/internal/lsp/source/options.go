// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package source

import (
	"fmt"
	"os"

	"golang.org/x/tools/internal/lsp/protocol"
	"golang.org/x/tools/internal/telemetry/tag"
	errors "golang.org/x/xerrors"
)

var (
	DefaultSessionOptions = SessionOptions{
		TextDocumentSyncKind:   protocol.Incremental,
		HoverKind:              SynopsisDocumentation,
		InsertTextFormat:       protocol.PlainTextTextFormat,
		PreferredContentFormat: protocol.PlainText,
		SupportedCodeActions: map[FileKind]map[protocol.CodeActionKind]bool{
			Go: {
				protocol.SourceOrganizeImports: true,
				protocol.QuickFix:              true,
			},
			Mod: {},
			Sum: {},
		},
		Completion: CompletionOptions{
			Documentation: true,
			Deep:          true,
			FuzzyMatching: true,
		},
		DefaultViewOptions: ViewOptions{
			Env: os.Environ(),
		},
	}
)

type SessionOptions struct {
	Env              []string
	BuildFlags       []string
	HoverKind        HoverKind
	DisabledAnalyses map[string]struct{}

	WatchFileChanges              bool
	InsertTextFormat              protocol.InsertTextFormat
	ConfigurationSupported        bool
	DynamicConfigurationSupported bool
	DynamicWatchedFilesSupported  bool
	PreferredContentFormat        protocol.MarkupKind
	LineFoldingOnly               bool

	SupportedCodeActions map[FileKind]map[protocol.CodeActionKind]bool

	// TODO: Remove the option once we are certain there are no issues here.
	TextDocumentSyncKind protocol.TextDocumentSyncKind

	Completion CompletionOptions

	DefaultViewOptions ViewOptions
}

type ViewOptions struct {
	// Env is the current set of environment overrides on this view.
	Env []string

	// BuildFlags is used to adjust the build flags applied to the view.
	BuildFlags []string
}

type CompletionOptions struct {
	Deep              bool
	FuzzyMatching     bool
	Unimported        bool
	Documentation     bool
	FullDocumentation bool
	Placeholders      bool
}

type HoverKind int

type Options interface {
	set(name string, value interface{}) OptionResult
}

const (
	SingleLine = HoverKind(iota)
	NoDocumentation
	SynopsisDocumentation
	FullDocumentation

	// structured is an experimental setting that returns a structured hover format.
	// This format separates the signature from the documentation, so that the client
	// can do more manipulation of these fields.
	//
	// This should only be used by clients that support this behavior.
	Structured
)

type OptionResults []OptionResult

type OptionResult struct {
	Name  string
	Value interface{}
	State OptionState
	Error error
}

type OptionState int

const (
	OptionHandled = OptionState(iota)
	OptionDeprecated
	OptionUnexpected
)

func SetOptions(options Options, opts interface{}) OptionResults {
	var results OptionResults
	switch opts := opts.(type) {
	case nil:
	case map[string]interface{}:
		for name, value := range opts {
			results = append(results, options.set(name, value))
		}
	default:
		results = append(results, OptionResult{
			Value: opts,
			Error: errors.Errorf("Invalid options type %T", opts),
		})
	}
	return results
}

func (o *SessionOptions) ForClientCapabilities(caps protocol.ClientCapabilities) {
	// Check if the client supports snippets in completion items.
	if caps.TextDocument.Completion.CompletionItem != nil &&
		caps.TextDocument.Completion.CompletionItem.SnippetSupport {
		o.InsertTextFormat = protocol.SnippetTextFormat
	}
	// Check if the client supports configuration messages.
	o.ConfigurationSupported = caps.Workspace.Configuration
	o.DynamicConfigurationSupported = caps.Workspace.DidChangeConfiguration.DynamicRegistration
	o.DynamicWatchedFilesSupported = caps.Workspace.DidChangeWatchedFiles.DynamicRegistration

	// Check which types of content format are supported by this client.
	if len(caps.TextDocument.Hover.ContentFormat) > 0 {
		o.PreferredContentFormat = caps.TextDocument.Hover.ContentFormat[0]
	}
	// Check if the client supports only line folding.
	o.LineFoldingOnly = caps.TextDocument.FoldingRange.LineFoldingOnly
}

func (o *SessionOptions) set(name string, value interface{}) OptionResult {
	result := OptionResult{Name: name, Value: value}
	switch name {
	case "noIncrementalSync":
		if v, ok := result.asBool(); ok && v {
			o.TextDocumentSyncKind = protocol.Full
		}
	case "watchFileChanges":
		result.setBool(&o.WatchFileChanges)
	case "wantCompletionDocumentation":
		result.setBool(&o.Completion.Documentation)
	case "usePlaceholders":
		result.setBool(&o.Completion.Placeholders)
	case "disableDeepCompletion":
		result.setNotBool(&o.Completion.Deep)
	case "disableFuzzyMatching":
		result.setNotBool(&o.Completion.FuzzyMatching)
	case "wantUnimportedCompletions":
		result.setBool(&o.Completion.Unimported)

	case "hoverKind":
		hoverKind, ok := value.(string)
		if !ok {
			result.errorf("Invalid type %T for string option %q", value, name)
			break
		}
		switch hoverKind {
		case "NoDocumentation":
			o.HoverKind = NoDocumentation
		case "SingleLine":
			o.HoverKind = SingleLine
		case "SynopsisDocumentation":
			o.HoverKind = SynopsisDocumentation
		case "FullDocumentation":
			o.HoverKind = FullDocumentation
		case "Structured":
			o.HoverKind = Structured
		default:
			result.errorf("Unsupported hover kind", tag.Of("HoverKind", hoverKind))
		}

	case "experimentalDisabledAnalyses":
		disabledAnalyses, ok := value.([]interface{})
		if !ok {
			result.errorf("Invalid type %T for []string option %q", value, name)
			break
		}
		o.DisabledAnalyses = make(map[string]struct{})
		for _, a := range disabledAnalyses {
			o.DisabledAnalyses[fmt.Sprint(a)] = struct{}{}
		}

	case "wantSuggestedFixes":
		result.State = OptionDeprecated

	default:
		return o.DefaultViewOptions.set(name, value)
	}
	return result
}

func (o *ViewOptions) set(name string, value interface{}) OptionResult {
	result := OptionResult{Name: name, Value: value}
	switch name {
	case "env":
		menv, ok := value.(map[string]interface{})
		if !ok {
			result.errorf("invalid config gopls.env type %T", value)
			break
		}
		for k, v := range menv {
			o.Env = append(o.Env, fmt.Sprintf("%s=%s", k, v))
		}

	case "buildFlags":
		iflags, ok := value.([]interface{})
		if !ok {
			result.errorf("invalid config gopls.buildFlags type %T", value)
			break
		}
		flags := make([]string, 0, len(iflags))
		for _, flag := range iflags {
			flags = append(flags, fmt.Sprintf("%s", flag))
		}
		o.BuildFlags = flags

	default:
		result.State = OptionUnexpected
	}
	return result
}

func (r *OptionResult) errorf(msg string, values ...interface{}) {
	r.Error = errors.Errorf(msg, values...)
}

func (r *OptionResult) asBool() (bool, bool) {
	b, ok := r.Value.(bool)
	if !ok {
		r.errorf("Invalid type %T for bool option %q", r.Value, r.Name)
		return false, false
	}
	return b, true
}

func (r *OptionResult) setBool(b *bool) {
	if v, ok := r.asBool(); ok {
		*b = v
	}
}

func (r *OptionResult) setNotBool(b *bool) {
	if v, ok := r.asBool(); ok {
		*b = !v
	}
}
