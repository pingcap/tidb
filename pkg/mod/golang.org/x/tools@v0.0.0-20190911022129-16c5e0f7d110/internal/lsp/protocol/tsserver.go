package protocol

// Code generated (see typescript/README.md) DO NOT EDIT.

import (
	"context"
	"encoding/json"

	"golang.org/x/tools/internal/jsonrpc2"
	"golang.org/x/tools/internal/telemetry/log"
)

type Server interface {
	DidChangeWorkspaceFolders(context.Context, *DidChangeWorkspaceFoldersParams) error
	Initialized(context.Context, *InitializedParams) error
	Exit(context.Context) error
	DidChangeConfiguration(context.Context, *DidChangeConfigurationParams) error
	DidOpen(context.Context, *DidOpenTextDocumentParams) error
	DidChange(context.Context, *DidChangeTextDocumentParams) error
	DidClose(context.Context, *DidCloseTextDocumentParams) error
	DidSave(context.Context, *DidSaveTextDocumentParams) error
	WillSave(context.Context, *WillSaveTextDocumentParams) error
	DidChangeWatchedFiles(context.Context, *DidChangeWatchedFilesParams) error
	Progress(context.Context, *ProgressParams) error
	SetTraceNotification(context.Context, *SetTraceParams) error
	LogTraceNotification(context.Context, *LogTraceParams) error
	Implementation(context.Context, *ImplementationParams) ([]Location, error)
	TypeDefinition(context.Context, *TypeDefinitionParams) ([]Location, error)
	DocumentColor(context.Context, *DocumentColorParams) ([]ColorInformation, error)
	ColorPresentation(context.Context, *ColorPresentationParams) ([]ColorPresentation, error)
	FoldingRange(context.Context, *FoldingRangeParams) ([]FoldingRange, error)
	Declaration(context.Context, *DeclarationParams) ([]DeclarationLink, error)
	SelectionRange(context.Context, *SelectionRangeParams) ([]SelectionRange, error)
	Initialize(context.Context, *ParamInitia) (*InitializeResult, error)
	Shutdown(context.Context) error
	WillSaveWaitUntil(context.Context, *WillSaveTextDocumentParams) ([]TextEdit, error)
	Completion(context.Context, *CompletionParams) (*CompletionList, error)
	Resolve(context.Context, *CompletionItem) (*CompletionItem, error)
	Hover(context.Context, *HoverParams) (*Hover, error)
	SignatureHelp(context.Context, *SignatureHelpParams) (*SignatureHelp, error)
	Definition(context.Context, *DefinitionParams) ([]Location, error)
	References(context.Context, *ReferenceParams) ([]Location, error)
	DocumentHighlight(context.Context, *DocumentHighlightParams) ([]DocumentHighlight, error)
	DocumentSymbol(context.Context, *DocumentSymbolParams) ([]DocumentSymbol, error)
	CodeAction(context.Context, *CodeActionParams) ([]CodeAction, error)
	Symbol(context.Context, *WorkspaceSymbolParams) ([]SymbolInformation, error)
	CodeLens(context.Context, *CodeLensParams) ([]CodeLens, error)
	ResolveCodeLens(context.Context, *CodeLens) (*CodeLens, error)
	Formatting(context.Context, *DocumentFormattingParams) ([]TextEdit, error)
	RangeFormatting(context.Context, *DocumentRangeFormattingParams) ([]TextEdit, error)
	OnTypeFormatting(context.Context, *DocumentOnTypeFormattingParams) ([]TextEdit, error)
	Rename(context.Context, *RenameParams) (*WorkspaceEdit, error)
	PrepareRename(context.Context, *PrepareRenameParams) (*Range, error)
	DocumentLink(context.Context, *DocumentLinkParams) ([]DocumentLink, error)
	ResolveDocumentLink(context.Context, *DocumentLink) (*DocumentLink, error)
	ExecuteCommand(context.Context, *ExecuteCommandParams) (interface{}, error)
}

func (h serverHandler) Deliver(ctx context.Context, r *jsonrpc2.Request, delivered bool) bool {
	if delivered {
		return false
	}
	switch r.Method {
	case "$/cancelRequest":
		var params CancelParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		r.Conn().Cancel(params.ID)
		return true
	case "workspace/didChangeWorkspaceFolders": // notif
		var params DidChangeWorkspaceFoldersParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.DidChangeWorkspaceFolders(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "initialized": // notif
		var params InitializedParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.Initialized(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "exit": // notif
		if err := h.server.Exit(ctx); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "workspace/didChangeConfiguration": // notif
		var params DidChangeConfigurationParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.DidChangeConfiguration(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/didOpen": // notif
		var params DidOpenTextDocumentParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.DidOpen(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/didChange": // notif
		var params DidChangeTextDocumentParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.DidChange(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/didClose": // notif
		var params DidCloseTextDocumentParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.DidClose(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/didSave": // notif
		var params DidSaveTextDocumentParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.DidSave(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/willSave": // notif
		var params WillSaveTextDocumentParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.WillSave(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "workspace/didChangeWatchedFiles": // notif
		var params DidChangeWatchedFilesParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.DidChangeWatchedFiles(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "$/progress": // notif
		var params ProgressParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.Progress(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "$/setTraceNotification": // notif
		var params SetTraceParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.SetTraceNotification(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "$/logTraceNotification": // notif
		var params LogTraceParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		if err := h.server.LogTraceNotification(ctx, &params); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/implementation": // req
		var params ImplementationParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Implementation(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/typeDefinition": // req
		var params TypeDefinitionParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.TypeDefinition(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/documentColor": // req
		var params DocumentColorParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.DocumentColor(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/colorPresentation": // req
		var params ColorPresentationParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.ColorPresentation(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/foldingRange": // req
		var params FoldingRangeParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.FoldingRange(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/declaration": // req
		var params DeclarationParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Declaration(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/selectionRange": // req
		var params SelectionRangeParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.SelectionRange(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "initialize": // req
		var params ParamInitia
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Initialize(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "shutdown": // req
		if r.Params != nil {
			r.Reply(ctx, nil, jsonrpc2.NewErrorf(jsonrpc2.CodeInvalidParams, "Expected no params"))
			return true
		}
		err := h.server.Shutdown(ctx)
		if err := r.Reply(ctx, nil, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/willSaveWaitUntil": // req
		var params WillSaveTextDocumentParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.WillSaveWaitUntil(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/completion": // req
		var params CompletionParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Completion(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "completionItem/resolve": // req
		var params CompletionItem
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Resolve(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/hover": // req
		var params HoverParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Hover(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/signatureHelp": // req
		var params SignatureHelpParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.SignatureHelp(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/definition": // req
		var params DefinitionParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Definition(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/references": // req
		var params ReferenceParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.References(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/documentHighlight": // req
		var params DocumentHighlightParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.DocumentHighlight(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/documentSymbol": // req
		var params DocumentSymbolParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.DocumentSymbol(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/codeAction": // req
		var params CodeActionParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.CodeAction(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "workspace/symbol": // req
		var params WorkspaceSymbolParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Symbol(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/codeLens": // req
		var params CodeLensParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.CodeLens(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "codeLens/resolve": // req
		var params CodeLens
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.ResolveCodeLens(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/formatting": // req
		var params DocumentFormattingParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Formatting(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/rangeFormatting": // req
		var params DocumentRangeFormattingParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.RangeFormatting(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/onTypeFormatting": // req
		var params DocumentOnTypeFormattingParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.OnTypeFormatting(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/rename": // req
		var params RenameParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.Rename(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/prepareRename": // req
		var params PrepareRenameParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.PrepareRename(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "textDocument/documentLink": // req
		var params DocumentLinkParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.DocumentLink(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "documentLink/resolve": // req
		var params DocumentLink
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.ResolveDocumentLink(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true
	case "workspace/executeCommand": // req
		var params ExecuteCommandParams
		if err := json.Unmarshal(*r.Params, &params); err != nil {
			sendParseError(ctx, r, err)
			return true
		}
		resp, err := h.server.ExecuteCommand(ctx, &params)
		if err := r.Reply(ctx, resp, err); err != nil {
			log.Error(ctx, "", err)
		}
		return true

	default:
		return false
	}
}

type serverDispatcher struct {
	*jsonrpc2.Conn
}

func (s *serverDispatcher) DidChangeWorkspaceFolders(ctx context.Context, params *DidChangeWorkspaceFoldersParams) error {
	return s.Conn.Notify(ctx, "workspace/didChangeWorkspaceFolders", params)
}

func (s *serverDispatcher) Initialized(ctx context.Context, params *InitializedParams) error {
	return s.Conn.Notify(ctx, "initialized", params)
}

func (s *serverDispatcher) Exit(ctx context.Context) error {
	return s.Conn.Notify(ctx, "exit", nil)
}

func (s *serverDispatcher) DidChangeConfiguration(ctx context.Context, params *DidChangeConfigurationParams) error {
	return s.Conn.Notify(ctx, "workspace/didChangeConfiguration", params)
}

func (s *serverDispatcher) DidOpen(ctx context.Context, params *DidOpenTextDocumentParams) error {
	return s.Conn.Notify(ctx, "textDocument/didOpen", params)
}

func (s *serverDispatcher) DidChange(ctx context.Context, params *DidChangeTextDocumentParams) error {
	return s.Conn.Notify(ctx, "textDocument/didChange", params)
}

func (s *serverDispatcher) DidClose(ctx context.Context, params *DidCloseTextDocumentParams) error {
	return s.Conn.Notify(ctx, "textDocument/didClose", params)
}

func (s *serverDispatcher) DidSave(ctx context.Context, params *DidSaveTextDocumentParams) error {
	return s.Conn.Notify(ctx, "textDocument/didSave", params)
}

func (s *serverDispatcher) WillSave(ctx context.Context, params *WillSaveTextDocumentParams) error {
	return s.Conn.Notify(ctx, "textDocument/willSave", params)
}

func (s *serverDispatcher) DidChangeWatchedFiles(ctx context.Context, params *DidChangeWatchedFilesParams) error {
	return s.Conn.Notify(ctx, "workspace/didChangeWatchedFiles", params)
}

func (s *serverDispatcher) Progress(ctx context.Context, params *ProgressParams) error {
	return s.Conn.Notify(ctx, "$/progress", params)
}

func (s *serverDispatcher) SetTraceNotification(ctx context.Context, params *SetTraceParams) error {
	return s.Conn.Notify(ctx, "$/setTraceNotification", params)
}

func (s *serverDispatcher) LogTraceNotification(ctx context.Context, params *LogTraceParams) error {
	return s.Conn.Notify(ctx, "$/logTraceNotification", params)
}
func (s *serverDispatcher) Implementation(ctx context.Context, params *ImplementationParams) ([]Location, error) {
	var result []Location
	if err := s.Conn.Call(ctx, "textDocument/implementation", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) TypeDefinition(ctx context.Context, params *TypeDefinitionParams) ([]Location, error) {
	var result []Location
	if err := s.Conn.Call(ctx, "textDocument/typeDefinition", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) DocumentColor(ctx context.Context, params *DocumentColorParams) ([]ColorInformation, error) {
	var result []ColorInformation
	if err := s.Conn.Call(ctx, "textDocument/documentColor", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) ColorPresentation(ctx context.Context, params *ColorPresentationParams) ([]ColorPresentation, error) {
	var result []ColorPresentation
	if err := s.Conn.Call(ctx, "textDocument/colorPresentation", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) FoldingRange(ctx context.Context, params *FoldingRangeParams) ([]FoldingRange, error) {
	var result []FoldingRange
	if err := s.Conn.Call(ctx, "textDocument/foldingRange", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) Declaration(ctx context.Context, params *DeclarationParams) ([]DeclarationLink, error) {
	var result []DeclarationLink
	if err := s.Conn.Call(ctx, "textDocument/declaration", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) SelectionRange(ctx context.Context, params *SelectionRangeParams) ([]SelectionRange, error) {
	var result []SelectionRange
	if err := s.Conn.Call(ctx, "textDocument/selectionRange", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) Initialize(ctx context.Context, params *ParamInitia) (*InitializeResult, error) {
	var result InitializeResult
	if err := s.Conn.Call(ctx, "initialize", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *serverDispatcher) Shutdown(ctx context.Context) error {
	return s.Conn.Call(ctx, "shutdown", nil, nil)
}

func (s *serverDispatcher) WillSaveWaitUntil(ctx context.Context, params *WillSaveTextDocumentParams) ([]TextEdit, error) {
	var result []TextEdit
	if err := s.Conn.Call(ctx, "textDocument/willSaveWaitUntil", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) Completion(ctx context.Context, params *CompletionParams) (*CompletionList, error) {
	var result CompletionList
	if err := s.Conn.Call(ctx, "textDocument/completion", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *serverDispatcher) Resolve(ctx context.Context, params *CompletionItem) (*CompletionItem, error) {
	var result CompletionItem
	if err := s.Conn.Call(ctx, "completionItem/resolve", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *serverDispatcher) Hover(ctx context.Context, params *HoverParams) (*Hover, error) {
	var result Hover
	if err := s.Conn.Call(ctx, "textDocument/hover", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *serverDispatcher) SignatureHelp(ctx context.Context, params *SignatureHelpParams) (*SignatureHelp, error) {
	var result SignatureHelp
	if err := s.Conn.Call(ctx, "textDocument/signatureHelp", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *serverDispatcher) Definition(ctx context.Context, params *DefinitionParams) ([]Location, error) {
	var result []Location
	if err := s.Conn.Call(ctx, "textDocument/definition", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) References(ctx context.Context, params *ReferenceParams) ([]Location, error) {
	var result []Location
	if err := s.Conn.Call(ctx, "textDocument/references", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) DocumentHighlight(ctx context.Context, params *DocumentHighlightParams) ([]DocumentHighlight, error) {
	var result []DocumentHighlight
	if err := s.Conn.Call(ctx, "textDocument/documentHighlight", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) DocumentSymbol(ctx context.Context, params *DocumentSymbolParams) ([]DocumentSymbol, error) {
	var result []DocumentSymbol
	if err := s.Conn.Call(ctx, "textDocument/documentSymbol", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) CodeAction(ctx context.Context, params *CodeActionParams) ([]CodeAction, error) {
	var result []CodeAction
	if err := s.Conn.Call(ctx, "textDocument/codeAction", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) Symbol(ctx context.Context, params *WorkspaceSymbolParams) ([]SymbolInformation, error) {
	var result []SymbolInformation
	if err := s.Conn.Call(ctx, "workspace/symbol", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) CodeLens(ctx context.Context, params *CodeLensParams) ([]CodeLens, error) {
	var result []CodeLens
	if err := s.Conn.Call(ctx, "textDocument/codeLens", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) ResolveCodeLens(ctx context.Context, params *CodeLens) (*CodeLens, error) {
	var result CodeLens
	if err := s.Conn.Call(ctx, "codeLens/resolve", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *serverDispatcher) Formatting(ctx context.Context, params *DocumentFormattingParams) ([]TextEdit, error) {
	var result []TextEdit
	if err := s.Conn.Call(ctx, "textDocument/formatting", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) RangeFormatting(ctx context.Context, params *DocumentRangeFormattingParams) ([]TextEdit, error) {
	var result []TextEdit
	if err := s.Conn.Call(ctx, "textDocument/rangeFormatting", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) OnTypeFormatting(ctx context.Context, params *DocumentOnTypeFormattingParams) ([]TextEdit, error) {
	var result []TextEdit
	if err := s.Conn.Call(ctx, "textDocument/onTypeFormatting", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) Rename(ctx context.Context, params *RenameParams) (*WorkspaceEdit, error) {
	var result WorkspaceEdit
	if err := s.Conn.Call(ctx, "textDocument/rename", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *serverDispatcher) PrepareRename(ctx context.Context, params *PrepareRenameParams) (*Range, error) {
	var result Range
	if err := s.Conn.Call(ctx, "textDocument/prepareRename", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *serverDispatcher) DocumentLink(ctx context.Context, params *DocumentLinkParams) ([]DocumentLink, error) {
	var result []DocumentLink
	if err := s.Conn.Call(ctx, "textDocument/documentLink", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverDispatcher) ResolveDocumentLink(ctx context.Context, params *DocumentLink) (*DocumentLink, error) {
	var result DocumentLink
	if err := s.Conn.Call(ctx, "documentLink/resolve", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *serverDispatcher) ExecuteCommand(ctx context.Context, params *ExecuteCommandParams) (interface{}, error) {
	var result interface{}
	if err := s.Conn.Call(ctx, "workspace/executeCommand", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

type CancelParams struct {
	/**
	 * The request id to cancel.
	 */
	ID jsonrpc2.ID `json:"id"`
}

// Types constructed to avoid structs as formal argument types
type ParamInitia struct {
	InitializeParams
	WorkDoneProgressParams
}
