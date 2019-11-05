// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

'use strict';

import fs = require('fs');
import lsp = require('vscode-languageclient');
import vscode = require('vscode');
import path = require('path');

export function activate(ctx: vscode.ExtensionContext): void {
  // The handleDiagnostics middleware writes to the diagnostics log, in order
  // to confirm the order in which the extension received diagnostics.
  let r = Math.floor(Math.random() * 100000000);
  let diagnosticsLog = fs.openSync('/tmp/diagnostics' + r + '.log', 'w');

  let document = vscode.window.activeTextEditor.document;
  let config = vscode.workspace.getConfiguration('gopls', document.uri);
  let goplsCommand: string = config['command'];
  let goplsFlags: string[] = config['flags'];
  let serverOptions:
      lsp.ServerOptions = {command: getBinPath(goplsCommand), args: goplsFlags};
  let clientOptions: lsp.LanguageClientOptions = {
    initializationOptions: {},
    documentSelector: ['go'],
    uriConverters: {
      code2Protocol: (uri: vscode.Uri): string =>
          (uri.scheme ? uri : uri.with({scheme: 'file'})).toString(),
      protocol2Code: (uri: string) => vscode.Uri.parse(uri),
    },
    middleware: {
      handleDiagnostics: (uri: vscode.Uri, diagnostics: vscode.Diagnostic[], next: lsp.HandleDiagnosticsSignature) => {
        let diagString = "-------------------------------------------\n";
        diagString += uri.toString(); + ": " + diagnostics.length + "\n";
        if (diagnostics.length > 0) {
          diagString += "\n";
          for (const diag of diagnostics) {
            diagString += diag.message + "\n";
          }
        }
        fs.writeSync(diagnosticsLog, diagString);
        return next(uri, diagnostics);
      }
    },
    revealOutputChannelOn: lsp.RevealOutputChannelOn.Never,
  };
  const c = new lsp.LanguageClient('gopls', serverOptions, clientOptions);
  c.onReady().then(() => {
    const capabilities = c.initializeResult && c.initializeResult.capabilities;
    if (!capabilities) {
      return vscode.window.showErrorMessage(
          'The language server is not able to serve any features at the moment.');
    }
  });
  ctx.subscriptions.push(c.start());
}

function getBinPath(toolName: string): string {
  toolName = correctBinname(toolName);
  let tool = findToolIn(toolName, 'PATH', false);
  if (tool) {
    return tool;
  }
  return findToolIn(toolName, 'GOPATH', true);
}

function findToolIn(
    toolName: string, envVar: string, appendBinToPath: boolean): string {
  let value = process.env[envVar];
  if (value) {
    let paths = value.split(path.delimiter);
    for (let i = 0; i < paths.length; i++) {
      let binpath = path.join(paths[i], appendBinToPath ? 'bin' : '', toolName);
      if (fileExists(binpath)) {
        return binpath;
      }
    }
  }
  return null;
}

function fileExists(filePath: string): boolean {
  try {
    return fs.statSync(filePath).isFile();
  } catch (e) {
    return false;
  }
}

function correctBinname(toolName: string) {
  if (process.platform === 'win32')
    return toolName + '.exe';
  else
    return toolName;
}