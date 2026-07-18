// Copyright 2026 PingCAP, Inc.
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

// Command godump tokenizes SQL statements with the production TiDB Scanner and
// emits a normalized token-label stream, so the Rust tidb-lexer can be checked
// for bit-faithful tokenization against the Go implementation.
//
// It reads one SQL statement per line from stdin and writes, for each, a block:
//
//	#IDX <n>
//	<offset> <label>
//	...
//	#END
//
// Labels are engine-neutral (they do not leak goyacc's internal token numbers):
//
//	EOF                 end of input
//	IDENT:<text>        identifier (bare, backtick- or ANSI-quoted)
//	KW:<UPPERCASE>      recognized keyword / builtin-function keyword
//	STR                 string literal
//	NUM:INT|FLOAT|DEC|HEX|BIT   numeric / hex / bit literal
//	OP:<text>           operator or punctuation
//	INVALID             illegal character
//	OTHER:<text>        anything not classified above (surfaces gaps early)
//
// The identifier and string token ids are discovered at runtime by lexing known
// samples, so this tool depends only on the parser package's exported API.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	// Use the production parser driver (real MyDecimal/HexLiteral/BitLiteral), so
	// numeric-literal classification never hits test_driver's stub panics.
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

// newScanner builds a scanner configured like the real parser.New(): window
// functions enabled. NewScanner alone leaves supportWindowFunc=false, which
// would diverge from how TiDB actually tokenizes OVER/ROW_NUMBER/etc.
func newScanner(src string) *parser.Scanner {
	sc := parser.NewScanner(src)
	sc.EnableWindowFunc(true)
	return sc
}

// discoverTok lexes src and returns the id of its first token.
func discoverTok(src string) int {
	sc := newScanner(src)
	var t parser.Token
	return sc.Lex(&t)
}

func classify(sql string, identTok, strTok, invalidTok, tok int, t *parser.Token) string {
	if tok == 0 {
		return "EOF"
	}
	if tok == invalidTok {
		return "INVALID"
	}
	// Recover the exact source span; some operator tokens (e.g. ->>) carry no Lit.
	raw := t.Lit
	if t.EndOffset <= len(sql) && t.Offset <= t.EndOffset {
		raw = sql[t.Offset:t.EndOffset]
	}
	// User/system variables carry Item = lit (a string), so classify them by
	// their '@' prefix before the numeric-Item check below.
	if strings.HasPrefix(raw, "@") {
		return "VAR:" + raw
	}
	// Numeric / hex / bit literals set Item to a typed value. Non-numeric Item
	// (e.g. CAST/EXTRACT set Item to the lit string) falls through to the
	// keyword/operator classification.
	switch t.Item.(type) {
	case int64, uint64:
		return "NUM:INT"
	case float64:
		return "NUM:FLOAT"
	case string, nil:
		// not a numeric literal; continue
	default:
		ty := fmt.Sprintf("%T", t.Item)
		switch {
		case strings.Contains(ty, "Decimal"):
			return "NUM:DEC"
		case strings.Contains(ty, "Hex"):
			return "NUM:HEX"
		case strings.Contains(ty, "Bit"):
			return "NUM:BIT"
		default:
			return "NUM:OTHER"
		}
	}
	if tok == strTok {
		return "STR"
	}
	if tok == identTok {
		return "IDENT:" + t.Lit
	}
	if raw == "" {
		return "OTHER:"
	}
	c := raw[0]
	switch {
	case c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
		return "KW:" + strings.ToUpper(t.Lit)
	default:
		return "OP:" + raw
	}
}

func dumpStatement(w *bufio.Writer, idx int, sql string) {
	identTok := discoverTok("`zzq_ident_probe`")
	strTok := discoverTok("'zzq_str_probe'")
	invalidTok := discoverTok("'unterminated") // unterminated string -> invalid

	sc := newScanner(sql)
	fmt.Fprintf(w, "#IDX %d\n", idx)
	for {
		var t parser.Token
		tok := sc.Lex(&t)
		label := classify(sql, identTok, strTok, invalidTok, tok, &t)
		fmt.Fprintf(w, "%d %s\n", t.Offset, label)
		if tok == 0 {
			break
		}
	}
	fmt.Fprintln(w, "#END")
}

// restoreStatement parses one SQL statement and writes its canonical restored
// SQL, matching format.DefaultRestoreFlags. On a parse error it emits a
// distinguishable `!ERR ...` line so the Rust side can align by index.
func restoreStatement(w *bufio.Writer, idx int, sql string) {
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	fmt.Fprintf(w, "#IDX %d\n", idx)
	if err != nil || len(stmts) != 1 {
		fmt.Fprintf(w, "!ERR\n#END\n")
		return
	}
	var sb strings.Builder
	rc := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := stmts[0].Restore(rc); err != nil {
		fmt.Fprintf(w, "!ERR\n#END\n")
		return
	}
	fmt.Fprintf(w, "%s\n#END\n", sb.String())
	_ = ast.StmtNode(stmts[0])
}

// framedRestore is the lossless transport used by the integration parser
// ring. The legacy stdin protocol is intentionally one physical SQL line per
// record and therefore cannot carry the mysqltest fixture inputs: those may
// contain newlines, semicolons, tabs, or any other byte accepted by Go's
// string input. Do not use Scanner for this mode.
//
// Request frames are adjacent, with no sentinel after the payload:
//
//	@<index> <sql-byte-length>\n<exact sql bytes>
//
// A response is likewise length framed:
//
//	@<index> <A|P|R> <statement-count> <payload-byte-length>\n<payload>
//
// A means Parse and Restore succeeded, P means Parse rejected the exact
// input, and R means a parsed AST could not be restored. For A, payload is
// statement-count concatenated binary records, each `u64 big-endian length`
// followed by the exact restored SQL bytes. P and R have an empty payload.
// The explicit lengths, rather than line delimiters or escaping, make the
// protocol lossless even when SQL or a restored literal contains control
// characters.
func framedRestore(in *bufio.Reader, out *bufio.Writer) error {
	for {
		header, err := in.ReadString('\n')
		if err == io.EOF && header == "" {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read frame header: %w", err)
		}
		if !strings.HasPrefix(header, "@") {
			return fmt.Errorf("invalid frame header %q", header)
		}
		parts := strings.Fields(strings.TrimSuffix(header, "\n"))
		if len(parts) != 2 {
			return fmt.Errorf("invalid frame header %q", header)
		}
		idx, err := strconv.Atoi(strings.TrimPrefix(parts[0], "@"))
		if err != nil || idx < 0 {
			return fmt.Errorf("invalid frame index %q", parts[0])
		}
		length, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil || length > uint64(^uint(0)>>1) {
			return fmt.Errorf("invalid frame length %q", parts[1])
		}
		sql := make([]byte, int(length))
		if _, err := io.ReadFull(in, sql); err != nil {
			return fmt.Errorf("read frame payload for %d: %w", idx, err)
		}

		status, count, payload := restoreFramedSQL(string(sql))
		if _, err := fmt.Fprintf(out, "@%d %s %d %d\n", idx, status, count, len(payload)); err != nil {
			return err
		}
		if _, err := out.Write(payload); err != nil {
			return err
		}
	}
}

func restoreFramedSQL(sql string) (status string, count int, payload []byte) {
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	if err != nil {
		return "P", 0, nil
	}

	var result strings.Builder
	for _, stmt := range stmts {
		var restored strings.Builder
		rc := format.NewRestoreCtx(format.DefaultRestoreFlags, &restored)
		if err := stmt.Restore(rc); err != nil {
			// A restore failure has no usable per-statement payload. Keep its
			// frame count at zero so the receiver never mistakes an absent body
			// for a truncated restoration list.
			return "R", 0, nil
		}
		text := restored.String()
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(text)))
		result.Write(length[:])
		result.WriteString(text)
	}
	return "A", len(stmts), []byte(result.String())
}

func main() {
	mode := "tokens"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	if mode == "framed-restore" {
		if err := framedRestore(bufio.NewReader(os.Stdin), out); err != nil {
			fmt.Fprintln(os.Stderr, "framed restore error:", err)
			os.Exit(1)
		}
		return
	}

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1024*1024), 16*1024*1024)

	idx := 0
	for in.Scan() {
		line := in.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "##") {
			continue // blank line or corpus comment
		}
		switch mode {
		case "restore":
			restoreStatement(out, idx, line)
		default:
			dumpStatement(out, idx, line)
		}
		idx++
	}
	if err := in.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "read error:", err)
		os.Exit(1)
	}
}
