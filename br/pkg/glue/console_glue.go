// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import (
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strings"

	"github.com/fatih/color"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/zap"
	"golang.org/x/term"
)

// ConsoleOperations are some operations based on ConsoleGlue.
type ConsoleOperations struct {
	ConsoleGlue
}

func (ops ConsoleOperations) RootFrame() Frame {
	return Frame{
		width:   ops.GetWidth(),
		offset:  0,
		console: ops,
	}
}

// PromptBool prompts a boolean from the user.
func (ops ConsoleOperations) PromptBool(p string) bool {
	if !ops.IsInteractive() {
		return true
	}
	for {
		ans := ""
		ops.Print(p + "(y/N) ")
		if n, err := ops.Scanln(&ans); err != nil || n == 0 {
			// EOF or reply nothing.
			return false
		}
		trimed := strings.TrimSpace(ans)
		if strings.ToLower(trimed) == "y" {
			return true
		}
		if trimed == "" || strings.ToLower(trimed) == "n" {
			return false
		}
	}
}

func (ops *ConsoleOperations) CreateTable() *Table {
	return &Table{
		console: ops,
	}
}

func (ops ConsoleOperations) Print(args ...interface{}) {
	fmt.Fprint(ops, args...)
}

func (ops ConsoleOperations) Println(args ...interface{}) {
	fmt.Fprintln(ops, args...)
}

func (ops ConsoleOperations) Printf(format string, args ...interface{}) {
	fmt.Fprintf(ops, format, args...)
}

type Table struct {
	console *ConsoleOperations
	items   [][2]string
}

func (t *Table) Add(key, value string) {
	t.items = append(t.items, [...]string{key, value})
}

func (t *Table) maxKeyLen() int {
	maxLen := 0
	for _, item := range t.items {
		if len(item[0]) > maxLen {
			maxLen = len(item[0])
		}
	}
	return maxLen
}

// Print prints the table.
// The format would be like:
//    Key1: <Value>
//   Other: <Value>
// LongKey: <Value>
// The format may change if the terminal size is small.
func (t *Table) Print() {
	value := color.New(color.Bold)
	maxLen := t.maxKeyLen()
	f, ok := t.console.RootFrame().OffsetLeftWithMinWidth( /* maxLen + len(": ") */ maxLen+2, 40)
	// If the terminal is too narrow, we should use a compacted format for printing, like:
	// Key:
	// <Value>
	if !ok {
		// No padding keys if we use the two-line style for printing.
		maxLen = 0
	}
	for _, item := range t.items {
		t.console.Printf("%*s: ", maxLen, item[0])
		vs := NewPrettyString(value.Sprint(item[1]))
		if !ok {
			// If the terminal is too narrow to hold the line,
			// print the information to next line directly.
			t.console.Println()
		}
		f.Print(vs)
		t.console.Println()
	}
}

// ConsoleGlue is the glue between BR and some type of console,
// which is the port for interact with the user.
type ConsoleGlue interface {
	io.Writer

	// IsInteractive checks whether the shell supports input.
	IsInteractive() bool
	Scanln(args ...interface{}) (int, error)
	GetWidth() int
}

type NoOPConsoleGlue struct{}

func (NoOPConsoleGlue) Write(bs []byte) (int, error) {
	return len(bs), nil
}

func (NoOPConsoleGlue) IsInteractive() bool {
	return false
}

func (NoOPConsoleGlue) Scanln(args ...interface{}) (int, error) {
	return 0, nil
}

func (NoOPConsoleGlue) GetWidth() int {
	return math.MaxUint32
}

func GetConsole(g Glue) ConsoleOperations {
	if cg, ok := g.(ConsoleGlue); ok {
		return ConsoleOperations{ConsoleGlue: cg}
	}
	return ConsoleOperations{ConsoleGlue: NoOPConsoleGlue{}}
}

type StdIOGlue struct{}

func (s StdIOGlue) Write(p []byte) (n int, err error) {
	return os.Stdout.Write(p)
}

// IsInteractive checks whether the shell supports input.
func (s StdIOGlue) IsInteractive() bool {
	// should we detach whether we are in a interactive tty here?
	return term.IsTerminal(int(os.Stdin.Fd()))
}

func (s StdIOGlue) Scanln(args ...interface{}) (int, error) {
	return fmt.Scanln(args...)
}

func (s StdIOGlue) GetWidth() int {
	width, _, err := term.GetSize(int(os.Stdin.Fd()))
	if err != nil {
		log.Warn("failed to get terminal size, using infinity", logutil.ShortError(err), zap.Int("fd", int(os.Stdin.Fd())))
		return math.MaxUint32
	}
	log.Debug("terminal width got.", zap.Int("width", width))
	return width
}

// PrettyString is a string with ANSI escape sequence which would change its color.
// this wrapper can help to do some operations over those string.
type PrettyString struct {
	pretty              string
	raw                 string
	escapeSequencePlace [][]int
}

var ansiEscapeCodeRe = regexp.MustCompile("\x1b" + `\[(?:(?:\d+;)*\d+)?m`)

// NewPrettyString wraps a string with ANSI escape seuqnces with PrettyString.
func NewPrettyString(s string) PrettyString {
	return PrettyString{
		pretty:              s,
		raw:                 ansiEscapeCodeRe.ReplaceAllLiteralString(s, ""),
		escapeSequencePlace: ansiEscapeCodeRe.FindAllStringIndex(s, -1),
	}
}

// Len returns the length of the string with removal of all ANSI sequences.
// ("The raw length") .
// Example: "\e[38;5;226mhello, world\e[0m".Len() => 12
// Note: The length of golden string "hello, world" is 12.
func (ps PrettyString) Len() int {
	return len(ps.raw)
}

// Pretty returns the pretty form of the string:
// with keeping all ANSI escape sequences.
// Example: "\e[38;5;226mhello, world\e[0m".Pretty() => "\e[38;5;226mhello, world\e[0m"
func (ps PrettyString) Pretty() string {
	return ps.pretty
}

// Raw returns the raw form of the pretty string:
// with removal of all ANSI escape sequences.
// Example: "\e[38;5;226mhello, world\e[0m".Raw() => "hello, world"
func (ps PrettyString) Raw() string {
	return ps.raw
}

// SplitAt splits a pretty string at the place and ignoring all formats.
// Example: "\e[38;5;226mhello, world\e[0m".SplitAt(5) => ["\e[38;5;226mhello", ", world\e[0m"]
func (ps PrettyString) SplitAt(n int) (left, right PrettyString) {
	if n < 0 {
		panic(fmt.Sprintf("PrettyString::SplitAt: index out of bound (%d vs %d)", n, len(ps.raw)))
	}
	if n >= len(ps.raw) {
		left = ps
		return
	}
	realSlicePoint, endAt := ps.slicePointOf(n)
	left.pretty = ps.pretty[:realSlicePoint]
	left.raw = ps.raw[:n]
	left.escapeSequencePlace = ps.escapeSequencePlace[:endAt]
	right.pretty = ps.pretty[realSlicePoint:]
	right.raw = ps.raw[n:]
	for _, rp := range ps.escapeSequencePlace[endAt:] {
		right.escapeSequencePlace = append(right.escapeSequencePlace, []int{
			rp[0] - realSlicePoint,
			rp[1] - realSlicePoint,
		})
	}
	return
}

func (ps PrettyString) slicePointOf(s int) (realSlicePoint, endAt int) {
	endAt = 0
	realSlicePoint = s
	for i, m := range ps.escapeSequencePlace {
		start, end := m[0], m[1]
		length := end - start
		if realSlicePoint > start {
			realSlicePoint += length
		} else {
			endAt = i
			return
		}
	}
	return
}

// Frame is an fix-width place for printing.
// It is the abstraction of some subarea of the terminal,
// you might imagine it as a panel in the tmux, but with infinity height.
// For example, printing a frame with the width of 10 chars, and 4 chars offset left, would be like:
//    v~~~~~~~~~~v Here is the "width of a frame".
// +--+----------+--+
// |   Hello, wor   |
// |   ld.          |
// +--+----------+--+
// ^~~^
// Here is the "offset of a frame".
type Frame struct {
	offset  int
	width   int
	console ConsoleOperations
}

func (f Frame) newLine() {
	f.console.Printf("\n%s", strings.Repeat(" ", f.offset))
}

func (f Frame) Print(s PrettyString) {
	if f.width <= 0 {
		f.console.Print(s.Pretty())
		return
	}
	for left, right := s.SplitAt(f.width); left.Len() > 0; left, right = right.SplitAt(f.width) {
		f.console.Print(left.Pretty())
		if right.Len() > 0 {
			f.newLine()
		}
	}
}

func (f Frame) WithWidth(width int) Frame {
	return Frame{
		offset:  f.offset,
		width:   width,
		console: f.console,
	}
}

func (f Frame) OffsetLeftWithMinWidth(offset, minWidth int) (Frame, bool) {
	if f.width-offset < minWidth {
		return f, false
	}
	return Frame{
		offset:  offset,
		width:   f.width - offset,
		console: f.console,
	}, true
}

func (f Frame) OffsetLeft(offset int) (Frame, bool) {
	return f.OffsetLeftWithMinWidth(offset, 1)
}
