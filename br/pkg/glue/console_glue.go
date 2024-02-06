// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/fatih/color"
	"golang.org/x/term"
)

const defaultTerminalWidth = 80

// ConsoleOperations are some operations based on ConsoleGlue.
type ConsoleOperations struct {
	ConsoleGlue
}

// An extra field appending to the task.
// return type is a {key: string, value: string} tuple.
type ExtraField func() [2]string

// NOTE:
// Perhaps we'd better move these modifiers and terminal function to another package
// like `glue/termutil?`

// WithTimeCost adds the task information of time costing for `ShowTask`.
func WithTimeCost() ExtraField {
	start := time.Now()
	return func() [2]string {
		return [2]string{"take", time.Since(start).Round(time.Millisecond).String()}
	}
}

// WithConstExtraField adds an extra field with constant values.
func WithConstExtraField(key string, value any) ExtraField {
	return func() [2]string {
		return [2]string{key, fmt.Sprint(value)}
	}
}

// WithCallbackExtraField adds an extra field with the callback.
func WithCallbackExtraField(key string, value func() string) ExtraField {
	return func() [2]string {
		return [2]string{key, value()}
	}
}

func printFinalMessage(extraFields []ExtraField) func() string {
	return func() string {
		fields := make([]string, 0, len(extraFields))
		for _, fieldFunc := range extraFields {
			field := fieldFunc()
			fields = append(fields, fmt.Sprintf("%s = %s", field[0], color.New(color.Bold).Sprint(field[1])))
		}
		return fmt.Sprintf("%s { %s }", color.HiGreenString("DONE"), strings.Join(fields, ", "))
	}
}

// ShowTask prints a task start information, and mark as finished when the returned function called.
// This is for TUI presenting.
func (ops ConsoleOperations) ShowTask(message string, extraFields ...ExtraField) func() {
	ops.Print(message)
	return func() {
		fields := make([]string, 0, len(extraFields))
		for _, fieldFunc := range extraFields {
			field := fieldFunc()
			fields = append(fields, fmt.Sprintf("%s = %s", field[0], color.New(color.Bold).Sprint(field[1])))
		}
		ops.Printf("%s { %s }\n", color.HiGreenString("DONE"), strings.Join(fields, ", "))
	}
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

func (ops ConsoleOperations) IsInteractive() bool {
	f, ok := ops.In().(*os.File)
	if !ok {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

func (ops ConsoleOperations) Scanln(args ...any) (int, error) {
	return fmt.Fscanln(ops.In(), args...)
}

func (ops ConsoleOperations) GetWidth() int {
	f, ok := ops.In().(*os.File)
	if !ok {
		return defaultTerminalWidth
	}
	w, _, err := term.GetSize(int(f.Fd()))
	if err != nil {
		return defaultTerminalWidth
	}
	return w
}

func (ops ConsoleOperations) CreateTable() *Table {
	return &Table{
		console: ops,
	}
}

func (ops ConsoleOperations) Print(args ...any) {
	_, _ = fmt.Fprint(ops.Out(), args...)
}

func (ops ConsoleOperations) Println(args ...any) {
	_, _ = fmt.Fprintln(ops.Out(), args...)
}

func (ops ConsoleOperations) Printf(format string, args ...any) {
	_, _ = fmt.Fprintf(ops.Out(), format, args...)
}

type Table struct {
	console ConsoleOperations
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
//
//	 Key1: <Value>
//	Other: <Value>
//
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
// Generally, this is a abstraction of an UNIX terminal.
type ConsoleGlue interface {
	// Out returns the output port of the console.
	Out() io.Writer
	// In returns the input of the console.
	// Usually is should be an *os.File.
	In() io.Reader
}

// NoOPConsoleGlue is the glue for "embedded" BR, say, BRIE via SQL.
// This Glue simply drop all console operations.
type NoOPConsoleGlue struct{}

func (NoOPConsoleGlue) In() io.Reader {
	return strings.NewReader("")
}

func (NoOPConsoleGlue) Out() io.Writer {
	return io.Discard
}

func GetConsole(g Glue) ConsoleOperations {
	if cg, ok := g.(ConsoleGlue); ok {
		return ConsoleOperations{ConsoleGlue: cg}
	}
	return ConsoleOperations{ConsoleGlue: NoOPConsoleGlue{}}
}

// StdIOGlue is the console glue for CLI applications, like the BR CLI.
type StdIOGlue struct{}

func (s StdIOGlue) Out() io.Writer {
	return os.Stdout
}

func (s StdIOGlue) In() io.Reader {
	// should we detach whether we are in a interactive tty here?
	return os.Stdin
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
		if realSlicePoint <= start {
			endAt = i
			return
		}
		realSlicePoint += length
	}
	return
}

// Frame is an fix-width place for printing.
// It is the abstraction of some subarea of the terminal,
// you might imagine it as a panel in the tmux, but with infinity height.
// For example, printing a frame with the width of 10 chars, and 4 chars offset left, would be like:
//
//	v~~~~~~~~~~v Here is the "width of a frame".
//
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
