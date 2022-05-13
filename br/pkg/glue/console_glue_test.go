// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package glue_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/fatih/color"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/stretchr/testify/require"
)

func TestColorfulTUIFunctions(t *testing.T) {
	// when testing, the teriminal would be redirected to the dummy terminal.
	color.NoColor = false

	t.Run("TestPrettyString", testPrettyString)
	t.Run("TestPrettyStringSlicing", testPrettyStringSlicing)
	t.Run("TestPrintFrame", testPrintFrame)
}

func testPrettyString(t *testing.T) {
	fgAttrs := []color.Attribute{
		// -1 means skip this type of attr.
		color.Attribute(-1),
		color.FgHiBlack,
		color.FgHiRed,
		color.FgHiGreen,
		color.FgHiYellow,
		color.FgHiBlue,
		color.FgHiMagenta,
		color.FgHiCyan,
		color.FgHiWhite,
	}
	bgAttrs := []color.Attribute{
		color.Attribute(-1),
		color.BgBlack,
		color.BgRed,
		color.BgGreen,
		color.BgYellow,
		color.BgBlue,
		color.BgMagenta,
		color.BgCyan,
		color.BgWhite,
	}
	fontAttrs := []color.Attribute{
		color.Attribute(-1),
		color.Bold,
		color.Faint,
		color.Italic,
		color.Underline,
		color.BlinkSlow,
		color.BlinkRapid,
		color.ReverseVideo,
		color.Concealed,
		color.CrossedOut,
	}
	runTest := func(c *color.Color) {
		ps := glue.NewPrettyString(c.Sprint("hello, world"))
		require.Equal(t, ps.Len(), 12, "%v vs %v", ps.Pretty(), ps.Raw())
	}

	for _, fg := range fgAttrs {
		for _, bg := range bgAttrs {
			for _, ft := range fontAttrs {
				as := make([]color.Attribute, 0, 3)
				for _, attr := range []color.Attribute{fg, bg, ft} {
					if attr != -1 {
						as = append(as, attr)
					}
				}

				runTest(color.New(as...))
			}
		}
	}

}

func testPrettyStringSlicing(t *testing.T) {
	bs := glue.NewPrettyString(color.HiBlackString("hello, world") + color.CyanString(", and my friend"))
	checkInternalConsistency := func(ss ...glue.PrettyString) {
		for _, s := range ss {
			sp := glue.NewPrettyString(s.Pretty())
			require.Equal(t, sp.Raw(), s.Raw(), "%#v", ss)
		}
	}
	testSplit := func(s glue.PrettyString, n int) (glue.PrettyString, glue.PrettyString) {
		raw := s.Raw()
		left, right := s.SplitAt(n)
		require.Equal(t, left.Raw(), raw[:n], "%#v(@%d) -> (\n%#v \n%#v)", s, n, left, right)
		require.Equal(t, right.Raw(), raw[n:], "%#v(@%d) -> (\n%#v \n%#v)", s, n, left, right)
		checkInternalConsistency(left, right)
		return left, right
	}

	testSplit(bs, 5)
	l, r := testSplit(bs, 15)
	testSplit(l, 5)
	testSplit(r, 3)
	testSplit(bs, 12)
}

type writerGlue struct {
	glue.NoOPConsoleGlue
	w io.Writer
}

func (w writerGlue) Write(b []byte) (int, error) {
	return w.w.Write(b)
}

func testPrintFrame(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	ops := glue.ConsoleOperations{writerGlue{w: buf}}
	f, ok := ops.RootFrame().OffsetLeft(10)
	require.True(t, ok)
	f = f.WithWidth(10)
	bs := glue.NewPrettyString(color.HiGreenString("hello, world") +
		color.CyanString(", and my friend") +
		color.RedString(", and all good people."))
	indent := strings.Repeat(" ", 10)
	/*
		hello, wor
		ld, and my
		 friend, a
		nd all goo
		d people.
	*/
	expected := color.HiGreenString("hello, wor\n"+
		indent+"ld") + color.CyanString(", and my\n"+
		indent+" friend") + color.RedString(", a\n"+
		indent+"nd all goo\n"+
		indent+"d people.")
	f.Print(bs)
	require.Equal(t, expected, buf.String())
}
