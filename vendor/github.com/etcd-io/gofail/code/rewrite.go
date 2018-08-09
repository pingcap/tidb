// Copyright 2016 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package code

import (
	"bufio"
	"io"
	"strings"
	"unicode"
)

// ToFailpoints turns all gofail comments into failpoint code. Returns a list of
// all failpoints it activated.
func ToFailpoints(wdst io.Writer, rsrc io.Reader) (fps []*Failpoint, err error) {
	var curfp *Failpoint

	dst := bufio.NewWriter(wdst)
	defer func() {
		if err == nil && curfp != nil {
			err = curfp.flush(dst)
		}
		if err == nil {
			err = dst.Flush()
		}
	}()

	src := bufio.NewReader(rsrc)
	for err == nil {
		l, rerr := src.ReadString('\n')
		if curfp != nil {
			if strings.HasPrefix(strings.TrimSpace(l), "//") {
				if len(l) > 0 && l[len(l)-1] == '\n' {
					l = l[:len(l)-1]
				}
				curfp.code = append(curfp.code, strings.Replace(l, "//", "\t", 1))
				continue
			} else {
				curfp.flush(dst)
				fps = append(fps, curfp)
				curfp = nil
			}
		} else if label := gofailLabel(l); label != "" {
			// expose gofail label
			l = label
		} else if curfp, err = newFailpoint(l); err != nil {
			return
		} else if curfp != nil {
			// found a new failpoint
			continue
		}
		if _, err = dst.WriteString(l); err != nil {
			return
		}
		if rerr == io.EOF {
			break
		}
	}
	return
}

// ToComments turns all failpoint code into GOFAIL comments. It returns
// a list of all failpoints  it deactivated.
func ToComments(wdst io.Writer, rsrc io.Reader) (fps []*Failpoint, err error) {
	src := bufio.NewReader(rsrc)
	dst := bufio.NewWriter(wdst)
	ws := ""
	unmatchedBraces := 0
	for err == nil {
		l, rerr := src.ReadString('\n')
		err = rerr
		lTrim := strings.TrimSpace(l)

		if unmatchedBraces > 0 {
			opening, closing := numBraces(l)
			unmatchedBraces += opening - closing
			if unmatchedBraces == 0 {
				// strip off badType footer
				lTrim = strings.Split(lTrim, "; __badType")[0]
			}
			s := ws + "//" + wsPrefix(l, ws)[1:] + lTrim + "\n"
			dst.WriteString(s)
			continue
		}

		isHdr := strings.Contains(l, ", __fpErr := __fp_") && strings.HasPrefix(lTrim, "if")
		if isHdr {
			ws = strings.Split(l, "i")[0]
			n := strings.Split(strings.Split(l, "__fp_")[1], ".")[0]
			t := strings.Split(strings.Split(l, ".(")[1], ")")[0]
			dst.WriteString(ws + "// gofail: var " + n + " " + t + "\n")
			if !strings.Contains(l, "; __badType") {
				// not single liner
				unmatchedBraces = 1
			}
			fps = append(fps, &Failpoint{name: n, varType: t})
			continue
		}

		if isLabel := strings.Contains(l, "\t/* gofail-label */"); isLabel {
			l = strings.Replace(l, "/* gofail-label */", "// gofail:", 1)
		}

		if _, werr := dst.WriteString(l); werr != nil {
			return fps, werr
		}
	}
	if err == io.EOF {
		err = nil
	}
	dst.Flush()
	return
}

func gofailLabel(l string) string {
	if !strings.HasPrefix(strings.TrimSpace(l), "// gofail:") {
		return ""
	}
	label := strings.SplitAfter(l, "// gofail:")[1]
	if len(label) == 0 || !strings.Contains(label, ":") {
		return ""
	}
	return strings.Replace(l, "// gofail:", "/* gofail-label */", 1)
}

func numBraces(l string) (opening int, closing int) {
	for i := 0; i < len(l); i++ {
		switch l[i] {
		case '{':
			opening++
		case '}':
			closing++
		}
	}
	return
}

// wsPrefix computes the left padding of a line given a whitespace prefix.
func wsPrefix(l, wsPfx string) string {
	lws := ""
	if len(wsPfx) == 0 {
		lws = l
	} else {
		wsSplit := strings.SplitAfter(l, wsPfx)
		if len(wsSplit) < 2 {
			return ""
		}
		lws = strings.Join(wsSplit[1:], "")
	}
	for i, c := range lws {
		if !unicode.IsSpace(c) {
			return lws[:i]
		}
	}
	return lws
}
