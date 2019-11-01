package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pingcap/parser/ast"
)

type Terminal interface {
	Read() string
	WriteRows([]*ast.ResultField, [][]string, time.Duration) string
	WriteEmpty(time.Duration) string
	Error(error) string
}

func NewTerm() Terminal {
	return &Term{bufio.NewReader(os.Stdin), []string{}}
}

type Term struct {
	reader   *bufio.Reader
	commands []string
}

func (t *Term) Read() string {
	if len(t.commands) > 0 {
		cmd := t.commands[0]
		t.commands = t.commands[1:]
		return cmd
	}
	fmt.Print("TiDB [pingcap]> ")
	for {
		text, _ := t.reader.ReadString('\n')
		text = strings.Trim(text, " \n\t")
		cmds := strings.Split(text, ";")
		if t.appendCommands(cmds) {
			fmt.Print("...")
			continue
		} else {
			break
		}
	}
	if len(t.commands) > 0 {
		cmd := t.commands[0]
		t.commands = t.commands[1:]
		return cmd
	} else {
		return ""
	}
}

func (t *Term) appendCommands(cmds []string) (cont bool) {
	if len(cmds) == 0 {
		panic("append empty commands")
	}
	if cmds[len(cmds)-1] == "" {
		cmds = cmds[0 : len(cmds)-1]
		if len(cmds) == 0 {
			return len(t.commands) != 0
		}
	} else {
		cont = true
	}
	if len(t.commands) == 0 {
		t.commands = cmds
	} else {
		l := len(t.commands)
		t.commands[l-1] = t.commands[l-1] + " " + cmds[0]
		t.commands = append(t.commands, cmds[1:]...)
	}
	return cont
}

type Column struct {
	Name   string
	Values []string
	Len    int
}

func (t *Term) WriteRows(fields []*ast.ResultField, rows [][]string, d time.Duration) string {
	columns := make([]*Column, len(fields))
	for i := range columns {
		columns[i] = &Column{
			Name: fields[i].Column.Name.O,
			Len:  len(fields[i].Column.Name.O),
		}
	}

	for i := range rows {
		for j, c := range columns {
			value := rows[i][j]
			c.Values = append(c.Values, value)
			if len(value) > c.Len {
				c.Len = len(value)
			}
		}
	}

	ret := t.divider(columns)
	ret += t.print(columns, -1)
	ret += t.divider(columns)
	for idx := range rows {
		ret += t.print(columns, idx)
	}
	if len(rows) != 0 {
		ret += t.divider(columns)
		ret += fmt.Sprintf("%d row in set (%.2f sec)\n", len(rows), d.Seconds())
	} else {
		ret += fmt.Sprintf("Empty set (%.2f sec)\n", d.Seconds())
	}

	return ret
}

func (t *Term) WriteEmpty(d time.Duration) string {
	return fmt.Sprintf("Execute success (%.2f sec)\n", d.Seconds())
}

func (*Term) divider(cs []*Column) string {
	ret := fmt.Sprint("+")
	for _, c := range cs {
		for i := 0; i < c.Len+2; i++ {
			ret += fmt.Sprint("-")
		}
		ret += fmt.Sprint("+")
	}
	ret += fmt.Sprintln("")
	return ret
}

func (*Term) print(cs []*Column, idx int) string {
	ret := fmt.Sprint("| ")
	for _, c := range cs {
		format := fmt.Sprintf("%%-%dv", c.Len)
		if idx < 0 {
			ret += fmt.Sprintf(format, c.Name)
		} else {
			ret += fmt.Sprintf(format, c.Values[idx])
		}
		ret += fmt.Sprint(" | ")
	}
	ret += fmt.Sprintln("")
	return ret
}

func (*Term) Error(err error) string {
	return fmt.Sprintln(err)
}
