package present

import (
	"errors"
	"html/template"
	"path/filepath"
	"strings"
)

func init() {
	Register("html", parseHTML)
}

func parseHTML(ctx *Context, fileName string, lineno int, text string) (Elem, error) {
	p := strings.Fields(text)
	if len(p) != 2 {
		return nil, errors.New("invalid .html args")
	}
	name := filepath.Join(filepath.Dir(fileName), p[1])
	b, err := ctx.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return HTML{template.HTML(b)}, nil
}

type HTML struct {
	template.HTML
}

func (s HTML) TemplateName() string { return "html" }
