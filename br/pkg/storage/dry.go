package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
)

type Effect any

type EffPut struct {
	File    string `json:"file"`
	Content []byte `json:"content"`
}

type EffDeleteFiles struct {
	Files []string `json:"files"`
}

type EffDeleteFile string

type EffRename struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func JSONEffects(es []Effect, output io.Writer) error {
	type Typed struct {
		Type string `json:"type"`
		Eff  Effect `json:"effect"`
	}

	out := make([]Typed, 0, len(es))
	for _, eff := range es {
		out = append(out, Typed{
			Type: fmt.Sprintf("%T", eff),
			Eff:  eff,
		})
	}

	return json.NewEncoder(output).Encode(out)
}

type Dry struct {
	ExternalStorage
	effectsMu sync.Mutex
	// It will be one of:
	// EffPut, EffDeleteFiles, EffDeleteFile, EffRename
	effects []Effect
}

func DryRun(s ExternalStorage) *Dry {
	return &Dry{ExternalStorage: s}
}

func (d *Dry) Effects() []Effect {
	return d.effects
}

func (d *Dry) CleanEffects() {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = nil
}

func (d *Dry) DeleteFiles(ctx context.Context, names []string) error {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = append(d.effects, EffDeleteFiles{Files: names})
	return nil
}

func (d *Dry) DeleteFile(ctx context.Context, name string) error {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = append(d.effects, EffDeleteFile(name))
	return nil
}

func (d *Dry) WriteFile(ctx context.Context, name string, data []byte) error {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = append(d.effects, EffPut{File: name, Content: data})
	return nil
}

func (d *Dry) Rename(ctx context.Context, oldName, newName string) error {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = append(d.effects, EffRename{From: oldName, To: newName})
	return nil
}

func (d *Dry) Create(ctx context.Context, path string, option *WriterOption) (ExternalFileWriter, error) {
	return nil, errors.Annotatef(berrors.ErrStorageUnknown, "ExternalStorage.Create isn't allowed in dry run mode for now.")
}
