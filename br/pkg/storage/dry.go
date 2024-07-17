package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"go.uber.org/multierr"
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

type Batched struct {
	ExternalStorage
	effectsMu sync.Mutex
	// It will be one of:
	// EffPut, EffDeleteFiles, EffDeleteFile, EffRename
	effects []Effect
}

func Batch(s ExternalStorage) *Batched {
	return &Batched{ExternalStorage: s}
}

func (d *Batched) Effects() []Effect {
	return d.effects
}

func (d *Batched) CleanEffects() {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = nil
}

func (d *Batched) DeleteFiles(ctx context.Context, names []string) error {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = append(d.effects, EffDeleteFiles{Files: names})
	return nil
}

func (d *Batched) DeleteFile(ctx context.Context, name string) error {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = append(d.effects, EffDeleteFile(name))
	return nil
}

func (d *Batched) WriteFile(ctx context.Context, name string, data []byte) error {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = append(d.effects, EffPut{File: name, Content: data})
	return nil
}

func (d *Batched) Rename(ctx context.Context, oldName, newName string) error {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	d.effects = append(d.effects, EffRename{From: oldName, To: newName})
	return nil
}

func (d *Batched) Create(ctx context.Context, path string, option *WriterOption) (ExternalFileWriter, error) {
	return nil, errors.Annotatef(berrors.ErrStorageUnknown, "ExternalStorage.Create isn't allowed in batch mode for now.")
}

// Commit performs all effects recorded so long in the REAL external storage.
func (d *Batched) Commit(ctx context.Context) error {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()

	var err error
	for _, eff := range d.effects {
		switch e := eff.(type) {
		case EffPut:
			err = multierr.Combine(d.ExternalStorage.WriteFile(ctx, e.File, e.Content), err)
		case EffDeleteFiles:
			err = multierr.Combine(d.ExternalStorage.DeleteFiles(ctx, e.Files), err)
		case EffDeleteFile:
			err = multierr.Combine(d.ExternalStorage.DeleteFile(ctx, string(e)), err)
		case EffRename:
			err = multierr.Combine(d.ExternalStorage.Rename(ctx, e.From, e.To), err)
		default:
			return errors.Annotatef(berrors.ErrStorageUnknown, "Unknown effect type %T", eff)
		}
	}

	d.effects = nil

	return nil
}
