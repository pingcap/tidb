package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"go.uber.org/multierr"
)

// Effect is an side effect that happens in the batch storage.
type Effect any

// EffPut is the side effect of a call to `WriteFile`.
type EffPut struct {
	File    string `json:"file"`
	Content []byte `json:"content"`
}

// EffDeleteFiles is the side effect of a call to `DeleteFiles`.
type EffDeleteFiles struct {
	Files []string `json:"files"`
}

// EffDeleteFile is the side effect of a call to `DeleteFile`.
type EffDeleteFile string

// EffRename is the side effect of a call to `Rename`.
type EffRename struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// JSONEffects converts a slices of effects into json.
// The json will be a tagged union: `{"type": $go_type_name, "effect": $effect}`
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

func SaveJSONEffectsToTmp(es []Effect) (string, error) {
	// Save the json to a subdir so user can redirect the output path by symlinking...
	tmp, err := os.CreateTemp(os.TempDir(), "br-effects-*.json")
	if err != nil {
		return "", err
	}
	if err := JSONEffects(es, tmp); err != nil {
		return "", err
	}
	return tmp.Name(), nil
}

// Batched is a wrapper of an external storage that suspends all write operations ("effects").
// If `Close()` without calling `Commit()`, nothing will happen in the underlying external storage.
// In that case, we have done a "dry run".
//
// You may use `ReadOnlyEffects()` to get the history of the effects.
// But don't modify the returned slice!
//
// You may use `Commit()` to execute all suspended effects.
type Batched struct {
	ExternalStorage
	effectsMu sync.Mutex
	// It will be one of:
	// EffPut, EffDeleteFiles, EffDeleteFile, EffRename
	effects []Effect
}

// Batch wraps an external storage instance to a batched version.
func Batch(s ExternalStorage) *Batched {
	return &Batched{ExternalStorage: s}
}

// Fetch all effects from the batched storage.
//
// **The returned slice should not be modified.**
func (d *Batched) ReadOnlyEffects() []Effect {
	d.effectsMu.Lock()
	defer d.effectsMu.Unlock()
	return d.effects
}

// CleanEffects cleans all suspended effects.
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
// This will cleanup all of the suspended effects.
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
