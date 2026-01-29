// Copyright 2025 PingCAP, Inc.
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

package workload

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

type CaseSpec struct {
	Name string
	Case Case
}

type RunConfig struct {
	TickCount    int
	TickInterval time.Duration
	Seed         int64
	Parallel     bool
}

type Runner struct {
	db    *sql.DB
	store StateStore
	cases []CaseSpec
}

func NewRunner(db *sql.DB, store StateStore, cases ...Case) (*Runner, error) {
	specs := make([]CaseSpec, len(cases))
	for i, c := range cases {
		specs[i] = CaseSpec{Case: c}
	}
	return NewRunnerWithSpecs(db, store, specs...)
}

func NewRunnerWithSpecs(db *sql.DB, store StateStore, specs ...CaseSpec) (*Runner, error) {
	if db == nil {
		return nil, fmt.Errorf("workload: nil db")
	}
	if store == nil {
		return nil, fmt.Errorf("workload: nil state store")
	}

	caseSpecs, err := normalizeCaseSpecs(specs)
	if err != nil {
		return nil, err
	}
	return &Runner{db: db, store: store, cases: caseSpecs}, nil
}

func (r *Runner) Cases() []CaseSpec {
	out := make([]CaseSpec, len(r.cases))
	copy(out, r.cases)
	return out
}

func (r *Runner) Prepare(ctx context.Context) error {
	if err := r.store.Reset(ctx); err != nil {
		return err
	}

	for _, spec := range r.cases {
		state, err := spec.Case.Prepare(Context{Context: ctx, DB: r.db})
		if err != nil {
			return err
		}
		if err := r.store.Put(ctx, spec.Name, state); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) Run(ctx context.Context, cfg RunConfig) error {
	if cfg.TickCount <= 0 {
		return fmt.Errorf("workload: TickCount must be > 0")
	}
	if cfg.TickInterval < 0 {
		return fmt.Errorf("workload: TickInterval must be >= 0")
	}

	states, err := r.store.GetAll(ctx)
	if err != nil {
		return err
	}
	byName := make(map[string]Case, len(r.cases))
	for _, spec := range r.cases {
		byName[spec.Name] = spec.Case
	}
	for name := range states {
		if _, ok := byName[name]; !ok {
			return fmt.Errorf("workload: unknown case %q in state store", name)
		}
	}

	selected := make([]CaseSpec, 0, len(r.cases))
	for _, spec := range r.cases {
		if _, ok := states[spec.Name]; ok {
			selected = append(selected, spec)
		}
	}
	if len(selected) == 0 {
		return fmt.Errorf("workload: no cases in state store; run Prepare first")
	}

	rngs := newCaseRNGs(cfg.Seed, selected)
	if cfg.Parallel {
		if err := r.runParallelTicks(ctx, cfg, selected, states, rngs); err != nil {
			return err
		}
	} else {
		if err := r.runSequentialTicks(ctx, cfg, selected, states, rngs); err != nil {
			return err
		}
	}

	for _, spec := range selected {
		state, ok := states[spec.Name]
		if !ok {
			return fmt.Errorf("workload: case %q not found in state store; run Prepare first", spec.Name)
		}
		exitCtx := ExitContext{
			Context: Context{Context: ctx, DB: r.db},
			UpdateStateFn: func(updated json.RawMessage) {
				states[spec.Name] = updated
			},
		}
		if err := spec.Case.Exit(exitCtx, state); err != nil {
			return err
		}
	}

	finalStates := make(map[string]json.RawMessage, len(selected))
	for _, spec := range selected {
		if state, ok := states[spec.Name]; ok {
			finalStates[spec.Name] = state
		}
	}
	if err := r.store.PutMany(ctx, finalStates); err != nil {
		return err
	}
	return nil
}

func (r *Runner) Verify(ctx context.Context) error {
	states, err := r.store.GetAll(ctx)
	if err != nil {
		return err
	}
	byName := make(map[string]Case, len(r.cases))
	for _, spec := range r.cases {
		byName[spec.Name] = spec.Case
	}

	for name, state := range states {
		c, ok := byName[name]
		if !ok {
			base, _, cut := strings.Cut(name, "#")
			if cut {
				c, ok = byName[base]
			}
		}
		if !ok {
			return fmt.Errorf("workload: unknown case %q in state store", name)
		}
		if err := c.Verify(Context{Context: ctx, DB: r.db}, state); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) runSequentialTicks(
	ctx context.Context,
	cfg RunConfig,
	selected []CaseSpec,
	states map[string]json.RawMessage,
	rngs map[string]*rand.Rand,
) error {
	shuffleRNG := rand.New(rand.NewPCG(uint64(cfg.Seed), uint64(cfg.Seed>>1)))
	for tick := 0; tick < cfg.TickCount; tick++ {
		shuffleRNG.Shuffle(len(selected), func(i, j int) { selected[i], selected[j] = selected[j], selected[i] })

		for _, spec := range selected {
			state, ok := states[spec.Name]
			if !ok {
				return fmt.Errorf("workload: case %q not found in state store; run Prepare first", spec.Name)
			}
			rng := rngs[spec.Name]
			tickCtx := TickContext{
				Context: Context{Context: ctx, DB: r.db},
				RNG:     rng,
				UpdateStateFn: func(updated json.RawMessage) {
					states[spec.Name] = updated
				},
			}
			if err := spec.Case.Tick(tickCtx, state); err != nil {
				return err
			}
		}

		if tick != cfg.TickCount-1 {
			if err := sleep(ctx, cfg.TickInterval); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runner) runParallelTicks(
	ctx context.Context,
	cfg RunConfig,
	selected []CaseSpec,
	states map[string]json.RawMessage,
	rngs map[string]*rand.Rand,
) error {
	var mu sync.Mutex
	for tick := 0; tick < cfg.TickCount; tick++ {
		if err := r.runParallelTick(ctx, selected, states, rngs, &mu); err != nil {
			return err
		}

		if tick != cfg.TickCount-1 {
			if err := sleep(ctx, cfg.TickInterval); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runner) runParallelTick(
	ctx context.Context,
	selected []CaseSpec,
	states map[string]json.RawMessage,
	rngs map[string]*rand.Rand,
	mu *sync.Mutex,
) error {
	if len(selected) == 0 {
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error

	for _, spec := range selected {
		spec := spec
		wg.Add(1)
		go func() {
			defer wg.Done()

			mu.Lock()
			state, ok := states[spec.Name]
			mu.Unlock()
			if !ok {
				once.Do(func() {
					firstErr = fmt.Errorf("workload: case %q not found in state store; run Prepare first", spec.Name)
					cancel()
				})
				return
			}
			rng := rngs[spec.Name]

			tickCtx := TickContext{
				Context: Context{Context: runCtx, DB: r.db},
				RNG:     rng,
				UpdateStateFn: func(updated json.RawMessage) {
					mu.Lock()
					states[spec.Name] = updated
					mu.Unlock()
				},
			}
			if err := spec.Case.Tick(tickCtx, state); err != nil {
				once.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func newCaseRNGs(seed int64, selected []CaseSpec) map[string]*rand.Rand {
	out := make(map[string]*rand.Rand, len(selected))
	for _, spec := range selected {
		out[spec.Name] = newCaseRNG(seed, spec.Name)
	}
	return out
}

func newCaseRNG(seed int64, name string) *rand.Rand {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	seq := h.Sum64() | 1
	return rand.New(rand.NewPCG(uint64(seed), seq))
}

func normalizeCaseSpecs(specs []CaseSpec) ([]CaseSpec, error) {
	out := make([]CaseSpec, 0, len(specs))
	nameCounts := make(map[string]int, len(specs))
	for _, spec := range specs {
		if spec.Case == nil {
			return nil, fmt.Errorf("workload: nil case")
		}
		if spec.Name == "" {
			nameCounts[spec.Case.Name()]++
		}
	}

	used := make(map[string]struct{}, len(specs))
	caseIndex := make(map[string]int, len(specs))
	for _, spec := range specs {
		name := spec.Name
		if name == "" {
			base := spec.Case.Name()
			if nameCounts[base] > 1 {
				caseIndex[base]++
				name = fmt.Sprintf("%s#%d", base, caseIndex[base])
			} else {
				name = base
			}
		}
		if _, ok := used[name]; ok {
			return nil, fmt.Errorf("workload: duplicate case name %q", name)
		}
		used[name] = struct{}{}
		out = append(out, CaseSpec{Name: name, Case: spec.Case})
	}
	return out, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
