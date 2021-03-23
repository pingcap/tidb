package expression

import (
	"strings"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *baseBuiltinHsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufVal, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err := b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}

	bufPat, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufPat)

	if err := b.args[1].VecEvalString(b.ctx, input, bufPat); err != nil {
		return err
	}
	if b.db == nil && n > 0 {
		for i := 0; i < n; i++ {
			if bufPat.IsNull(i) {
				continue
			}
			db, err := b.buildBlockDB(bufPat.GetString(i))
			if err != nil {
				return err
			}
			b.db = db
			break
		}
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(bufVal, bufPat)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		matched := b.hsMatch(bufVal.GetString(i))
		i64s[i] = boolToInt64(matched)
	}
	return nil
}

func (b *baseBuiltinHsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufVal, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err := b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}

	bufPat, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufPat)

	if err := b.args[1].VecEvalString(b.ctx, input, bufPat); err != nil {
		return err
	}
	if b.db == nil && n > 0 {
		for i := 0; i < n; i++ {
			if bufPat.IsNull(i) {
				continue
			}
			db, err := b.buildBlockDB(bufPat.GetString(i))
			if err != nil {
				return err
			}
			b.db = db
			break
		}
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if bufVal.IsNull(i) {
			result.AppendNull()
			continue
		}
		ids := b.hsMatchIds(bufVal.GetString(i))
		mids := ""
		if len(ids) > 0 {
			mids = strings.Join(ids, ",")
		}
		result.AppendString(mids)
	}
	return nil
}

func (b *builtinHsMatchSig) vectorized() bool {
	return true
}
