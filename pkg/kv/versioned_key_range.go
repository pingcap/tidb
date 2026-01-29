package kv

// VersionedKeyRange binds a KeyRange with a per-range read_ts.
// It is used by TiCI versioned lookup; callers must ensure Range is a point range.
type VersionedKeyRange struct {
	Range  KeyRange
	ReadTS uint64
}
