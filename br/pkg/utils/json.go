package utils

import (
	"encoding/hex"
	"encoding/json"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
)

// MarshalBackupMeta converts the backupmeta strcture to JSON.
// Unlike json.Marshal, this function also format some []byte fields for human reading.
func MarshalBackupMeta(meta *backuppb.BackupMeta) ([]byte, error) {
	result, err := makeJSONBackupMeta(meta)
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

// UnmarshalBackupMeta converts the prettied JSON format of backupmeta
// (made by MarshalBackupMeta) back to the go structure.
func UnmarshalBackupMeta(data []byte) (*backuppb.BackupMeta, error) {
	jMeta := &jsonBackupMeta{}
	if err := json.Unmarshal(data, jMeta); err != nil {
		return nil, errors.Trace(err)
	}
	return fromJSONBackupMeta(jMeta)
}

func MarshalMetaFile(meta *backuppb.MetaFile) ([]byte, error) {
	result, err := makeJSONMetaFile(meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return json.Marshal(result)
}

func UnmarshalMetaFile(data []byte) (*backuppb.MetaFile, error) {
	jMeta := &jsonMetaFile{}
	if err := json.Unmarshal(data, jMeta); err != nil {
		return nil, errors.Trace(err)
	}
	return fromJSONMetaFile(jMeta)
}

func MarshalStatsFile(meta *backuppb.StatsFile) ([]byte, error) {
	result, err := makeJSONStatsFile(meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return json.Marshal(result)
}

func UnmarshalStatsFile(data []byte) (*backuppb.StatsFile, error) {
	jMeta := &jsonStatsFile{}
	if err := json.Unmarshal(data, jMeta); err != nil {
		return nil, errors.Trace(err)
	}
	return fromJSONStatsFile(jMeta)
}

type jsonValue any

type jsonFile struct {
	SHA256   string `json:"sha256,omitempty"`
	StartKey string `json:"start_key,omitempty"`
	EndKey   string `json:"end_key,omitempty"`
	*backuppb.File
}

func makeJSONFile(file *backuppb.File) *jsonFile {
	return &jsonFile{
		SHA256:   hex.EncodeToString(file.Sha256),
		StartKey: hex.EncodeToString(file.StartKey),
		EndKey:   hex.EncodeToString(file.EndKey),
		File:     file,
	}
}

func fromJSONFile(jFile *jsonFile) (*backuppb.File, error) {
	f := jFile.File
	var err error
	f.Sha256, err = hex.DecodeString(jFile.SHA256)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f.StartKey, err = hex.DecodeString(jFile.StartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f.EndKey, err = hex.DecodeString(jFile.EndKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return f, nil
}

type jsonRawRange struct {
	StartKey string `json:"start_key,omitempty"`
	EndKey   string `json:"end_key,omitempty"`
	*backuppb.RawRange
}

func makeJSONRawRange(raw *backuppb.RawRange) *jsonRawRange {
	return &jsonRawRange{
		StartKey: hex.EncodeToString(raw.StartKey),
		EndKey:   hex.EncodeToString(raw.EndKey),
		RawRange: raw,
	}
}

func fromJSONRawRange(rng *jsonRawRange) (*backuppb.RawRange, error) {
	r := rng.RawRange
	var err error
	r.StartKey, err = hex.DecodeString(rng.StartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.EndKey, err = hex.DecodeString(rng.EndKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return r, nil
}

type jsonSchema struct {
	Table jsonValue `json:"table,omitempty"`
	DB    jsonValue `json:"db,omitempty"`
	Stats jsonValue `json:"stats,omitempty"`
	*backuppb.Schema
}

func makeJSONSchema(schema *backuppb.Schema) (*jsonSchema, error) {
	result := &jsonSchema{Schema: schema}
	if err := json.Unmarshal(schema.Db, &result.DB); err != nil {
		return nil, errors.Trace(err)
	}

	if schema.Table != nil {
		if err := json.Unmarshal(schema.Table, &result.Table); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if schema.Stats != nil {
		if err := json.Unmarshal(schema.Stats, &result.Stats); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return result, nil
}

func fromJSONSchema(jSchema *jsonSchema) (*backuppb.Schema, error) {
	schema := jSchema.Schema
	if schema == nil {
		schema = &backuppb.Schema{}
	}

	var err error
	schema.Db, err = json.Marshal(jSchema.DB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if jSchema.Table != nil {
		schema.Table, err = json.Marshal(jSchema.Table)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if jSchema.Stats != nil {
		schema.Stats, err = json.Marshal(jSchema.Stats)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return schema, nil
}

type jsonBackupMeta struct {
	Files     []*jsonFile     `json:"files,omitempty"`
	RawRanges []*jsonRawRange `json:"raw_ranges,omitempty"`
	Schemas   []*jsonSchema   `json:"schemas,omitempty"`
	DDLs      jsonValue       `json:"ddls,omitempty"`

	*backuppb.BackupMeta
}

func makeJSONBackupMeta(meta *backuppb.BackupMeta) (*jsonBackupMeta, error) {
	result := &jsonBackupMeta{
		BackupMeta: meta,
	}
	for _, file := range meta.Files {
		result.Files = append(result.Files, makeJSONFile(file))
	}
	for _, rawRange := range meta.RawRanges {
		result.RawRanges = append(result.RawRanges, makeJSONRawRange(rawRange))
	}
	for _, schema := range meta.Schemas {
		s, err := makeJSONSchema(schema)
		if err != nil {
			return nil, err
		}
		result.Schemas = append(result.Schemas, s)
	}
	if err := json.Unmarshal(meta.Ddls, &result.DDLs); err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

func fromJSONBackupMeta(jMeta *jsonBackupMeta) (*backuppb.BackupMeta, error) {
	meta := jMeta.BackupMeta

	for _, schema := range jMeta.Schemas {
		s, err := fromJSONSchema(schema)
		if err != nil {
			return nil, err
		}
		meta.Schemas = append(meta.Schemas, s)
	}
	for _, file := range jMeta.Files {
		f, err := fromJSONFile(file)
		if err != nil {
			return nil, err
		}
		meta.Files = append(meta.Files, f)
	}
	for _, rawRange := range jMeta.RawRanges {
		rng, err := fromJSONRawRange(rawRange)
		if err != nil {
			return nil, err
		}
		meta.RawRanges = append(meta.RawRanges, rng)
	}
	var err error
	meta.Ddls, err = json.Marshal(jMeta.DDLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta, nil
}

type jsonMetaFile struct {
	DataFiles []*jsonFile     `json:"data_files,omitempty"`
	Schemas   []*jsonSchema   `json:"schemas,omitempty"`
	RawRanges []*jsonRawRange `json:"raw_ranges,omitempty"`
	DDLs      []jsonValue     `json:"ddls,omitempty"`

	*backuppb.MetaFile
}

func makeJSONMetaFile(meta *backuppb.MetaFile) (*jsonMetaFile, error) {
	result := &jsonMetaFile{
		MetaFile: meta,
	}
	for _, file := range meta.DataFiles {
		result.DataFiles = append(result.DataFiles, makeJSONFile(file))
	}
	for _, rawRange := range meta.RawRanges {
		result.RawRanges = append(result.RawRanges, makeJSONRawRange(rawRange))
	}
	for _, schema := range meta.Schemas {
		s, err := makeJSONSchema(schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result.Schemas = append(result.Schemas, s)
	}
	for _, ddl := range meta.Ddls {
		var d jsonValue
		if err := json.Unmarshal(ddl, &d); err != nil {
			return nil, errors.Trace(err)
		}
		result.DDLs = append(result.DDLs, d)
	}
	return result, nil
}

func fromJSONMetaFile(jMeta *jsonMetaFile) (*backuppb.MetaFile, error) {
	meta := jMeta.MetaFile
	if meta == nil {
		meta = &backuppb.MetaFile{}
	}

	for _, schema := range jMeta.Schemas {
		s, err := fromJSONSchema(schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		meta.Schemas = append(meta.Schemas, s)
	}
	for _, file := range jMeta.DataFiles {
		f, err := fromJSONFile(file)
		if err != nil {
			return nil, errors.Trace(err)
		}
		meta.DataFiles = append(meta.DataFiles, f)
	}
	for _, rawRange := range jMeta.RawRanges {
		rng, err := fromJSONRawRange(rawRange)
		if err != nil {
			return nil, errors.Trace(err)
		}
		meta.RawRanges = append(meta.RawRanges, rng)
	}
	for _, ddl := range jMeta.DDLs {
		d, err := json.Marshal(ddl)
		if err != nil {
			return nil, errors.Trace(err)
		}
		meta.Ddls = append(meta.Ddls, d)
	}
	return meta, nil
}

type jsonStatsBlock struct {
	JSONTable jsonValue `json:"json_table,omitempty"`

	*backuppb.StatsBlock
}

func makeJSONStatsBlock(statsBlock *backuppb.StatsBlock) (*jsonStatsBlock, error) {
	result := &jsonStatsBlock{
		StatsBlock: statsBlock,
	}
	if err := json.Unmarshal(statsBlock.JsonTable, &result.JSONTable); err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

func fromJSONStatsBlock(jMeta *jsonStatsBlock) (*backuppb.StatsBlock, error) {
	meta := jMeta.StatsBlock

	var err error
	meta.JsonTable, err = json.Marshal(jMeta.JSONTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta, nil
}

type jsonStatsFile struct {
	Blocks []*jsonStatsBlock `json:"blocks,omitempty"`
}

func makeJSONStatsFile(statsFile *backuppb.StatsFile) (*jsonStatsFile, error) {
	result := &jsonStatsFile{}
	for _, block := range statsFile.Blocks {
		b, err := makeJSONStatsBlock(block)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result.Blocks = append(result.Blocks, b)
	}
	return result, nil
}

func fromJSONStatsFile(jMeta *jsonStatsFile) (*backuppb.StatsFile, error) {
	meta := &backuppb.StatsFile{}

	for _, schema := range jMeta.Blocks {
		b, err := fromJSONStatsBlock(schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		meta.Blocks = append(meta.Blocks, b)
	}
	return meta, nil
}
