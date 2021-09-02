// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
)

// BackendOptions further configures the storage backend not expressed by the
// storage URL.
type BackendOptions struct {
	S3  S3BackendOptions  `json:"s3" toml:"s3"`
	GCS GCSBackendOptions `json:"gcs" toml:"gcs"`
}

// ParseRawURL parse raw url to url object.
func ParseRawURL(rawURL string) (*url.URL, error) {
	// https://github.com/pingcap/br/issues/603
	// In aws the secret key may contain '/+=' and '+' has a special meaning in URL.
	// Replace "+" by "%2B" here to avoid this problem.
	rawURL = strings.ReplaceAll(rawURL, "+", "%2B")
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return u, nil
}

// ParseBackend constructs a structured backend description from the
// storage URL.
func ParseBackend(rawURL string, options *BackendOptions) (*backuppb.StorageBackend, error) {
	if len(rawURL) == 0 {
		return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "empty store is not allowed")
	}
	u, err := ParseRawURL(rawURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch u.Scheme {
	case "":
		absPath, err := filepath.Abs(rawURL)
		if err != nil {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "covert data-source-dir '%s' to absolute path failed", rawURL)
		}
		local := &backuppb.Local{Path: absPath}
		return &backuppb.StorageBackend{Backend: &backuppb.StorageBackend_Local{Local: local}}, nil

	case "local", "file":
		local := &backuppb.Local{Path: u.Path}
		return &backuppb.StorageBackend{Backend: &backuppb.StorageBackend_Local{Local: local}}, nil

	case "noop":
		noop := &backuppb.Noop{}
		return &backuppb.StorageBackend{Backend: &backuppb.StorageBackend_Noop{Noop: noop}}, nil

	case "s3":
		if u.Host == "" {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "please specify the bucket for s3 in %s", rawURL)
		}
		prefix := strings.Trim(u.Path, "/")
		s3 := &backuppb.S3{Bucket: u.Host, Prefix: prefix}
		if options == nil {
			options = &BackendOptions{S3: S3BackendOptions{ForcePathStyle: true}}
		}
		ExtractQueryParameters(u, &options.S3)
		if err := options.S3.Apply(s3); err != nil {
			return nil, errors.Trace(err)
		}
		return &backuppb.StorageBackend{Backend: &backuppb.StorageBackend_S3{S3: s3}}, nil

	case "gs", "gcs":
		if u.Host == "" {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "please specify the bucket for gcs in %s", rawURL)
		}
		prefix := strings.Trim(u.Path, "/")
		gcs := &backuppb.GCS{Bucket: u.Host, Prefix: prefix}
		if options == nil {
			options = &BackendOptions{}
		}
		ExtractQueryParameters(u, &options.GCS)
		if err := options.GCS.apply(gcs); err != nil {
			return nil, errors.Trace(err)
		}
		return &backuppb.StorageBackend{Backend: &backuppb.StorageBackend_Gcs{Gcs: gcs}}, nil

	default:
		return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "storage %s not support yet", u.Scheme)
	}
}

// ExtractQueryParameters moves the query parameters of the URL into the options
// using reflection.
//
// The options must be a pointer to a struct which contains only string or bool
// fields (more types will be supported in the future), and tagged for JSON
// serialization.
//
// All of the URL's query parameters will be removed after calling this method.
func ExtractQueryParameters(u *url.URL, options interface{}) {
	type field struct {
		index int
		kind  reflect.Kind
	}

	// First, find all JSON fields in the options struct type.
	o := reflect.Indirect(reflect.ValueOf(options))
	ty := o.Type()
	numFields := ty.NumField()
	tagToField := make(map[string]field, numFields)
	for i := 0; i < numFields; i++ {
		f := ty.Field(i)
		tag := f.Tag.Get("json")
		tagToField[tag] = field{index: i, kind: f.Type.Kind()}
	}

	// Then, read content from the URL into the options.
	for key, params := range u.Query() {
		if len(params) == 0 {
			continue
		}
		param := params[0]
		normalizedKey := strings.ToLower(strings.ReplaceAll(key, "_", "-"))
		if f, ok := tagToField[normalizedKey]; ok {
			field := o.Field(f.index)
			switch f.kind {
			case reflect.Bool:
				if v, e := strconv.ParseBool(param); e == nil {
					field.SetBool(v)
				}
			case reflect.String:
				field.SetString(param)
			default:
				panic("BackendOption introduced an unsupported kind, please handle it! " + f.kind.String())
			}
		}
	}

	// Clean up the URL finally.
	u.RawQuery = ""
}

// FormatBackendURL obtains the raw URL which can be used the reconstruct the
// backend. The returned URL does not contain options for further configurating
// the backend. This is to avoid exposing secret tokens.
func FormatBackendURL(backend *backuppb.StorageBackend) (u url.URL) {
	switch b := backend.Backend.(type) {
	case *backuppb.StorageBackend_Local:
		u.Scheme = "local"
		u.Path = b.Local.Path
	case *backuppb.StorageBackend_Noop:
		u.Scheme = "noop"
		u.Path = "/"
	case *backuppb.StorageBackend_S3:
		u.Scheme = "s3"
		u.Host = b.S3.Bucket
		u.Path = b.S3.Prefix
	case *backuppb.StorageBackend_Gcs:
		u.Scheme = "gcs"
		u.Host = b.Gcs.Bucket
		u.Path = b.Gcs.Prefix
	}
	return
}
