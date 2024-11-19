package storage_test

import (
	"context"
	"io"
	"os"
	"testing"

	. "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBatched(t *testing.T) {
	ctx := context.Background()
	bat := Batch(nil) // Passing nil as we don't need actual storage operations

	// Test operations
	operations := []struct {
		name     string
		op       func() error
		expected []Effect
	}{
		{
			name: "DeleteFiles",
			op: func() error {
				return bat.DeleteFiles(ctx, []string{"file1.txt", "file2.txt"})
			},
			expected: []Effect{EffDeleteFiles{Files: []string{"file1.txt", "file2.txt"}}},
		},
		{
			name: "DeleteFile",
			op: func() error {
				return bat.DeleteFile(ctx, "file3.txt")
			},
			expected: []Effect{EffDeleteFile("file3.txt")},
		},
		{
			name: "WriteFile",
			op: func() error {
				return bat.WriteFile(ctx, "file4.txt", []byte("content"))
			},
			expected: []Effect{EffPut{File: "file4.txt", Content: []byte("content")}},
		},
		{
			name: "Rename",
			op: func() error {
				return bat.Rename(ctx, "oldName.txt", "newName.txt")
			},
			expected: []Effect{EffRename{From: "oldName.txt", To: "newName.txt"}},
		},
		{
			name: "SequenceOfOperations",
			op: func() error {
				if err := bat.DeleteFile(ctx, "file5.txt"); err != nil {
					return err
				}
				if err := bat.WriteFile(ctx, "file6.txt", []byte("new content")); err != nil {
					return err
				}
				return bat.Rename(ctx, "file6.txt", "fileRenamed.txt")
			},
			expected: []Effect{
				EffDeleteFile("file5.txt"),
				EffPut{File: "file6.txt", Content: []byte("new content")},
				EffRename{From: "file6.txt", To: "fileRenamed.txt"},
			}},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			require.NoError(t, op.op())

			effects := bat.ReadOnlyEffects()
			require.Equal(t, len(op.expected), len(effects))
			for i, effect := range effects {
				require.Equal(t, op.expected[i], effect)
			}

			// Reset effects for the next test
			bat.CleanEffects()
		})
	}
}

func TestJSONEffects(t *testing.T) {
	effects := []Effect{
		EffPut{File: "example.txt", Content: []byte("Hello, world")},
		EffDeleteFiles{Files: []string{"old_file.txt", "temp.txt"}},
		EffDeleteFile("obsolete.txt"),
		EffRename{From: "old_name.txt", To: "new_name.txt"},
	}

	tmp, err := SaveJSONEffectsToTmp(effects)
	require.NoError(t, err)
	f, err := os.Open(tmp)
	require.NoError(t, err)
	buf, err := io.ReadAll(f)
	require.NoError(t, err)

	expectedJSON := `[
        {"type":"storage.EffPut","effect":{"file":"example.txt","content":"SGVsbG8sIHdvcmxk"}},
        {"type":"storage.EffDeleteFiles","effect":{"files":["old_file.txt","temp.txt"]}},
        {"type":"storage.EffDeleteFile","effect":"obsolete.txt"},
        {"type":"storage.EffRename","effect":{"from":"old_name.txt","to":"new_name.txt"}}
    ]`

	require.JSONEq(t, expectedJSON, string(buf), "Output JSON should match expected JSON")
}
