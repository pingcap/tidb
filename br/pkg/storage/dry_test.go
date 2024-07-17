package storage_test

import (
	"context"
	"testing"

	. "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDryRun(t *testing.T) {
	ctx := context.Background()
	dry := Batch(nil) // Passing nil as we don't need actual storage operations

	// Test operations
	operations := []struct {
		name     string
		op       func() error
		expected []Effect
	}{
		{
			name: "DeleteFiles",
			op: func() error {
				return dry.DeleteFiles(ctx, []string{"file1.txt", "file2.txt"})
			},
			expected: []Effect{EffDeleteFiles{Files: []string{"file1.txt", "file2.txt"}}},
		},
		{
			name: "DeleteFile",
			op: func() error {
				return dry.DeleteFile(ctx, "file3.txt")
			},
			expected: []Effect{EffDeleteFile("file3.txt")},
		},
		{
			name: "WriteFile",
			op: func() error {
				return dry.WriteFile(ctx, "file4.txt", []byte("content"))
			},
			expected: []Effect{EffPut{File: "file4.txt", Content: []byte("content")}},
		},
		{
			name: "Rename",
			op: func() error {
				return dry.Rename(ctx, "oldName.txt", "newName.txt")
			},
			expected: []Effect{EffRename{From: "oldName.txt", To: "newName.txt"}},
		},
		{
			name: "SequenceOfOperations",
			op: func() error {
				if err := dry.DeleteFile(ctx, "file5.txt"); err != nil {
					return err
				}
				if err := dry.WriteFile(ctx, "file6.txt", []byte("new content")); err != nil {
					return err
				}
				if err := dry.Rename(ctx, "file6.txt", "fileRenamed.txt"); err != nil {
					return err
				}
				return nil
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

			effects := dry.Effects()
			require.Equal(t, len(op.expected), len(effects))
			for i, effect := range effects {
				require.Equal(t, op.expected[i], effect)
			}

			// Reset effects for the next test
			dry.CleanEffects()
		})
	}
}
