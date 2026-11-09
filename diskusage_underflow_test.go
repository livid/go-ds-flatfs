package flatfs_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
)

// A stale but positive cache is accepted at startup. Deleting more bytes than
// that estimate must repair the counter without requiring another restart.
func TestDiskUsageRecoversAfterUnderestimatedCache(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	fs, err := flatfs.CreateOrOpen(dir, flatfs.Prefix(2), false)
	if err != nil {
		t.Fatal(err)
	}
	first, second := datastore.NewKey("AA-FIRST"), datastore.NewKey("AA-SECOND")
	value := make([]byte, 64<<10)
	for _, key := range []datastore.Key{first, second} {
		if err := fs.Put(ctx, key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(dir, flatfs.DiskUsageFile)
	if err := os.WriteFile(cache, []byte(`{"diskUsage":1,"accuracy":"initial-approximate"}`), 0644); err != nil {
		t.Fatal(err)
	}
	fs, err = flatfs.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	if err := fs.Delete(ctx, first); err != nil {
		t.Fatal(err)
	}
	usage, err := fs.DiskUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage < uint64(len(value)) || usage > uint64(len(value))+(1<<20) {
		t.Fatalf("usage after recovery = %d; want remaining block plus metadata", usage)
	}
	if err := fs.Delete(ctx, second); err != nil {
		t.Fatal(err)
	}
	after, err := fs.DiskUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage-after != uint64(len(value)) {
		t.Fatalf("subsequent deletion removed %d accounted bytes, want %d", usage-after, len(value))
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cache)
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		DiskUsage int64 `json:"diskUsage"`
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.DiskUsage != int64(after) || saved.DiskUsage <= 0 {
		t.Fatalf("persisted usage = %d, want %d", saved.DiskUsage, after)
	}
	reopened, err := flatfs.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.DiskUsage(ctx)
	if err != nil || got != after {
		t.Fatalf("reopened usage = %d, %v; want %d", got, err, after)
	}
}
