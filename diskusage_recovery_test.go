package flatfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
)

func newRecoveryDatastore(t *testing.T) *Datastore {
	t.Helper()
	fs, err := CreateOrOpen(t.TempDir(), Prefix(2), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	return fs
}

func TestDiskUsageRecoveryCanceled(t *testing.T) {
	fs := newRecoveryDatastore(t)
	atomic.StoreInt64(&fs.diskUsage, -1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	usage, err := fs.DiskUsage(ctx)
	if usage != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("DiskUsage = %d, %v; want 0, context.Canceled", usage, err)
	}
	if got := atomic.LoadInt64(&fs.diskUsage); got != -1 {
		t.Fatalf("canceled scan changed counter to %d", got)
	}
	if _, err := fs.DiskUsage(t.Context()); err != nil {
		t.Fatalf("retry after cancellation: %v", err)
	}
}

func TestDiskUsageRecoveryTimeout(t *testing.T) {
	fs := newRecoveryDatastore(t)
	previous := DiskUsageCalcTimeout
	DiskUsageCalcTimeout = 0
	defer func() { DiskUsageCalcTimeout = previous }()
	atomic.StoreInt64(&fs.diskUsage, -1)
	usage, err := fs.DiskUsage(t.Context())
	if usage != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DiskUsage = %d, %v; want 0, context.DeadlineExceeded", usage, err)
	}
	if got := atomic.LoadInt64(&fs.diskUsage); got != -1 {
		t.Fatalf("timed-out scan published partial count %d", got)
	}
}

func TestDiskUsageRecoveryIOError(t *testing.T) {
	fs := newRecoveryDatastore(t)
	// Checkpoint writes are excluded while the directory is moved, as they
	// would be during the recovery scan itself.
	fs.diskUsageLock.Lock()
	original := fs.path
	fs.path = filepath.Join(original, "missing")
	atomic.StoreInt64(&fs.diskUsage, -1)
	fs.diskUsageLock.Unlock()
	usage, err := fs.DiskUsage(t.Context())
	fs.diskUsageLock.Lock()
	fs.path = original
	fs.diskUsageLock.Unlock()
	if usage != 0 || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DiskUsage = %d, %v; want filesystem error", usage, err)
	}
	if got := atomic.LoadInt64(&fs.diskUsage); got != -1 {
		t.Fatalf("failed scan changed counter to %d", got)
	}
	if _, err := fs.DiskUsage(t.Context()); err != nil {
		t.Fatalf("retry after I/O error: %v", err)
	}
}

func TestDiskUsageRecoveryExcludesUncommittedBatch(t *testing.T) {
	fs := newRecoveryDatastore(t)
	batch, err := fs.Batch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 2<<20)
	key := datastore.NewKey("AA-BATCH")
	if err := batch.Put(t.Context(), key, value); err != nil {
		t.Fatal(err)
	}
	// Wait for the async temp file so this actually checks its exclusion.
	if _, err := batch.(BatchReader).Get(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt64(&fs.diskUsage, -1)
	usage, err := fs.DiskUsage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if usage >= uint64(len(value)) {
		t.Fatalf("uncommitted batch counted: usage %d", usage)
	}
	if err := batch.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := fs.DiskUsage(t.Context())
	if err != nil || after < usage+uint64(len(value)) {
		t.Fatalf("committed usage = %d, %v; want at least %d", after, err, usage+uint64(len(value)))
	}
}

func TestDiskUsageRecoverySerializesMutations(t *testing.T) {
	fs := newRecoveryDatastore(t)
	ctx := t.Context()
	old := datastore.NewKey("AA-OLD")
	value := make([]byte, 64<<10)
	if err := fs.Put(ctx, old, value); err != nil {
		t.Fatal(err)
	}
	batch, err := fs.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(ctx, datastore.NewKey("AA-BATCH"), value); err != nil {
		t.Fatal(err)
	}

	// Pause recovery after it has excluded mutations but before scanning.
	fs.diskUsageLock.Lock()
	locked := true
	defer func() {
		if locked {
			fs.diskUsageLock.Unlock()
		}
	}()
	atomic.StoreInt64(&fs.diskUsage, -1)
	type result struct {
		usage uint64
		err   error
	}
	recovered := make(chan result, 1)
	go func() { usage, err := fs.DiskUsage(ctx); recovered <- result{usage, err} }()
	deadline := time.Now().Add(5 * time.Second)
	for fs.shutdownLock.TryRLock() {
		fs.shutdownLock.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("recovery did not exclude mutations")
		}
		time.Sleep(time.Millisecond)
	}

	operations := []func() error{
		func() error { return fs.Put(ctx, datastore.NewKey("AA-NEW"), value) },
		func() error { return fs.Delete(ctx, old) },
		func() error { return batch.Commit(ctx) },
	}
	var wg sync.WaitGroup
	errorsCh := make(chan error, len(operations))
	for _, operation := range operations {
		wg.Go(func() { errorsCh <- operation() })
	}
	fs.diskUsageLock.Unlock()
	locked = false
	first := <-recovered
	if first.err != nil {
		t.Fatal(first.err)
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	after, err := fs.DiskUsage(ctx)
	if err != nil || after != first.usage+uint64(len(value)) {
		t.Fatalf("usage after put/delete/commit = %d, %v; want %d", after, err, first.usage+uint64(len(value)))
	}
}

func TestDiskUsageRecoveryClosed(t *testing.T) {
	fs := newRecoveryDatastore(t)
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt64(&fs.diskUsage, -1)
	usage, err := fs.DiskUsage(t.Context())
	if usage != 0 || !errors.Is(err, ErrClosed) {
		t.Fatalf("DiskUsage = %d, %v; want 0, ErrClosed", usage, err)
	}
}

// A batch must not re-enter shutdownLock.RLock after a recovery writer queues
// for Lock. RWMutex blocks new readers at that point, including recursive ones.
func TestDiskUsageRecoveryWaitsForBatchDeletes(t *testing.T) {
	fs := newRecoveryDatastore(t)
	ctx := t.Context()
	old, fresh := datastore.NewKey("AA-OLD"), datastore.NewKey("AA-FRESH")
	value := make([]byte, 64<<10)
	if err := fs.Put(ctx, old, value); err != nil {
		t.Fatal(err)
	}
	batch, err := fs.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(ctx, fresh, value); err != nil {
		t.Fatal(err)
	}
	if err := batch.Delete(ctx, old); err != nil {
		t.Fatal(err)
	}
	// Hold the batch's rename until Commit holds the mutation read lock.
	pending := fs.opMap.Begin(fresh.String())
	if pending == nil {
		t.Fatal("unexpected pending operation")
	}
	commit := make(chan error, 1)
	go func() { commit <- batch.Commit(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for fs.shutdownLock.TryLock() {
		fs.shutdownLock.Unlock()
		if time.Now().After(deadline) {
			pending.Finish(false)
			t.Fatal("batch did not acquire mutation lock")
		}
		time.Sleep(time.Millisecond)
	}
	atomic.StoreInt64(&fs.diskUsage, -1)
	recovered := make(chan error, 1)
	go func() { _, err := fs.DiskUsage(ctx); recovered <- err }()
	deadline = time.Now().Add(5 * time.Second)
	for fs.shutdownLock.TryRLock() {
		fs.shutdownLock.RUnlock()
		if time.Now().After(deadline) {
			pending.Finish(false)
			t.Fatal("recovery did not queue for mutation lock")
		}
		time.Sleep(time.Millisecond)
	}
	pending.Finish(false)
	for _, done := range []<-chan error{commit, recovered} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("batch deletion and recovery deadlocked")
		}
	}
	if present, err := fs.Has(ctx, old); err != nil || present {
		t.Fatalf("deleted block still present: %v, %v", present, err)
	}
	if got, err := fs.Get(ctx, fresh); err != nil || len(got) != len(value) {
		t.Fatalf("new block missing: %d bytes, %v", len(got), err)
	}
}
