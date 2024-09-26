// wal_test.go

package wal_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/wal"
	"io/ioutil"
)

func createInitialSegment(dir string) error {
	initialSegmentPath := filepath.Join(dir, "00000001.wal")
	return ioutil.WriteFile(initialSegmentPath, []byte{}, 0644)
}

func openWALWithRetry(opts wal.WalOptions, maxRetries int) (*wal.Wal, error) {
	var w *wal.Wal
	var err error
	for i := 0; i < maxRetries; i++ {
		w, err = wal.Open(opts)
		if err == nil {
			return w, nil
		}
		if !strings.Contains(err.Error(), "WAL is locked") {
			return nil, err
		}
		time.Sleep(time.Duration(1<<uint(i)) * time.Millisecond)
	}
	return nil, fmt.Errorf("failed to open WAL after %d retries: %v", maxRetries, err)
}

func TestWalWrite(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := createInitialSegment(tempDir); err != nil {
		t.Fatalf("Failed to create initial segment: %v", err)
	}

	opts := wal.WalOptions{
		Path:           tempDir,
		MaxSegmentSize: 1024 * 1024,
		MaxRecordSize:  1024,
	}

	w, err := wal.Open(opts)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer w.Close()

	// Write test records
	testRecords := []*wal.Record{
		{Type: wal.RecordTypeInsert, Entity: 1, Data: []byte("test data 1")},
		{Type: wal.RecordTypeUpdate, Entity: 2, Data: []byte("test data 2")},
		{Type: wal.RecordTypeDelete, Entity: 3, Data: []byte("test data 3")},
	}

	for _, rec := range testRecords {
		_, err := w.Write(rec)
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}
	}

	// Close the WAL to ensure all data is flushed
	w.Close()

	// Verify that files were created
	files, err := ioutil.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	if len(files) == 0 {
		t.Fatalf("No WAL files were created")
	}

	// You can add more specific checks here if you know the expected file format
}

func TestWalRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_recovery_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := createInitialSegment(tempDir); err != nil {
		t.Fatalf("Failed to create initial segment: %v", err)
	}

	opts := wal.WalOptions{
		Path:           tempDir,
		MaxSegmentSize: 1024 * 1024,
		MaxRecordSize:  1024,
	}

	// Create and populate WAL
	{
		w, err := openWALWithRetry(opts, 5)
		if err != nil {
			t.Fatalf("Failed to open WAL: %v", err)
		}

		testRecords := []*wal.Record{
			{Type: wal.RecordTypeInsert, Entity: 1, Data: []byte("test data 1")},
			{Type: wal.RecordTypeUpdate, Entity: 2, Data: []byte("test data 2")},
			{Type: wal.RecordTypeDelete, Entity: 3, Data: []byte("test data 3")},
		}

		for _, rec := range testRecords {
			_, err := w.Write(rec)
			if err != nil {
				t.Fatalf("Failed to write record: %v", err)
			}
		}

		w.Close()
	}

	// Reopen WAL to test recovery
	recoveredWal, err := openWALWithRetry(opts, 5)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer recoveredWal.Close()

	// Verify that the WAL was recovered successfully
	// You might need to add specific checks based on the WAL implementation
}

func TestWalCompaction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_compaction_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := createInitialSegment(tempDir); err != nil {
		t.Fatalf("Failed to create initial segment: %v", err)
	}

	opts := wal.WalOptions{
		Path:           tempDir,
		MaxSegmentSize: 1024 * 10, // 10KB
		MaxRecordSize:  1024,      // 1KB
		Seed:           uint64(time.Now().UnixNano()), // Add this line
	}

	w, err := openWALWithRetry(opts, 5)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Write many records with sizes up to MaxRecordSize to create multiple segments
	rand.Seed(int64(opts.Seed))
	for i := 0; i < 1000; i++ {
		dataSize := rand.Intn(opts.MaxRecordSize - 100) + 100 // Random size between 100 and MaxRecordSize-1 bytes
		data := make([]byte, dataSize)
		rand.Read(data)
		rec := &wal.Record{
			Type:   wal.RecordTypeInsert,
			Entity: uint64(i),
			Data:   data,
		}
		_, err := w.Write(rec)
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}
	}

	// Get initial segment count
	initialSegments, err := filepath.Glob(filepath.Join(tempDir, "*.wal"))
	if err != nil {
		t.Fatalf("Failed to list WAL segments: %v", err)
	}
	initialCount := len(initialSegments)

	if initialCount < 2 {
		t.Fatalf("Expected multiple segments, but got %d", initialCount)
	}

	// Perform compaction
	err = w.Compact()
	if err != nil {
		t.Fatalf("Failed to compact WAL: %v", err)
	}

	// Get final segment count
	finalSegments, err := filepath.Glob(filepath.Join(tempDir, "*.wal"))
	if err != nil {
		t.Fatalf("Failed to list WAL segments after compaction: %v", err)
	}
	finalCount := len(finalSegments)

	if finalCount >= initialCount {
		t.Errorf("Compaction did not reduce segment count. Initial: %d, Final: %d", initialCount, finalCount)
	}

	w.Close()
}