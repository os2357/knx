package wal_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"blockwatch.cc/knoxdb/internal/wal"
	"flag"
)

var debugFlag = flag.Int("debug", 1, "Debug level: 1, 2, or 3")

func TestMain(m *testing.M) {
	flag.Parse()
	wal.SetDebugLevel(*debugFlag)
	os.Exit(m.Run())
}

func TestWalBasicOperations(t *testing.T) {
	t.Log("Starting TestWalBasicOperations")
	tempDir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Log("Created temporary directory:", tempDir)

	opts := wal.WalOptions{
		Path:           tempDir,
		MaxSegmentSize: 1024,
		MaxRecordSize:  100,
		BufferSize:     256,
		SyncInterval:   time.Second,
	}

	t.Log("Opening WAL")
	w, err := wal.Open(opts)
	require.NoError(t, err)
	defer w.Close()

	t.Log("WAL opened successfully")

	t.Log("Writing records")
	// Test writing records
	for i := 0; i < 10; i++ {
		t.Logf("Creating record %d", i)
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		t.Logf("Writing record %d", i)
		lsn, err := w.Write(rec)
		require.NoError(t, err)
		t.Logf("Wrote record %d, LSN: %d", i, lsn)
	}

	t.Log("Creating new reader")
	reader := w.NewReader()
	require.NotNil(t, reader, "NewReader() returned nil")
	defer reader.Close()

	t.Log("Seeking to start of WAL")
	err = reader.Seek(0)
	require.NoError(t, err, "Failed to seek to start of WAL")

	t.Log("Reading records")
	for i := 0; i < 10; i++ {
		rec, err := reader.Next()
		if err != nil {
			t.Fatalf("Failed to read record %d: %v", i, err)
		}
		t.Logf("Read record %d: Type=%d, Entity=%d, TxID=%d, Data=%s", i, rec.Type, rec.Entity, rec.TxID, string(rec.Data))
		assert.Equal(t, wal.RecordType(i%3), rec.Type)
		assert.Equal(t, uint64(i), rec.Entity)
		assert.Equal(t, uint64(i*100), rec.TxID)
		assert.Equal(t, []byte(fmt.Sprintf("test data %d", i)), rec.Data)
	}

	// Test EOF
	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)

	t.Log("TestWalBasicOperations completed successfully")
}

func TestWalSegmentRollover(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	opts := wal.WalOptions{
		Path:           tempDir,
		MaxSegmentSize: 100,
		MaxRecordSize:  50,
		BufferSize:     256,
		SyncInterval:   time.Second,
	}

	w, err := wal.Open(opts)
	require.NoError(t, err)
	defer w.Close()

	// Write records until we have multiple segments
	for i := 0; i < 20; i++ {
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		_, err := w.Write(rec)
		assert.NoError(t, err)
	}

	// Check if multiple segments were created
	segments, err := filepath.Glob(filepath.Join(tempDir, "*.wal"))
	assert.NoError(t, err)
	assert.Greater(t, len(segments), 1)

	// Test reading across segments
	reader := w.NewReader()
	defer reader.Close()

	err = reader.Seek(0)
	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		rec, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, wal.RecordType(i%3), rec.Type)
		assert.Equal(t, uint64(i), rec.Entity)
		assert.Equal(t, uint64(i*100), rec.TxID)
		assert.Equal(t, []byte(fmt.Sprintf("test data %d", i)), rec.Data)
	}
}

func TestWalCheckpointAndRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	opts := wal.WalOptions{
		Path:           tempDir,
		MaxSegmentSize: 1024,
		MaxRecordSize:  100,
		BufferSize:     256,
		SyncInterval:   time.Second,
	}

	w, err := wal.Open(opts)
	require.NoError(t, err)

	// Write some records
	for i := 0; i < 10; i++ {
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		_, err := w.Write(rec)
		assert.NoError(t, err)
	}

	// Create a checkpoint
	err = w.Checkpoint()
	assert.NoError(t, err)

	// Write more records
	for i := 10; i < 20; i++ {
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		_, err := w.Write(rec)
		assert.NoError(t, err)
	}

	w.Close()

	// Reopen the WAL
	w, err = wal.Open(opts)
	require.NoError(t, err)
	defer w.Close()

	// Check if recovery was successful
	reader := w.NewReader()
	defer reader.Close()

	err = reader.Seek(0)
	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		rec, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, wal.RecordType(i%3), rec.Type)
		assert.Equal(t, uint64(i), rec.Entity)
		assert.Equal(t, uint64(i*100), rec.TxID)
		assert.Equal(t, []byte(fmt.Sprintf("test data %d", i)), rec.Data)
	}
}

func TestWalCompaction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	opts := wal.WalOptions{
		Path:           tempDir,
		MaxSegmentSize: 100,
		MaxRecordSize:  50,
		BufferSize:     256,
		SyncInterval:   time.Second,
	}

	w, err := wal.Open(opts)
	require.NoError(t, err)
	defer w.Close()

	// Write records to create multiple segments
	var compactionLSN wal.LSN
	for i := 0; i < 20; i++ {
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		lsn, err := w.Write(rec)
		assert.NoError(t, err)
		if i == 10 {
			compactionLSN = lsn
		}
	}

	// Count initial segments
	initialSegments, err := filepath.Glob(filepath.Join(tempDir, "*.wal"))
	assert.NoError(t, err)
	initialCount := len(initialSegments)

	// Perform compaction
	err = w.Compact(compactionLSN)
	assert.NoError(t, err)

	// Count remaining segments
	remainingSegments, err := filepath.Glob(filepath.Join(tempDir, "*.wal"))
	assert.NoError(t, err)
	remainingCount := len(remainingSegments)

	assert.Less(t, remainingCount, initialCount)

	// Verify that we can still read all records after compaction
	reader := w.NewReader()
	defer reader.Close()

	err = reader.Seek(0)
	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		rec, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, wal.RecordType(i%3), rec.Type)
		assert.Equal(t, uint64(i), rec.Entity)
		assert.Equal(t, uint64(i*100), rec.TxID)
		assert.Equal(t, []byte(fmt.Sprintf("test data %d", i)), rec.Data)
	}
}

func TestWalMetrics(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	opts := wal.WalOptions{
		Path:           tempDir,
		MaxSegmentSize: 1024,
		MaxRecordSize:  100,
		BufferSize:     256,
		SyncInterval:   time.Second,
	}

	w, err := wal.Open(opts)
	require.NoError(t, err)
	defer w.Close()

	// Write some records
	for i := 0; i < 10; i++ {
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		_, err := w.Write(rec)
		assert.NoError(t, err)
	}

	// Check metrics
	metrics := w.GetMetrics()
	assert.Equal(t, int64(10), metrics.TotalWrites)
	assert.Greater(t, metrics.TotalBytesWritten, int64(0))
	assert.Greater(t, metrics.AvgWriteLatency, time.Duration(0))
}

func TestWalConcurrency(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	opts := wal.WalOptions{
		Path:           tempDir,
		MaxSegmentSize: 1024,
		MaxRecordSize:  100,
		BufferSize:     256,
		SyncInterval:   time.Second,
	}

	w, err := wal.Open(opts)
	require.NoError(t, err)
	defer w.Close()

	numWorkers := 10
	numRecordsPerWorker := 100
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numRecordsPerWorker; j++ {
				rec := &wal.Record{
					Type:   wal.RecordType(j % 3),
					Entity: uint64(workerID*numRecordsPerWorker + j),
					TxID:   uint64((workerID*numRecordsPerWorker + j) * 100),
					Data:   []byte(fmt.Sprintf("worker %d data %d", workerID, j)),
				}
				_, err := w.Write(rec)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify that all records were written correctly
	reader := w.NewReader()
	defer reader.Close()

	err = reader.Seek(0)
	assert.NoError(t, err)

	recordCount := 0
	for {
		_, err := reader.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		recordCount++
	}

	assert.Equal(t, numWorkers*numRecordsPerWorker, recordCount)
}
