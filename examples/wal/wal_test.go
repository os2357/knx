package wal_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		_, err := w.Write(rec)
		assert.NoError(t, err)
	}

	t.Log("Reading records")
	// Test reading records
	reader := w.NewReader()
	defer reader.Close()

	err = reader.Seek(0)
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
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
