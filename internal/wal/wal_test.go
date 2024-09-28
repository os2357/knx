package wal_test

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createWal creates a new WAL instance with specified options and returns it. This function is used to reduce redundancy in test setup.
func createWal(t *testing.T, testDir string) *wal.Wal {
	opts := wal.WalOptions{
		Path:           testDir,
		MaxSegmentSize: 1024,
	}
	w, err := wal.Create(opts)
	require.NoError(t, err, "Failed to create WAL")
	return w
}

// TestWalCreation tests the creation of a new WAL to ensure it initializes correctly.
func TestWalCreation(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()
}

// TestWalOpening tests opening an existing WAL to ensure it can be reopened correctly after being closed.
func TestWalOpening(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	w.Close()

	opts := wal.WalOptions{
		Path:           testDir,
		MaxSegmentSize: 1024,
	}
	w, err := wal.Open(0, opts)
	require.NoError(t, err, "Failed to open WAL")
	defer w.Close()
}

// TestWalWrite tests writing multiple records to the WAL to ensure data is written correctly.
func TestWalWrite(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Write records
	for i := 0; i < 10; i++ {
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		_, err := w.Write(rec)
		assert.NoError(t, err, "Failed to write record")
	}
}

// TestWalRead tests reading records from the WAL to ensure data is read correctly and matches what was written.
func TestWalRead(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Write records
	for i := 0; i < 10; i++ {
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		_, err := w.Write(rec)
		require.NoError(t, err, "Failed to write record")
	}

	// Read records and validate
	reader := w.NewReader()
	defer reader.Close()

	err := reader.Seek(0)
	require.NoError(t, err, "Failed to seek to start")

	for i := 0; i < 10; i++ {
		rec, err := reader.Next()
		assert.NoError(t, err, "Failed to read record")
		assert.Equal(t, wal.RecordType(i%3), rec.Type, "Record type mismatch")
		assert.Equal(t, uint64(i), rec.Entity, "Record entity mismatch")
		assert.Equal(t, uint64(i*100), rec.TxID, "Record TxID mismatch")
		assert.Equal(t, []byte(fmt.Sprintf("test data %d", i)), rec.Data, "Record data mismatch")
	}
}

// TestWalConcurrentWrites tests the WAL's behavior under concurrent write operations to ensure thread safety and data integrity.
func TestWalConcurrentWrites(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := &wal.Record{
				Type:   wal.RecordType(i % 3),
				Entity: uint64(i),
				TxID:   uint64(i * 100),
				Data:   []byte(fmt.Sprintf("test data %d", i)),
			}
			_, err := w.Write(rec)
			assert.NoError(t, err, "Failed to write record")
		}(i)
	}
	wg.Wait()
}

// TestWalConcurrentReadsAndWrites tests the WAL's behavior when reads and writes occur concurrently to ensure correct operation under mixed workloads.
func TestWalConcurrentReadsAndWrites(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := &wal.Record{
				Type:   wal.RecordType(i % 3),
				Entity: uint64(i),
				TxID:   uint64(i * 100),
				Data:   []byte(fmt.Sprintf("test data %d", i)),
			}
			_, err := w.Write(rec)
			assert.NoError(t, err, "Failed to write record")
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reader := w.NewReader()
			defer reader.Close()

			err := reader.Seek(0)
			require.NoError(t, err, "Failed to seek to start")

			for j := 0; j < 10; j++ {
				rec, err := reader.Next()
				assert.NoError(t, err, "Failed to read record")
				if rec != nil {
					assert.Equal(t, wal.RecordType(j%3), rec.Type, "Record type mismatch")
					assert.Equal(t, uint64(j), rec.Entity, "Record entity mismatch")
					assert.Equal(t, uint64(j*100), rec.TxID, "Record TxID mismatch")
					assert.Equal(t, []byte(fmt.Sprintf("test data %d", j)), rec.Data, "Record data mismatch")
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestWalLargeDataHandling tests writing and reading very large records (e.g., 100 MB) to ensure the WAL handles large data correctly.
func TestWalLargeDataHandling(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Write a 100 MB record
	largeData := make([]byte, 100*1024*1024)
	rec := &wal.Record{
		Type:   wal.RecordTypeInsert,
		Entity: 1,
		TxID:   100,
		Data:   largeData,
	}
	_, err := w.Write(rec)
	require.NoError(t, err, "Failed to write large record")

	// Read the record
	reader := w.NewReader()
	defer reader.Close()

	err = reader.Seek(0)
	require.NoError(t, err, "Failed to seek to start")

	readRec, err := reader.Next()
	require.NoError(t, err, "Failed to read large record")
	assert.Equal(t, largeData, readRec.Data, "Large record data mismatch")
}

// TestWalSegmentRollOver tests the behavior when the WAL rolls over to a new segment due to reaching the maximum segment size.
func TestWalSegmentRollOver(t *testing.T) {
	testDir := t.TempDir()

	opts := wal.WalOptions{
		Path:           testDir,
		MaxSegmentSize: 1024,
	}

	w, err := wal.Create(opts)
	require.NoError(t, err)
	defer w.Close()

	// Write records to trigger segment roll over
	for i := 0; i < 100; i++ {
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		_, err := w.Write(rec)
		assert.NoError(t, err, "Failed to write record")
	}

	// Verify that multiple segments were created
	segmentFiles, err := os.ReadDir(testDir)
	require.NoError(t, err, "Failed to read segment files")
	assert.Greater(t, len(segmentFiles), 1, "Expected multiple segment files")
}

// TestWalRecoveryFromPartialWrites simulates a scenario where a write operation is interrupted (e.g., due to a crash) and ensures the WAL can recover correctly.
func TestWalRecoveryFromPartialWrites(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Write a record
	rec := &wal.Record{
		Type:   wal.RecordTypeInsert,
		Entity: 1,
		TxID:   100,
		Data:   []byte("test data"),
	}
	_, err := w.Write(rec)
	require.NoError(t, err, "Failed to write record")

	// Simulate a partial write by truncating the segment file
	segmentFile := fmt.Sprintf("%d.SEG", rec.Lsn.calculateFilename(wal.WalOptions{}.MaxSegmentSize))
	err = os.Truncate(segmentFile, 0)
	require.NoError(t, err, "Failed to truncate segment file")

	// Attempt to read the truncated record
	reader := w.NewReader()
	defer reader.Close()

	err = reader.Seek(0)
	require.NoError(t, err, "Failed to seek to start")

	_, err = reader.Next()
	assert.Error(t, err, "Expected an error due to partial write")
}

// TestWalEmpty tests the behavior when the WAL is empty, ensuring that reading from an empty WAL is handled correctly.
func TestWalEmpty(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Attempt to read from an empty WAL
	reader := w.NewReader()
	defer reader.Close()

	err := reader.Seek(0)
	require.NoError(t, err, "Failed to seek to start")

	_, err = reader.Next()
	assert.Error(t, err, "Expected an error due to empty WAL")
}

// TestWalInvalidRecordType tests the behavior when invalid record types are written to the WAL to ensure proper error handling.
func TestWalInvalidRecordType(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Write a record with an invalid type
	rec := &wal.Record{
		Type:   wal.RecordType(255), // Invalid type
		Entity: 1,
		TxID:   100,
		Data:   []byte("test data"),
	}
	_, err := w.Write(rec)
	assert.Error(t, err, "Expected an error due to invalid record type")
}

// TestWalTruncation tests truncating the WAL and ensures that old segments are correctly removed and the WAL remains consistent.
func TestWalTruncation(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Write records
	for i := 0; i < 10; i++ {
		rec := &wal.Record{
			Type:   wal.RecordType(i % 3),
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		_, err := w.Write(rec)
		assert.NoError(t, err, "Failed to write record")
	}

	// Truncate the WAL
	err := w.Truncate(5)
	require.NoError(t, err, "Failed to truncate WAL")

	// Verify that only records up to the truncation point are present
	reader := w.NewReader()
	defer reader.Close()

	err = reader.Seek(0)
	require.NoError(t, err, "Failed to seek to start")

	for i := 0; i < 5; i++ {
		rec, err := reader.Next()
		assert.NoError(t, err, "Failed to read record")
		assert.Equal(t, wal.RecordType(i%3), rec.Type, "Record type mismatch")
		assert.Equal(t, uint64(i), rec.Entity, "Record entity mismatch")
		assert.Equal(t, uint64(i*100), rec.TxID, "Record TxID mismatch")
		assert.Equal(t, []byte(fmt.Sprintf("test data %d", i)), rec.Data, "Record data mismatch")
	}

	_, err = reader.Next()
	assert.Error(t, err, "Expected an error due to truncation")
}

// TestWalBitflipDetection tests the detection of data corruption by intentionally flipping a bit in a record and verifying that the WAL detects the corruption.
func TestWalBitflipDetection(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Write a record
	rec := &wal.Record{
		Type:   wal.RecordTypeInsert,
		Entity: 1,
		TxID:   100,
		Data:   []byte("test data"),
	}
	lsn, err := w.Write(rec)
	require.NoError(t, err, "Failed to write record")

	// Corrupt the record by flipping a bit in the data
	segmentFile := fmt.Sprintf("%d.SEG", lsn.calculateFilename(wal.WalOptions{}.MaxSegmentSize))
	file, err := os.OpenFile(segmentFile, os.O_RDWR, os.ModePerm)
	require.NoError(t, err, "Failed to open segment file")
	defer file.Close()

	// Seek to the position of the data in the file
	_, err = file.Seek(lsn.calculateOffset(wal.WalOptions{}.MaxSegmentSize)+30, os.SEEK_SET) // 30 is the header size
	require.NoError(t, err, "Failed to seek to data position")

	// Read the data
	data := make([]byte, len(rec.Data))
	_, err = file.Read(data)
	require.NoError(t, err, "Failed to read data")

	// Flip a bit in the data
	data[0] ^= 0x01

	// Write the corrupted data back
	_, err = file.Seek(lsn.calculateOffset(wal.WalOptions{}.MaxSegmentSize)+30, os.SEEK_SET)
	require.NoError(t, err, "Failed to seek to data position")
	_, err = file.Write(data)
	require.NoError(t, err, "Failed to write corrupted data")

	// Attempt to read the corrupted record
	reader := w.NewReader()
	defer reader.Close()

	err = reader.Seek(0)
	require.NoError(t, err, "Failed to seek to start")

	_, err = reader.Next()
	assert.Error(t, err, "Expected an error due to data corruption")
}

// TestWalSkipRecords tests reading records with filters (e.g., based on record type) to ensure the WAL reader correctly skips records that do not match the filter criteria.
func TestWalSkipRecords(t *testing.T) {
	testDir := t.TempDir()

	opts := wal.WalOptions{
		Path:           testDir,
		MaxSegmentSize: 1024,
	}

	w, err := wal.Create(opts)
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
		assert.NoError(t, err, "Failed to write record")
	}

	// Test skipping records based on filters
	reader := w.NewReader().WithType(wal.RecordTypeInsert)
	defer reader.Close()

	err = reader.Seek(0)
	require.NoError(t, err, "Failed to seek to start")

	for i := 0; i < 10; i++ {
		rec, err := reader.Next()
		if i%3 == 0 {
			assert.NoError(t, err, "Failed to read record")
			assert.Equal(t, wal.RecordTypeInsert, rec.Type, "Record type mismatch")
		} else {
			assert.Error(t, err, "Expected an error due to record type mismatch")
		}
	}
}

// TestWALCloseTimeout tests that the WAL can close within a reasonable time even if it encounters an unexpected state.
func TestWALCloseTimeout(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Simulate an unexpected state by writing an out-of-order transaction
	rec := &wal.Record{
		Type:   wal.RecordTypeInsert,
		Entity: 1,
		TxID:   2, // Out-of-order TxID
		Data:   []byte("test data"),
	}
	_, err := w.Write(rec)
	require.NoError(t, err, "Failed to write out-of-order record")

	// Attempt to close the WAL and ensure it does not block indefinitely
	done := make(chan struct{})
	go func() {
		err = w.Close()
		close(done)
	}()

	select {
	case <-done:
		require.NoError(t, err, "Failed to close WAL")
	case <-time.After(5 * time.Second):
		t.Fatal("WAL close timed out")
	}
}

// Helper function to validate first and last index
func testFirstLast(t *testing.T, w *wal.Wal, expectFirst, expectLast uint64) {
	t.Helper()
	fi, err := w.FirstIndex()
	require.NoError(t, err, "Failed to get first index")
	assert.Equal(t, expectFirst, fi, "First index mismatch")

	li, err := w.LastIndex()
	require.NoError(t, err, "Failed to get last index")
	assert.Equal(t, expectLast, li, "Last index mismatch")
}

// TestWalTruncateFront tests truncating the front of the WAL and ensures that old entries are correctly removed.
func TestWalTruncateFront(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Write records
	for i := 1; i <= 100; i++ {
		rec := &wal.Record{
			Type:   wal.RecordTypeInsert,
			Entity: uint64(i),
			TxID:   uint64(i),
			Data:   []byte(fmt.Sprintf("data-%d", i)),
		}
		_, err := w.Write(rec)
		require.NoError(t, err, "Failed to write record")
	}

	// Truncate front and validate
	err := w.TruncateFront(50)
	require.NoError(t, err, "Failed to truncate front")
	testFirstLast(t, w, 50, 100)

	// Reopen and validate
	w.Close()
	w, err = wal.Open(0, wal.WalOptions{Path: testDir, MaxSegmentSize: 1024})
	require.NoError(t, err, "Failed to reopen WAL")
	defer w.Close()
	testFirstLast(t, w, 50, 100)
}

// TestWalTruncateBack tests truncating the back of the WAL and ensures that old entries are correctly removed.
func TestWalTruncateBack(t *testing.T) {
	testDir := t.TempDir()

	w := createWal(t, testDir)
	defer w.Close()

	// Write records
	for i := 1; i <= 100; i++ {
		rec := &wal.Record{
			Type:   wal.RecordTypeInsert,
			Entity: uint64(i),
			TxID:   uint64(i),
			Data:   []byte(fmt.Sprintf("data-%d", i)),
		}
		_, err := w.Write(rec)
		require.NoError(t, err, "Failed to write record")
	}

	// Truncate back and validate
	err := w.TruncateBack(50)
	require.NoError(t, err, "Failed to truncate back")
	testFirstLast(t, w, 1, 50)

	// Reopen and validate
	w.Close()
	w, err = wal.Open(0, wal.WalOptions{Path: testDir, MaxSegmentSize: 1024})
	require.NoError(t, err, "Failed to reopen WAL")
	defer w.Close()
	testFirstLast(t, w, 1, 50)
}