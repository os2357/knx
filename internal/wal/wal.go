// wal.go

package wal

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gofrs/flock"
	"blockwatch.cc/knoxdb/internal/types"
)

var (
	// Error definitions
	ErrCorruptRecord     = errors.New("corrupt record")
	ErrIncompleteRecord  = errors.New("incomplete record")
	ErrChecksumMismatch  = errors.New("checksum mismatch")
	ErrTransactionNotFound = errors.New("transaction not found")
	ErrTransactionNotActive = errors.New("transaction is not active")
)

type WalOptions struct {
    Path           string
    MaxSegmentSize int64
    MaxRecordSize  int
    BufferSize     int
    EncryptionKey  []byte
    Seed           uint64
}

type Checkpoint struct {
	LSN            LSN
	LastCommitTxID uint64
	Timestamp      int64
}

type Wal struct {
	opts           WalOptions
	active         *segment
	csum           uint64
	hash           *xxhash.Digest
	checkpoint     Checkpoint
	mu             sync.RWMutex
	schemaRegistry *SchemaRegistry
	packEngine     PackEngine
	kvEngine       KVEngine
	txManager      *TransactionManager
	writeBuffer    *bufio.Writer
	metrics        *WalMetrics
	logger         *log.Logger
	encryptionIV   []byte
}

type WalMetrics struct {
	TotalWrites       int64
	TotalBytesWritten int64
	AvgWriteLatency   time.Duration
}

// PackEngine defines the interface for pack-based storage operations
type PackEngine interface {
	InsertOrUpdate(entityID uint64, values map[string]interface{}) error
	Delete(entityID uint64, pk interface{}) error
}

// KVEngine defines the interface for key-value based storage operations
type KVEngine interface {
	InsertOrUpdate(entityID uint64, values map[string]interface{}) error
	Delete(entityID uint64, pk interface{}) error
}

type TransactionManager struct {
    // Add necessary fields
}

func NewTransactionManager() *TransactionManager {
    return &TransactionManager{}
}

type WalReader interface {
    Next() (*Record, error)
    Close() error
    WithType(t RecordType) WalReader
    WithTag(tag types.ObjectTag) WalReader
    WithEntity(entity uint64) WalReader
    WithTxID(txID uint64) WalReader
}

func Create(opts WalOptions) (*Wal, error) {
	err := os.MkdirAll(opts.Path, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	segmentPath := filepath.Join(opts.Path, "00000000.wal")
	file, err := os.OpenFile(segmentPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create initial WAL segment: %w", err)
	}

	segment := &segment{
		id:   0,
		file: file,
		pos:  0,
	}

	wal := &Wal{
		opts:           opts,
		active:         segment,
		csum:           opts.Seed,
		hash:           xxhash.New(),
		schemaRegistry: NewSchemaRegistry(),
		txManager:      NewTransactionManager(),
		writeBuffer:    bufio.NewWriterSize(file, opts.BufferSize),
		metrics:        &WalMetrics{},
		logger:         log.New(os.Stderr, "WAL: ", log.LstdFlags),
	}

	if len(opts.EncryptionKey) > 0 {
		wal.encryptionIV = make([]byte, aes.BlockSize)
		if _, err := rand.Read(wal.encryptionIV); err != nil {
			return nil, fmt.Errorf("failed to generate encryption IV: %w", err)
		}
	}

	return wal, nil
}

func Open(opts WalOptions) (*Wal, error) {
    w := &Wal{
        opts: opts,
        // ... other initializations ...
    }

    // Acquire the lock
    if err := w.acquireLock(); err != nil {
        return nil, fmt.Errorf("failed to acquire lock: %w", err)
    }

    // If Seed is not provided, use current time
    if opts.Seed == 0 {
        opts.Seed = uint64(time.Now().UnixNano())
    }

    var initErr error
    defer func() {
        if initErr != nil {
            w.releaseLock()
        }
    }()

    // Initialize segments
    if initErr = w.initSegments(); initErr != nil {
        return nil, fmt.Errorf("failed to initialize segments: %w", initErr)
    }

    // ... other initialization steps ...

    return w, nil
}

func (w *Wal) acquireLock() error {
    // Implementation of lock acquisition
}

func (w *Wal) releaseLock() {
    // Implementation of lock release
}

func (w *Wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writeBuffer.Flush(); err != nil {
		return fmt.Errorf("failed to flush write buffer: %w", err)
	}

	if err := w.active.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync active segment: %w", err)
	}

	if err := w.active.file.Close(); err != nil {
		return fmt.Errorf("failed to close active segment: %w", err)
	}

	return nil
}

func (w *Wal) Write(rec *Record) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	startTime := time.Now()

	if w.active.pos+int64(len(rec.Data)+28) > int64(w.opts.MaxSegmentSize) {
		if err := w.nextSegment(); err != nil {
			return 0, fmt.Errorf("failed to switch to next segment: %w", err)
		}
	}

	lsn := LSN(w.active.id)<<32 | LSN(w.active.pos)

	var head [28]byte
	head[0] = byte(rec.Type)
	head[1] = byte(rec.Tag)
	binary.LittleEndian.PutUint64(head[2:], rec.Entity)
	binary.LittleEndian.PutUint64(head[10:], rec.TxID)
	binary.LittleEndian.PutUint32(head[18:], uint32(len(rec.Data)))

	w.hash.Reset()
	w.hash.Write(head[:20])
	w.hash.Write(rec.Data)
	binary.LittleEndian.PutUint64(head[20:], w.hash.Sum64())

	if len(w.opts.EncryptionKey) > 0 {
		encryptedData, err := w.encrypt(append(head[:], rec.Data...))
		if err != nil {
			return 0, fmt.Errorf("failed to encrypt record: %w", err)
		}
		if _, err := w.writeBuffer.Write(encryptedData); err != nil {
			return 0, fmt.Errorf("failed to write encrypted record: %w", err)
		}
	} else {
		if _, err := w.writeBuffer.Write(head[:]); err != nil {
			return 0, fmt.Errorf("failed to write record header: %w", err)
		}
		if _, err := w.writeBuffer.Write(rec.Data); err != nil {
			return 0, fmt.Errorf("failed to write record data: %w", err)
		}
	}

	w.active.pos += int64(len(head) + len(rec.Data))

	// Update metrics
	atomic.AddInt64(&w.metrics.TotalWrites, 1)
	atomic.AddInt64(&w.metrics.TotalBytesWritten, int64(len(head)+len(rec.Data)))
	writeLatency := time.Since(startTime)
	atomic.StoreInt64((*int64)(&w.metrics.AvgWriteLatency), int64(writeLatency))

	return lsn, nil
}

func (w *Wal) encrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(w.opts.EncryptionKey)
	if err != nil {
		return nil, err
	}
	
	encryptedData := make([]byte, len(data))
	stream := cipher.NewCFBEncrypter(block, w.encryptionIV)
	stream.XORKeyStream(encryptedData, data)
	
	return encryptedData, nil
}

func (w *Wal) decrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(w.opts.EncryptionKey)
	if err != nil {
		return nil, err
	}
	
	decryptedData := make([]byte, len(data))
	stream := cipher.NewCFBDecrypter(block, w.encryptionIV)
	stream.XORKeyStream(decryptedData, data)
	
	return decryptedData, nil
}

func (w *Wal) nextSegment() error {
	if err := w.writeBuffer.Flush(); err != nil {
		return fmt.Errorf("failed to flush write buffer: %w", err)
	}

	if err := w.padSegment(w.active); err != nil {
		return fmt.Errorf("failed to pad current segment: %w", err)
	}

	if err := w.active.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync current segment: %w", err)
	}

	if err := w.active.file.Close(); err != nil {
		return fmt.Errorf("failed to close current segment: %w", err)
	}

	newID := w.active.id + 1
	newSegmentName := fmt.Sprintf("%08x.wal", newID)
	newSegmentPath := filepath.Join(w.opts.Path, newSegmentName)
	
	file, err := os.OpenFile(newSegmentPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new segment file: %w", err)
	}

	w.active = &segment{
		id:   newID,
		file: file,
		pos:  0,
	}
	w.writeBuffer.Reset(file)

	return nil
}

func (w *Wal) padSegment(seg *segment) error {
    // Implementation here
    return nil
}

func (w *Wal) Compact() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Identify obsolete segments
	segments, err := listSegments(w.opts.Path)
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}

	if len(segments) <= 1 {
		return nil // No compaction needed
	}

	// Determine the oldest segment we need to keep based on the last checkpoint
	oldestRequiredLSN := w.checkpoint.LSN

	// 2. Create a new compacted segment
	compactedSegmentID := w.active.id + 1
	compactedSegmentPath := filepath.Join(w.opts.Path, fmt.Sprintf("%08x.wal", compactedSegmentID))
	compactedFile, err := os.OpenFile(compactedSegmentPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("failed to create compacted segment: %w", err)
	}
	defer compactedFile.Close()

	compactedWriter := bufio.NewWriter(compactedFile)

	// 3. Copy relevant records to the new segment
	var lastCsum uint64
	for _, segmentName := range segments {
		segmentID, err := parseSegmentID(segmentName)
		if err != nil {
			return fmt.Errorf("failed to parse segment ID: %w", err)
		}

		if LSN(segmentID)<<32 >= oldestRequiredLSN {
			// This segment contains records we need to keep
			segmentPath := filepath.Join(w.opts.Path, segmentName)
			segmentFile, err := os.Open(segmentPath)
			if err != nil {
				return fmt.Errorf("failed to open segment file: %w", err)
			}
			defer segmentFile.Close()

			reader := bufio.NewReader(segmentFile)
			for {
				record, err := w.readRecord(reader, lastCsum)
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("failed to read record: %w", err)
				}

				// Write the record to the compacted segment
				if err := w.writeRecordToFile(compactedWriter, record); err != nil {
					return fmt.Errorf("failed to write record to compacted segment: %w", err)
				}

				lastCsum = binary.LittleEndian.Uint64(record.Data[len(record.Data)-8:])
			}
		}
	}

	if err := compactedWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush compacted segment: %w", err)
	}

	// 4. Update references and delete old segments
	for _, segmentName := range segments {
		segmentID, _ := parseSegmentID(segmentName)
		if LSN(segmentID)<<32 < oldestRequiredLSN {
			segmentPath := filepath.Join(w.opts.Path, segmentName)
			if err := os.Remove(segmentPath); err != nil {
				return fmt.Errorf("failed to remove old segment: %w", err)
			}
		}
	}

	// 5. Update checkpoints
	w.checkpoint.LSN = LSN(compactedSegmentID) << 32
	if err := w.writeCheckpoint(w.checkpoint); err != nil {
		return fmt.Errorf("failed to update checkpoint: %w", err)
	}

	// Switch to the new compacted segment
	return w.switchToCompactedSegment(compactedSegmentID)
}

func (w *Wal) switchToCompactedSegment(compactedSegmentID uint64) error {
	compactedSegmentPath := filepath.Join(w.opts.Path, fmt.Sprintf("%08x.wal", compactedSegmentID))
	compactedFile, err := os.OpenFile(compactedSegmentPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open compacted segment: %w", err)
	}

	// Close the current active segment
	if err := w.active.file.Close(); err != nil {
		compactedFile.Close()
		return fmt.Errorf("failed to close current active segment: %w", err)
	}

	// Switch to the new compacted segment
	w.active = &segment{
		id:   compactedSegmentID,
		file: compactedFile,
		pos:  0,
	}

	return nil
}

func (w *Wal) writeRecordToFile(writer *bufio.Writer, record *Record) error {
	var head [28]byte
	head[0] = byte(record.Type)
	head[1] = byte(record.Tag)
	binary.LittleEndian.PutUint64(head[2:], record.Entity)
	binary.LittleEndian.PutUint64(head[10:], record.TxID)
	binary.LittleEndian.PutUint32(head[18:], uint32(len(record.Data)))

	w.hash.Reset()
	w.hash.Write(head[:20])
	w.hash.Write(record.Data)
	binary.LittleEndian.PutUint64(head[20:], w.hash.Sum64())

	if _, err := writer.Write(head[:]); err != nil {
		return fmt.Errorf("failed to write record header: %w", err)
	}
	if _, err := writer.Write(record.Data); err != nil {
		return fmt.Errorf("failed to write record data: %w", err)
	}

	return nil
}

func (w *Wal) Recover() error {
	checkpoint, err := w.readCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to read checkpoint: %w", err)
	}

	if checkpoint.LSN == 0 {
		return w.recoverFromBeginning()
	}

	segments, err := listSegments(w.opts.Path)
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}

	checkpointSegmentID := uint64(checkpoint.LSN >> 32)
	checkpointOffset := uint64(checkpoint.LSN & 0xFFFFFFFF)

	var startSegmentIndex int
	for i, segment := range segments {
		segmentID, err := parseSegmentID(segment)
		if err != nil {
			return fmt.Errorf("failed to parse segment ID: %w", err)
		}
		if segmentID == checkpointSegmentID {
			startSegmentIndex = i
			break
		}
	}

	for i := startSegmentIndex; i < len(segments); i++ {
		segmentPath := filepath.Join(w.opts.Path, segments[i])
		err := w.replaySegment(segmentPath, checkpointOffset)
		if err != nil {
			return fmt.Errorf("failed to replay segment %s: %w", segments[i], err)
		}
		checkpointOffset = 0
	}

	return nil
}

func (w *Wal) recoverFromBeginning() error {
	segments, err := listSegments(w.opts.Path)
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}

	for _, segment := range segments {
		segmentPath := filepath.Join(w.opts.Path, segment)
		err := w.replaySegment(segmentPath, 0)
		if err != nil {
			return fmt.Errorf("failed to replay segment %s: %w", segment, err)
		}
	}

	return nil
}

func (w *Wal) replaySegment(segmentPath string, startOffset uint64) error {
	file, err := os.Open(segmentPath)
	if err != nil {
		return fmt.Errorf("failed to open segment file: %w", err)
	}
	defer file.Close()

	if startOffset > 0 {
		_, err = file.Seek(int64(startOffset), io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek to start offset: %w", err)
		}
	}

	reader := bufio.NewReader(file)
	var prevCsum uint64

	for {
		record, err := w.readRecord(reader, prevCsum)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read record: %w", err)
		}

		err = w.applyRecord(record)
		if err != nil {
			return fmt.Errorf("failed to apply record: %w", err)
		}

		prevCsum = binary.LittleEndian.Uint64(record.Data[len(record.Data)-8:])
	}

	return nil
}

func (w *Wal) readRecord(reader *bufio.Reader, prevCsum uint64) (*Record, error) {
	var header [28]byte
	_, err := io.ReadFull(reader, header[:])
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrIncompleteRecord, err)
	}

	rec := &Record{}
	rec.Type = RecordType(header[0])
	rec.Tag = types.ObjectTag(header[1])
	rec.Entity = binary.LittleEndian.Uint64(header[2:])
	rec.TxID = binary.LittleEndian.Uint64(header[10:])
	dataLen := binary.LittleEndian.Uint32(header[16:])

	if dataLen > uint32(w.opts.MaxRecordSize) {
		return nil, fmt.Errorf("%w: record size %d exceeds maximum allowed size %d", ErrCorruptRecord, dataLen, w.opts.MaxRecordSize)
	}

	rec.Data = make([]byte, dataLen)
	_, err = io.ReadFull(reader, rec.Data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIncompleteRecord, err)
	}

	if err := w.verifyChecksum(rec, prevCsum); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrChecksumMismatch, err)
	}

	return rec, nil
}

func (w *Wal) verifyChecksum(rec *Record, prevCsum uint64) error {
	w.hash.Reset()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], prevCsum)
	w.hash.Write(b[:])
	w.hash.Write(rec.Data[:len(rec.Data)-8])
	calculatedCsum := w.hash.Sum64()

	recordCsum := binary.LittleEndian.Uint64(rec.Data[len(rec.Data)-8:])
	if calculatedCsum != recordCsum {
		return ErrChecksumMismatch
	}
	return nil
}

func (w *Wal) seekNextRecord(reader *bufio.Reader) error {
	for {
		header, err := reader.Peek(28)
		if err != nil {
			if err == io.EOF {
				return err
			}
			_, _ = reader.Discard(1)
			continue
		}

		if w.isValidHeader(header) {
			return nil
		}

		_, _ = reader.Discard(1)
	}
}

func (w *Wal) isValidHeader(header []byte) bool {
	recordType := RecordType(header[0])
	dataLen := binary.LittleEndian.Uint32(header[16:])
	return recordType < RecordTypeMax && dataLen <= uint32(w.opts.MaxRecordSize)
}

func (w *Wal) applyRecord(record *Record) error {
	switch record.Type {
	case RecordTypeInsert, RecordTypeUpdate:
		return w.applyInsertOrUpdate(record)
	case RecordTypeDelete:
		return w.applyDelete(record)
	default:
		return fmt.Errorf("unknown record type: %d", record.Type)
	}
}

func (w *Wal) applyInsertOrUpdate(record *Record) error {
	schema, err := w.schemaRegistry.GetSchema(record.Entity)
	if err != nil {
		return fmt.Errorf("failed to get schema for entity %d: %w", record.Entity, err)
	}

	values, err := decodeRecord(record.Data, schema)
	if err != nil {
		return fmt.Errorf("failed to decode record: %w", err)
	}

	if schema.IsPackBased() {
		return w.packEngine.InsertOrUpdate(record.Entity, values)
	} else {
		return w.kvEngine.InsertOrUpdate(record.Entity, values)
	}
}

func (w *Wal) applyDelete(record *Record) error {
	schema, err := w.schemaRegistry.GetSchema(record.Entity)
	if err != nil {
		return fmt.Errorf("failed to get schema for entity %d: %w", record.Entity, err)
	}

	pk, err := decodePrimaryKey(record.Data, schema)
	if err != nil {
		return fmt.Errorf("failed to decode primary key: %w", err)
	}

	if schema.IsPackBased() {
		return w.packEngine.Delete(record.Entity, pk)
	} else {
		return w.kvEngine.Delete(record.Entity, pk)
	}
}

func listSegments(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segments []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".wal") {
			segments = append(segments, file.Name())
		}
	}

	sort.Strings(segments)
	return segments, nil
}

func parseSegmentID(name string) (uint64, error) {
	base := filepath.Base(name)
	ext := filepath.Ext(base)
	idStr := base[:len(base)-len(ext)]
	return strconv.ParseUint(idStr, 16, 64)
}

// Schema-related code

type FieldType int

const (
	TypeUint64 FieldType = iota
	TypeInt64
	TypeBytes
	TypeString
	TypeFloat64
)

type Field struct {
	Name        string
	Type        FieldType
	IsPK        bool
	IsIndexed   bool
	IndexType   string
	Compression string
}

type Schema struct {
	Name   string
	Fields []*Field
}

func (s *Schema) PrimaryKeyField() *Field {
	for _, f := range s.Fields {
		if f.IsPK {
			return f
		}
	}
	return nil
}

func (s *Schema) IsPackBased() bool {
	// Implement logic to determine if this schema is pack-based or KV-based
	return true // Placeholder implementation
}

type SchemaRegistry struct {
	schemas map[uint64]*Schema
	mu      sync.RWMutex
}

func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas: make(map[uint64]*Schema),
	}
}

func (r *SchemaRegistry) RegisterSchema(entityID uint64, schema *Schema) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[entityID] = schema
}

func (r *SchemaRegistry) GetSchema(entityID uint64) (*Schema, error) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    schema, ok := r.schemas[entityID]
    if !ok {
        return nil, fmt.Errorf("schema not found for entity %d", entityID)
    }
    return schema, nil
}

func decodeRecord(data []byte, schema *Schema) (map[string]interface{}, error) {
	values := make(map[string]interface{})
	offset := 0

	for _, field := range schema.Fields {
		value, n, err := decodeField(data[offset:], field)
		if err != nil {
			return nil, fmt.Errorf("failed to decode field %s: %w", field.Name, err)
		}
		values[field.Name] = value
		offset += n
	}

	return values, nil
}

func decodeField(data []byte, field *Field) (interface{}, int, error) {
	switch field.Type {
	case TypeUint64:
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for uint64")
		}
		return binary.LittleEndian.Uint64(data), 8, nil
	case TypeInt64:
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for int64")
		}
		return int64(binary.LittleEndian.Uint64(data)), 8, nil
	case TypeFloat64:
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for float64")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data)), 8, nil
	case TypeString:
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for string length")
		}
		length := int(binary.LittleEndian.Uint32(data))
		if len(data) < 4+length {
			return nil, 0, fmt.Errorf("insufficient data for string content")
		}
		return string(data[4 : 4+length]), 4 + length, nil
	case TypeBytes:
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for bytes length")
		}
		length := int(binary.LittleEndian.Uint32(data))
		if len(data) < 4+length {
			return nil, 0, fmt.Errorf("insufficient data for bytes content")
		}
		return data[4 : 4+length], 4 + length, nil
	default:
		return nil, 0, fmt.Errorf("unknown field type")
	}
}

func decodePrimaryKey(data []byte, schema *Schema) (interface{}, error) {
    // Implementation here
    return nil, nil
}

func readLastChecksum(file *os.File) (uint64, error) {
    // Implement the logic to read the last checksum from the file
    // This is a placeholder implementation
    return 0, nil
}

func (w *Wal) readCheckpoint() (Checkpoint, error) {
    checkpointPath := filepath.Join(w.opts.Path, "checkpoint")
    file, err := os.Open(checkpointPath)
    if err != nil {
        if os.IsNotExist(err) {
            return Checkpoint{}, nil
        }
        return Checkpoint{}, fmt.Errorf("failed to open checkpoint file: %w", err)
    }
    defer file.Close()

    var checkpoint Checkpoint
    decoder := json.NewDecoder(file)
    if err := decoder.Decode(&checkpoint); err != nil {
        return Checkpoint{}, fmt.Errorf("failed to decode checkpoint: %w", err)
    }

    return checkpoint, nil
}

func (w *Wal) writeCheckpoint(checkpoint Checkpoint) error {
    checkpointPath := filepath.Join(w.opts.Path, "checkpoint")
    file, err := os.Create(checkpointPath)
    if err != nil {
        return fmt.Errorf("failed to create checkpoint file: %w", err)
    }
    defer file.Close()

    encoder := json.NewEncoder(file)
    if err := encoder.Encode(checkpoint); err != nil {
        return fmt.Errorf("failed to encode checkpoint: %w", err)
    }

    return nil
}

func (w *Wal) GetSchema(entityID uint64) (*Schema, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	schema, ok := w.schemaRegistry.schemas[entityID]
	if !ok {
		return nil, fmt.Errorf("schema not found for entity %d", entityID)
	}
	return schema, nil
}


