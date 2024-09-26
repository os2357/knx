// wal.go

package wal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"math"
)

const (
	headerSize = 28
)

type Segment struct {
	id       uint64
	file     *os.File
	position int64
	maxSize  int64
	mu       sync.RWMutex
}

type Wal struct {
	opts           WalOptions
	segments       []*Segment
	active         *Segment
	mu             sync.RWMutex
	bufferPool     *sync.Pool
	lastCheckpoint LSN
	metrics        *WalMetrics
	logger         *log.Logger
	encryptionKey  []byte
	schemaRegistry map[uint64]*Schema
}

type WalOptions struct {
	Path           string
	MaxSegmentSize int64
	MaxRecordSize  int
	BufferSize     int
	SyncInterval   time.Duration
	EncryptionKey  []byte
}

type WalMetrics struct {
	TotalWrites       int64
	TotalBytesWritten int64
	AvgWriteLatency   time.Duration
}

type Field struct {
	Name string
	Type string
}

type Schema struct {
	Fields []Field
}

type Checkpoint struct {
	LSN       LSN
	Timestamp int64
}

func Open(opts WalOptions) (*Wal, error) {
	w := &Wal{
		opts: opts,
		bufferPool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, opts.BufferSize)
			},
		},
		metrics: &WalMetrics{},
		logger:  log.New(os.Stderr, "WAL: ", log.LstdFlags),
		schemaRegistry: make(map[uint64]*Schema),
	}

	if err := w.initSegments(); err != nil {
		return nil, err
	}

	if err := w.recover(); err != nil {
		return nil, err
	}

	if len(opts.EncryptionKey) > 0 {
		w.encryptionKey = opts.EncryptionKey
	}

	go w.periodicSync()

	return w, nil
}

func (w *Wal) initSegments() error {
	files, err := filepath.Glob(filepath.Join(w.opts.Path, "*.wal"))
	if err != nil {
		return fmt.Errorf("failed to list WAL segments: %w", err)
	}

	sort.Strings(files)

	for _, file := range files {
		segment, err := w.openSegment(file)
		if err != nil {
			return fmt.Errorf("failed to open segment %s: %w", file, err)
		}
		w.segments = append(w.segments, segment)
	}

	if len(w.segments) == 0 {
		segment, err := w.createNewSegment(0)
		if err != nil {
			return fmt.Errorf("failed to create initial segment: %w", err)
		}
		w.segments = append(w.segments, segment)
	}

	w.active = w.segments[len(w.segments)-1]
	return nil
}

func (w *Wal) openSegment(path string) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	id, err := parseSegmentID(filepath.Base(path))
	if err != nil {
		return nil, err
	}

	return &Segment{
		id:       id,
		file:     file,
		position: info.Size(),
		maxSize:  w.opts.MaxSegmentSize,
	}, nil
}

func (w *Wal) createNewSegment(id uint64) (*Segment, error) {
	path := filepath.Join(w.opts.Path, fmt.Sprintf("%016x.wal", id))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}

	return &Segment{
		id:       id,
		file:     file,
		position: 0,
		maxSize:  w.opts.MaxSegmentSize,
	}, nil
}

func (w *Wal) Write(rec *Record) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	startTime := time.Now()

	lsn := w.calculateLSN()
	
	if err := w.ensureCapacity(int64(rec.Size())); err != nil {
		return 0, fmt.Errorf("failed to ensure capacity: %w", err)
	}

	if err := w.writeRecordToSegment(rec); err != nil {
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	w.updateMetrics(startTime, int64(rec.Size()))

	return lsn, nil
}

func (w *Wal) calculateLSN() LSN {
	return LSN(w.active.id)<<32 | LSN(w.active.position)
}

func (w *Wal) ensureCapacity(size int64) error {
	if w.active.position+size > w.active.maxSize {
		if err := w.rolloverSegment(); err != nil {
			return fmt.Errorf("failed to rollover segment: %w", err)
		}
	}
	return nil
}

func (w *Wal) rolloverSegment() error {
	if err := w.active.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync active segment: %w", err)
	}

	newSegment, err := w.createNewSegment(w.active.id + 1)
	if err != nil {
		return fmt.Errorf("failed to create new segment: %w", err)
	}

	w.segments = append(w.segments, newSegment)
	w.active = newSegment

	return nil
}

func (w *Wal) writeRecordToSegment(rec *Record) error {
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint64(header[0:8], uint64(rec.Type))
	binary.LittleEndian.PutUint64(header[8:16], rec.Entity)
	binary.LittleEndian.PutUint64(header[16:24], rec.TxID)
	binary.LittleEndian.PutUint32(header[24:28], uint32(len(rec.Data)))

	data := append(header, rec.Data...)

	if w.encryptionKey != nil {
		var err error
		data, err = w.encrypt(data)
		if err != nil {
			return fmt.Errorf("failed to encrypt record: %w", err)
		}
	}

	w.active.mu.Lock()
	defer w.active.mu.Unlock()

	if _, err := w.active.file.Write(data); err != nil {
		return fmt.Errorf("failed to write to segment: %w", err)
	}
	w.active.position += int64(len(data))

	return nil
}

func (r *Record) Size() int {
	return headerSize + len(r.Data)
}

func (w *Wal) NewReader() WalReader {
	return NewReader(w)
}

func parseSegmentID(name string) (uint64, error) {
	return strconv.ParseUint(name[:16], 16, 64)
}

func (w *Wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, segment := range w.segments {
		if err := segment.file.Close(); err != nil {
			return fmt.Errorf("failed to close segment: %w", err)
		}
	}

	return nil
}

func (w *Wal) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.active.file.Sync()
}

func (w *Wal) Compact(upToLSN LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	segmentID := uint64(upToLSN >> 32)
	offset := uint64(upToLSN & 0xFFFFFFFF)

	var segmentsToRemove []*Segment
	for _, segment := range w.segments {
		if segment.id < segmentID || (segment.id == segmentID && int64(offset) >= segment.position) {
			segmentsToRemove = append(segmentsToRemove, segment)
		} else {
			break
		}
	}

	for _, segment := range segmentsToRemove {
		if err := segment.file.Close(); err != nil {
			return fmt.Errorf("failed to close segment during compaction: %w", err)
		}
		if err := os.Remove(segment.file.Name()); err != nil {
			return fmt.Errorf("failed to remove segment file during compaction: %w", err)
		}
	}

	w.segments = w.segments[len(segmentsToRemove):]

	return nil
}

func (w *Wal) periodicSync() {
	ticker := time.NewTicker(w.opts.SyncInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := w.Sync(); err != nil {
			w.logger.Printf("Error during periodic sync: %v", err)
		}
	}
}

func (w *Wal) updateMetrics(startTime time.Time, bytesWritten int64) {
	atomic.AddInt64(&w.metrics.TotalWrites, 1)
	atomic.AddInt64(&w.metrics.TotalBytesWritten, bytesWritten)
	writeLatency := time.Since(startTime)
	atomic.StoreInt64((*int64)(&w.metrics.AvgWriteLatency), int64(writeLatency))
}

func (w *Wal) GetMetrics() WalMetrics {
	return WalMetrics{
		TotalWrites:       atomic.LoadInt64(&w.metrics.TotalWrites),
		TotalBytesWritten: atomic.LoadInt64(&w.metrics.TotalBytesWritten),
		AvgWriteLatency:   time.Duration(atomic.LoadInt64((*int64)(&w.metrics.AvgWriteLatency))),
	}
}

func (w *Wal) encrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(w.encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, data, nil), nil
}

func (w *Wal) decrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(w.encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}

func (w *Wal) recover() error {
	reader := w.NewReader()
	defer reader.Close()

	lastLSN := w.lastCheckpoint
	err := reader.Seek(lastLSN)
	if err != nil {
		return fmt.Errorf("failed to seek to last checkpoint: %w", err)
	}

	for {
		_, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error during recovery: %w", err)
		}
	}

	w.lastCheckpoint = lastLSN
	return nil
}

func (w *Wal) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	checkpointData := struct {
		LSN       LSN
		Timestamp int64
	}{
		LSN:       w.calculateLSN(),
		Timestamp: time.Now().UnixNano(),
	}

	data, err := json.Marshal(checkpointData)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint data: %w", err)
	}

	checkpointFile := filepath.Join(w.opts.Path, "checkpoint")
	if err := os.WriteFile(checkpointFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	w.lastCheckpoint = checkpointData.LSN
	return nil
}

func decodeField(data []byte, field *Field) (interface{}, int, error) {
	switch field.Type {
	case "uint64":
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for uint64")
		}
		return binary.LittleEndian.Uint64(data), 8, nil
	case "int64":
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for int64")
		}
		return int64(binary.LittleEndian.Uint64(data)), 8, nil
	case "float64":
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for float64")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data)), 8, nil
	case "string":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for string length")
		}
		length := int(binary.LittleEndian.Uint32(data))
		if len(data) < 4+length {
			return nil, 0, fmt.Errorf("insufficient data for string content")
		}
		return string(data[4 : 4+length]), 4 + length, nil
	case "bytes":
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
	schema, ok := w.schemaRegistry[entityID]
	if !ok {
		return nil, fmt.Errorf("schema not found for entity %d", entityID)
	}
	return schema, nil
}
