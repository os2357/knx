// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"blockwatch.cc/knoxdb/internal/types"
	"fmt"
	"io"
	"encoding/binary"
)

type RecordFilter struct {
	Type   RecordType
	Tag    types.ObjectTag
	Entity uint64
	TxID   uint64
}

type Schema struct {
    Fields []Field
    // Add other necessary fields
}

func (f *RecordFilter) Match(r *Record) bool {
	if f == nil {
		return true
	}
	if f.Type.IsValid() && r.Type != f.Type {
		return false
	}
	if f.Tag.IsValid() && r.Tag != f.Tag {
		return false
	}
	if f.Entity > 0 && r.Entity != f.Entity {
		return false
	}
	if f.TxID > 0 && r.TxID != f.TxID {
		return false
	}
	return true
}

var _ WalReader = (*Reader)(nil)

type Reader struct {
	wal            *Wal
	currentSegment *Segment
	currentSegmentIndex int
	lsn            LSN
	buffer         []byte
	bufferPos      int
}

func NewReader(w *Wal) WalReader {
	return &Reader{
		wal:    w,
		buffer: make([]byte, w.opts.BufferSize),
	}
}

func (r *Reader) Seek(lsn LSN) error {
	debugPrint(3, "Seeking to LSN: %d\n", lsn)
	segmentID := uint64(lsn >> 32)
	offset := uint64(lsn & 0xFFFFFFFF)

	// If seeking to LSN 0, use the first segment
	if lsn == 0 {
		if len(r.wal.segments) == 0 {
			return fmt.Errorf("no segments available")
		}
		r.currentSegmentIndex = 0
		r.currentSegment = r.wal.segments[0]
		_, err := r.currentSegment.file.Seek(0, io.SeekStart)
		return err
	}

	for i, segment := range r.wal.segments {
		if segment.id == segmentID {
			r.currentSegmentIndex = i
			r.currentSegment = segment
			_, err := r.currentSegment.file.Seek(int64(offset), io.SeekStart)
			if err != nil {
				return fmt.Errorf("failed to seek within segment: %w", err)
			}
			fmt.Println("Seek completed")
			return nil
		}
	}

	return fmt.Errorf("segment not found for LSN: %d", lsn)
}

func (r *Reader) Next() (*Record, error) {
	fmt.Println("Reading next record")
	
	if r.currentSegment == nil {
		fmt.Println("No current segment, initializing")
		if err := r.initializeReader(); err != nil {
			return nil, err
		}
	}

	record, err := r.readNextRecord()
	if err == io.EOF {
		fmt.Println("Reached end of current segment")
		// Try to move to the next segment
		if r.currentSegmentIndex < len(r.wal.segments)-1 {
			r.currentSegmentIndex++
			r.currentSegment = r.wal.segments[r.currentSegmentIndex]
			r.currentSegment.file.Seek(0, io.SeekStart)
			return r.Next()
		}
		fmt.Println("No more records, returning EOF")
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read next record: %w", err)
	}

	return record, nil
}

func (r *Reader) initializeReader() error {
	if len(r.wal.segments) == 0 {
		return io.EOF
	}
	r.currentSegmentIndex = 0
	r.currentSegment = r.wal.segments[0]
	_, err := r.currentSegment.file.Seek(0, io.SeekStart)
	return err
}

// You might need to implement this method if it doesn't exist
func (r *Reader) readNextRecord() (*Record, error) {
	// Read the record header
	header := make([]byte, headerSize)
	_, err := io.ReadFull(r.currentSegment.file, header)
	if err != nil {
		if err == io.EOF {
			// Check if there's another segment to read from
			if r.currentSegmentIndex < len(r.wal.segments)-1 {
				r.currentSegmentIndex++
				r.currentSegment = r.wal.segments[r.currentSegmentIndex]
				r.currentSegment.file.Seek(0, io.SeekStart)
				return r.readNextRecord()
			}
		}
		return nil, err
	}

	// Parse the header
	recordType := RecordType(binary.LittleEndian.Uint64(header[0:8]))
	entity := binary.LittleEndian.Uint64(header[8:16])
	txID := binary.LittleEndian.Uint64(header[16:24])
	dataLength := binary.LittleEndian.Uint32(header[24:28])

	// Read the record data
	data := make([]byte, dataLength)
	_, err = io.ReadFull(r.currentSegment.file, data)
	if err != nil {
		return nil, fmt.Errorf("failed to read record data: %w", err)
	}

	// Calculate the LSN
	currentPosition, err := r.currentSegment.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to get current file position: %w", err)
	}
	lsn := LSN(uint64(r.currentSegment.id)<<32 | uint64(currentPosition))

	return &Record{
		LSN:    lsn,
		Type:   recordType,
		Entity: entity,
		TxID:   txID,
		Data:   data,
	}, nil
}

func (r *Reader) Close() error {
	// Implementation
	return nil
}

func (r *Reader) fillBuffer() error {
	// Implementation
	return nil // Replace with actual implementation
}

type WalReader interface {
	Seek(lsn LSN) error
	Next() (*Record, error)
	Close() error
}
