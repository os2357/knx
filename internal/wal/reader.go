// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"blockwatch.cc/knoxdb/internal/types"
)

type RecordFilter struct {
	Type   RecordType
	Tag    types.ObjectTag
	Entity uint64
	TxID   uint64
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
	currentSegment int
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
	// Implementation
	return nil // Replace with actual implementation
}

func (r *Reader) Next() (*Record, error) {
	// Implementation
	return nil, nil // Replace with actual implementation
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
