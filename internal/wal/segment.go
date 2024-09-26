// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"os"
	"fmt"
	"errors"
)

type segment struct {
	id   uint64
	file *os.File
	pos  int64
}

var ErrSegmentFull = errors.New("segment is full")

// func newSegement() *segment {
// 	return &segment{
// 		hash: xxhash.New(),
// 	}
// }

func createSegment(id LSN) (*segment, error) {
	// use the seed as first checksum
	return nil, nil
}

func openSegment(id LSN) (*segment, error) {
	// load last record's checksum
	return nil, nil
}

func (s *segment) Close() error {
	err := s.file.Close()
	s.file = nil
	s.id = 0
	s.pos = 0
	return err
}

func (s *segment) Sync() error {
	return s.file.Sync()
}

func (s *segment) LastRecord() (*Record, error) {
	// Implementation
	return nil, nil
}

func (s *segment) Truncate(sz int64) error {
	return s.file.Truncate(sz)
}

func (s *segment) Write(buf []byte) (int, error) {
	n, err := s.file.Write(buf)
	if err != nil {
		return n, err
	}
	s.pos += int64(n)
	return n, nil
}

// func (s *segment) Write(rec *Record) (lsn LSN, err error) {
// 	// Note: this is only an example to show how a record can be written
// 	//
// 	// create header
// 	var head [28]byte
// 	head[0] = byte(rec.Type)
// 	head[1] = byte(rec.Tag)
// 	LE.PutUint64(head[2:], rec.Entity)
// 	LE.PutUint64(head[10:], rec.TxID)
// 	LE.PutUint32(head[16:], uint32(len(rec.Data)))

// 	// calculate chained checksum
// 	s.hash.Reset()
// 	var b [8]byte
// 	LE.PutUint64(b[:], s.csum)
// 	s.hash.Write(b[:])
// 	s.hash.Write(head[:20])
// 	s.hash.Write(rec.Data)
// 	s.hash.Sum(head[20:])

// 	// write header
// 	var n, sz int
// 	n, err = s.fd.Write(head[:])
// 	if err != nil {
// 		return
// 	}
// 	sz += n

// 	// write data
// 	n, err = s.fd.Write(rec.Data)
// 	if err != nil {
// 		return
// 	}
// 	sz += n

// 	// TODO: mix in the segment id
// 	lsn = LSN(s.id + s.pos)

// 	// update state
// 	s.pos += sz
// 	s.csum = s.hash.Sum64()

// 	return
// }

func (s *segment) Write(r *Record) (LSN, error) {
	// Calculate the size of the record
	recordSize := r.calculateSize()

	// Check if the record size exceeds the maximum allowed size
	if recordSize > s.wal.opts.MaxRecordSize {
		return 0, fmt.Errorf("record size %d exceeds maximum allowed size %d", recordSize, s.wal.opts.MaxRecordSize)
	}

	// Check if writing this record would exceed the maximum segment size
	if s.pos+int64(recordSize) > s.wal.opts.MaxSegmentSize {
		return 0, ErrSegmentFull
	}

	// Existing write logic
	// ...
}

func (s *segment) writeRecord(r *Record) (LSN, error) {
	// Implementation of actual record writing
	// ...

	return LSN(uint64(s.id)<<32 | uint64(s.pos)), nil
}
