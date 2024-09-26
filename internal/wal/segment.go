// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"os"
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

func (s *segment) Write(r *Record) (LSN, error) {
	// Implementation
	return LSN(0), nil // Replace with actual implementation
}

func (s *segment) writeRecord(r *Record) (LSN, error) {
	// Implementation of actual record writing
	// ...

	return LSN(uint64(s.id)<<32 | uint64(s.pos)), nil
}
