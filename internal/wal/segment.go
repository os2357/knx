// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"fmt"
	"os"
)

const (
	segmentExt = "SEG"
)

type segment struct {
	id  int64
	pos int64
	fd  *os.File
}

func createSegment(id LSN, opts WalOptions) (*segment, error) {
	filename := id.calculateFilename(opts.MaxSegmentSize)
	name := fmt.Sprintf("%d.%s", filename, segmentExt)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	// use the seed as first checksum
	return &segment{
		pos: 0,
		id:  filename,
		fd:  f,
	}, nil
}

func openSegment(id LSN, opts WalOptions) (*segment, error) {
	filename := id.calculateFilename(opts.MaxSegmentSize)
	name := fmt.Sprintf("%d.%s", filename, segmentExt)
	f, err := os.OpenFile(name, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	fileOffset := id.calculateOffset(opts.MaxSegmentSize)
	// load last record's checksum
	return &segment{
		fd:  f,
		id:  filename,
		pos: fileOffset,
	}, nil
}

func (s *segment) Close() error {
	err := s.fd.Close()
	s.fd = nil
	s.id = 0
	s.pos = 0
	return err
}

func (s *segment) Sync() error {
	return s.fd.Sync()
}

func (s *segment) LastRecord() (*Record, error) {
	return nil, nil
}

func (s *segment) Truncate(sz int64) error {
	return s.fd.Truncate(sz)
}

func (s *segment) Write(buf []byte) (int, error) {
	n, err := s.fd.Write(buf)
	if err != nil {
		return n, err
	}
	s.pos += int64(n)
	return n, nil
}

func (s *segment) Seek(offset int64, whence int) (int64, error) {
	n, err := s.fd.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	return n, nil
}
