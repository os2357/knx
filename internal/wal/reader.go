// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"bytes"
	"io"

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
	flt *RecordFilter
	seg *segment
	wal *Wal
	buf *bytes.Buffer
	lsn LSN
}

func (r *Reader) WithType(t RecordType) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.Type = t
	return r
}

func (r *Reader) WithTag(t types.ObjectTag) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.Tag = t
	return r
}

func (r *Reader) WithEntity(v uint64) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.Entity = v
	return r
}

func (r *Reader) WithTxID(v uint64) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.TxID = v
	return r
}

func (r *Reader) Close() error {
	err := r.seg.Close()
	r.seg = nil
	r.wal = nil
	r.flt = nil
	return err
}

func (r *Reader) Seek(lsn LSN) error {
	// open segment and seek
	filepos := lsn.calculateOffset(r.wal.opts.MaxSegmentSize)
	seg, err := openSegment(lsn, r.wal.opts)
	if err != nil {
		return err
	}
	_, err = seg.fd.Seek(int64(filepos), 0)
	if err != nil {
		return err
	}
	r.lsn = lsn
	r.seg = seg
	return nil
}

func (r *Reader) Next() (*Record, error) {
	// check the seg is not nil
	// check the lsn no
	// open 0 segment if it is nil
	// read file to buffer

	// read protocol
	// - read large chunks of data (to amortize i/o costs) into a buffer
	// - then iterate the buffer record by record
	// - if the remaining data in the buffer is < record header size
	//   or if the remaining data is < record body len, read more chunks
	//   until the next full record is assemled
	// - assembling a very large record may require to work across segement
	//   files
	// - after reading each record, check the chained checksum
	// - then decide whether we should skip based on filter match

	return nil, io.EOF
}
