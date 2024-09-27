// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"bytes"
	"io"

	"blockwatch.cc/knoxdb/internal/types"
)

const (
	chunkSize = 10 << 10
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

func NewReader(wal *Wal) *Reader {
	return &Reader{wal: wal, buf: bytes.NewBuffer(make([]byte, 0, chunkSize))}
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
	_, err = seg.Seek(int64(filepos), 0)
	if err != nil {
		return err
	}
	r.lsn = lsn
	r.seg = seg
	return nil
}

func (r *Reader) Next() (*Record, error) {
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

	// 	if r.seg == nil {
	// 		r.lsn = LSN(r.wal.opts.Seed)
	// 		err := r.Seek(r.lsn)
	// 		if err != nil {
	// 			return nil, err
	// 		}

	// 		_, err = io.Copy(r.buf, r.seg.fd)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 	}

	// 	record := &Record{}
	// 	dataLength := uint32(0)

	// RecordAssembler:
	// 	for {
	// 		if r.buf.Len() == 0 {
	// 			err := r.nextSegment()
	// 			if err != nil {
	// 				if errors.Is(err, os.ErrNotExist) {
	// 					break RecordAssembler
	// 				}
	// 				return nil, err
	// 			}
	// 		}

	// 		switch {
	// 		case !record.Type.IsValid():
	// 			typ, err := r.buf.ReadByte()
	// 			if err != nil {
	// 				return nil, err
	// 			}
	// 			record.Type = RecordType(typ)
	// 		case !record.Tag.IsValid():
	// 			tag, err := r.buf.ReadByte()
	// 			if err != nil {
	// 				return nil, err
	// 			}
	// 			record.Tag = types.ObjectTag(tag)
	// 		case !record.isEntityWritten:
	// 			switch {
	// 			case r.buf.Len() >= 8:
	// 				record.Entity = LE.Uint64(r.buf.Next(8))
	// 				record.isEntityWritten = true
	// 			default:
	// 				err := r.nextSegment()
	// 				if err != nil {
	// 					if errors.Is(err, os.ErrNotExist) {
	// 						break RecordAssembler
	// 					}
	// 					return nil, err
	// 				}
	// 			}
	// 		case !record.isTxIDWritten:
	// 			switch {
	// 			case r.buf.Len() >= 8:
	// 				record.TxID = LE.Uint64(r.buf.Next(8))
	// 				record.isTxIDWritten = true
	// 			default:
	// 				err := r.nextSegment()
	// 				if err != nil {
	// 					if errors.Is(err, os.ErrNotExist) {
	// 						break RecordAssembler
	// 					}
	// 					return nil, err
	// 				}
	// 			}
	// 		case !record.isDataLengthWritten:
	// 			switch {
	// 			case r.buf.Len() >= 4:
	// 				dataLength = LE.Uint32(r.buf.Next(4))
	// 				record.isDataLengthWritten = true
	// 			default:
	// 				err := r.nextSegment()
	// 				if err != nil {
	// 					if errors.Is(err, os.ErrNotExist) {
	// 						break RecordAssembler
	// 					}
	// 					return nil, err
	// 				}
	// 			}
	// 		default:
	// 			switch {
	// 			case r.buf.Len() >= int(dataLength):
	// 				record.Data = r.buf.Next(int(dataLength))
	// 				if !r.flt.Match(record) {
	// 					record = &Record{}
	// 					dataLength = uint32(0)
	// 					continue
	// 				}
	// 				// add checksum comparison
	// 				return record, nil
	// 			default:
	// 				err := r.nextSegment()
	// 				if err != nil {
	// 					if errors.Is(err, os.ErrNotExist) {
	// 						break RecordAssembler
	// 					}
	// 					return nil, err
	// 				}
	// 			}
	// 		}
	// 	}
	return nil, io.EOF
}

func (r *Reader) nextSegment() error {
	id := r.lsn.calculateFilename(r.wal.opts.MaxSegmentSize)
	r.lsn = NewLSN(id+1, int64(r.wal.opts.MaxSegmentSize), 0)
	seg, err := openSegment(r.lsn, r.wal.opts)
	if err != nil {
		return err
	}
	r.seg = seg
	_, err = io.Copy(r.buf, seg.fd)
	if err != nil {
		return err
	}
	return nil
}
