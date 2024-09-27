// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"encoding/binary"
	"hash"
	"os"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/types"
)

var LE = binary.LittleEndian

const (
	headerSize = 30
)

type WalReader interface {
	Seek(LSN) error
	Next() (*Record, error)
	Close() error
	// NextN([]*Record) error

	WithType(RecordType) WalReader
	WithTag(types.ObjectTag) WalReader
	WithEntity(uint64) WalReader
	WithTxID(uint64) WalReader
}

type WalOptions struct {
	Seed           uint64
	Path           string
	MaxSegmentSize int
}

type Wal struct {
	opts   WalOptions
	active *segment
	csum   uint64
	hash   hash.Hash64
}

func Create(opts WalOptions) (*Wal, error) {
	// create directory
	_, err := os.Create(opts.Path)
	if err != nil {
		return nil, err
	}
	// create active wal segment
	seg, err := createSegment(LSN(opts.Seed), opts)
	if err != nil {
		return nil, err
	}
	return &Wal{
		opts:   opts,
		active: seg,
		hash:   xxhash.New(),
	}, nil
}

func Open(id LSN, opts WalOptions) (*Wal, error) {
	// try open directory
	_, err := os.Stat(opts.Path)
	if err != nil {
		return nil, err
	}
	// set exclusive lock
	// open last segment file
	// read hash of last record and init w.csum
	seg, err := openSegment(id, opts)
	if err != nil {
		return nil, err
	}
	return &Wal{
		opts:   opts,
		active: seg,
		hash:   xxhash.New(),
	}, nil
}

func (w *Wal) Close() error {
	err := w.active.Close()
	w.active = nil
	return err
}

func (w *Wal) Sync() error {
	return w.active.Sync()
}

func (w *Wal) Write(rec *Record) (LSN, error) {
	// write record to active segment
	// create header
	var head [headerSize]byte
	head[0] = byte(rec.Type)
	head[1] = byte(rec.Tag)
	LE.PutUint64(head[2:], rec.Entity)
	LE.PutUint64(head[10:], rec.TxID)
	LE.PutUint32(head[18:], uint32(len(rec.Data)))

	// calculate chained checksum
	w.hash.Reset()
	var b [8]byte
	LE.PutUint64(b[:], w.csum)
	w.hash.Write(b[:])
	w.hash.Write(head[:22])
	w.hash.Write(rec.Data)
	w.hash.Sum(head[22:])

	// remember current size and truncate on failed write
	// calculate the LSN
	lsn := NewLSN(w.active.id, int64(w.opts.MaxSegmentSize), w.active.pos)

	data := head[:]
	dataPos := int64(0)
	isHeaderWritten := false
	sizeOfRemainingDataToWrite := int64(headerSize)

	for {
		if w.opts.MaxSegmentSize == int(w.active.pos) {
			// make sure active file synced first
			err := w.nextSegment()
			if err != nil {
				return 0, err
			}
		}

		spaceLeft := int64(w.opts.MaxSegmentSize) - w.active.pos
		sizeOfDataToWriteToCurrentFile := sizeOfRemainingDataToWrite
		if sizeOfRemainingDataToWrite < spaceLeft {
			sizeOfDataToWriteToCurrentFile = spaceLeft
		}

		pos, err := w.writeData(data[dataPos : dataPos+sizeOfDataToWriteToCurrentFile])
		if err != nil {
			return 0, err
		}
		w.active.pos = int64(pos)
		sizeOfRemainingDataToWrite -= sizeOfDataToWriteToCurrentFile
		dataPos -= sizeOfDataToWriteToCurrentFile

		if sizeOfRemainingDataToWrite == 0 {
			if isHeaderWritten {
				break
			}

			isHeaderWritten = true
			data = rec.Data
			dataPos = int64(0)
		}
	}

	// update state
	w.csum = w.hash.Sum64()

	// TODO: mix in the segment id
	return lsn, nil
}

func (w *Wal) NewReader() WalReader {
	return &Reader{
		wal: w,
	}
}

// rolls the active segement
func (w *Wal) nextSegment() error {
	// close and fsync the current active segment
	// create new segment file
	// fsync the directory
	if err := w.active.Sync(); err != nil {
		return err
	}
	if err := w.active.Close(); err != nil {
		return err
	}
	lsn := NewLSN(w.active.id+1, int64(w.opts.MaxSegmentSize), 0)
	seg, err := createSegment(lsn, w.opts)
	if err != nil {
		return err
	}
	w.active = seg
	return nil
}

func (w *Wal) writeData(data []byte) (int, error) {
	pos, err := w.active.Write(data)
	if err != nil {
		_ = w.active.Truncate(w.active.pos)
		return 0, err
	}
	return pos, err
}
