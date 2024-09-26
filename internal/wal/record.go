// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"fmt"
	"io"
	"blockwatch.cc/knoxdb/internal/types"
)

type RecordType byte

const (
    RecordTypeInvalid RecordType = iota
    RecordTypeInsert
    RecordTypeUpdate
    RecordTypeDelete
    RecordTypeCommit
    RecordTypeAbort
    RecordTypeCheckpoint
    RecordTypeMax
)

var (
	recordTypeNames    = "__insert_update_delete_commit_abort_checkpoint"
	recordTypeNamesOfs = [...]int{0, 2, 9, 16, 23, 30, 36, 47}
)

func (t RecordType) IsValid() bool {
	return t != RecordTypeInvalid
}

func (t RecordType) String() string {
	return recordTypeNames[recordTypeNamesOfs[t] : recordTypeNamesOfs[t+1]-1]
}

// LSN represents a Log Serial Number for WAL records. The LSN is
// the unique position (offset) of a record in the wal.
type LSN uint64

type Record struct {
	LSN    LSN
	Type   RecordType
	Tag    types.ObjectTag // object kind (db, table, store, enum, etc)
	TxID   uint64          // unique transaction id this record was belongs to
	Entity uint64          // object id (tagged hash for db, table, store, enum, etc)
	Data   []byte          // body with encoded data, may be empty
	Lsn    LSN             // the record's byte offset in the WAL
}

func (r Record) String() string {
	return fmt.Sprintf("wal: LSN=0x%016x xid=0x%016x  typ=%s tag=%s entity=0x%016x len=%d",
		r.Lsn, r.TxID, r.Type, r.Tag, r.Entity, len(r.Data),
	)
}

func (r *Record) Write(w io.Writer) error {
	size := r.calculateSize()
	if size > MaxRecordSize {
		return fmt.Errorf("record size %d exceeds maximum allowed size %d", size, MaxRecordSize)
	}
	// ... rest of the write logic ...
	return nil
}

func (r *Record) calculateSize() int {
	// Calculate and return the size of the record
	// This should include the size of all fields in the record
	return len(r.Data) + 8 + 8 + 1 + 1 // Example: data size + entity size + txID size + type size + tag size
}

const MaxRecordSize = 1024 * 1024 // 1MB, adjust as needed

type FieldType string

const (
    TypeUint64  FieldType = "uint64"
    TypeInt64   FieldType = "int64"
    TypeFloat64 FieldType = "float64"
    TypeString  FieldType = "string"
    TypeBytes   FieldType = "bytes"
)
