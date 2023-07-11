// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package query

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"blockwatch.cc/knoxdb/pack"
	"blockwatch.cc/knoxdb/store"
	"github.com/echa/config"
	"github.com/echa/log"

	bolt "go.etcd.io/bbolt"

	_ "blockwatch.cc/knoxdb/store/bolt"
)

type Query struct {
	ctx         context.Context
	db          *pack.DB
	activeTable string
	opts        *bolt.Options
	dbPath      string
	table       *pack.Table
}

func (q Query) ActiveDB() string {
	if q.db == nil {
		return ""
	}
	base := filepath.Base(filepath.Dir(q.db.Path()))
	return base + ":" + q.activeTable
}

func New(ctx context.Context) *Query {
	dbPath := config.GetString("db.path")
	dbTable := config.GetString("db.table")
	opts := &bolt.Options{
		Timeout:      config.GetDuration("db.options.timeout"),  // open timeout when file is locked
		NoGrowSync:   config.GetBool("db.options.no_grow_sync"), // assuming Docker + XFS
		ReadOnly:     config.GetBool("db.options.readonly"),
		NoSync:       config.GetBool("db.options.no_sync"), // skip fsync (DANGEROUS on crashes)
		FreelistType: bolt.FreelistMapType,
	}
	db, err := pack.OpenDatabase(dbPath, dbTable, "*", opts)
	if err != nil {
		log.Warn("failed to open db")
		log.Debugf("failed to open selected db path: %v", err)
	} else {
		dbTable = ""
	}

	return &Query{
		ctx:         ctx,
		db:          db,
		activeTable: dbTable,
		opts:        opts,
		dbPath:      dbPath,
		table:       nil,
	}
}

func (q *Query) UseTable(table string) error {
	db, err := pack.OpenDatabase(q.dbPath, table, "*", q.opts)
	if err != nil {
		return err
	} else {
		q.activeTable = table
		q.db = db
	}
	if t, err := db.Table(table); err != nil {
		return err
	} else {
		q.table = t
	}
	return nil
}

func (q *Query) UseDatabase(path, table string) error {
	db, err := pack.OpenDatabase(path, table, "*", q.opts)
	if err != nil {
		return err
	} else {
		q.dbPath = path
		q.activeTable = table
		q.db = db
	}
	if t, err := db.Table(table); err != nil {
		return err
	} else {
		q.table = t
	}
	return nil
}

func (q *Query) setTable(db *pack.DB, table string) error {
	if t, err := db.Table(table); err != nil {
		return err
	} else {
		q.table = t
	}
	return nil
}

func (q *Query) ListTableFields(table string) (pack.FieldList, error) {
	if q.activeTable == "" {
		return nil, fmt.Errorf("no active table selected")
	}
	return q.table.Fields(), nil
}

func (q *Query) Table() *pack.Table {
	return q.table
}

func (q *Query) DumpTable() error {
	for i := 0; ; i++ {
		err := q.table.DumpPack(os.Stdout, i, pack.DumpModeDec)
		if err != nil && err != pack.ErrPackNotFound {
			return err
		}
		if err == pack.ErrPackNotFound {
			break
		}
	}
	return nil
}

// gc runs garbace collection on a bolt kv store by creating a new boltdb file
// and copying all nested buckets and key/value pairs from the original. Replaces
// the original file on success. this operation needs up to twice the amount of
// disk space.
func (q *Query) GC() error {
	return q.db.GC(q.ctx, 1.0)
}

// flush flushes journal data to packs
func (q *Query) Flush() error {
	return q.table.Flush(q.ctx)
}

// reindex drops and re-creates all indexes defined for a given table.
func (q *Query) RebuildStatistics() error {
	start := time.Now()

	if q.table == nil {
		return fmt.Errorf("invalid table selected")
	}

	// make sure source table journals are flushed
	if err := q.table.Flush(context.Background()); err != nil {
		return err
	}
	stats := q.table.Stats()

	log.Infof("Rebuilding metadata for %d rows / %d packs with statistics size %d bytes",
		stats[0].TupleCount, stats[0].PacksCount, stats[0].MetaSize)

	// Delete table metadata bucket
	log.Info("Dropping table statistics")
	err := q.db.Update(func(dbTx store.Tx) error {
		meta := dbTx.Bucket([]byte(q.activeTable + "_meta"))
		if meta == nil {
			return fmt.Errorf("missing table metdata bucket")
		}
		err := meta.DeleteBucket([]byte("_headers"))
		if !store.IsError(err, store.ErrBucketNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Open table, this will automatically rebuild all metadata
	log.Info("Rebuilding table statistics")
	table, err := q.db.Table(q.activeTable)
	if err != nil {
		return err
	}

	// Close table, this will automatically store the new metadata
	stats = table.Stats()
	log.Info("Storing table statistics")
	err = table.Close()
	if err != nil {
		return err
	}

	log.Infof("Rebuild took %s, new statistics size %d bytes", time.Since(start), stats[0].MetaSize)
	return nil
}

// reindex drops and re-creates all indexes defined for a given table.
func (q *Query) Reindex() error {
	// make sure source table journals are flushed
	if err := q.table.Flush(q.ctx); err != nil {
		return err
	}

	// walk source table in packs and bulk-insert data into target
	stats := q.table.Stats()
	log.Infof("Rebuild indexes over %d rows / %d packs from table %s",
		stats[0].TupleCount, stats[0].PacksCount, q.table.Name())

	// rebuild indexes
	for _, idx := range q.table.Indexes() {
		log.Infof("Rebuilding %s index on field %s (%s)", idx.Name, idx.Field.Name, idx.Field.Type)
		prog := make(chan float64, 100)
		go func() {
			for {
				select {
				case <-q.ctx.Done():
					return
				case f := <-prog:
					log.Infof("Index build progress %.2f%%", f)
					if f == 100 {
						return
					}
				}
			}
		}()
		// flush every 128 packs
		err := idx.Reindex(q.ctx, 128, prog)
		close(prog)
		if err != nil {
			return err
		}
	}
	return nil
}

// compact compacts a table and its indexes to remove pack fragmentation
func (q *Query) Compact() error {
	if err := q.table.Flush(q.ctx); err != nil {
		return err
	}
	if err := q.table.Compact(q.ctx); err != nil {
		return err
	}
	return nil
}
