// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
	"golang.org/x/exp/slices"
)

var _ engine.TableEngine = (*Table)(nil)

func init() {
	engine.RegisterTableFactory(engine.TableKindLSM, NewTable)
}

var (
	BE = binary.BigEndian    // byte order for keys
	NE = binary.NativeEndian // byte order for values (LE)

	DefaultTableOptions = engine.TableOptions{
		Driver:     "bolt",
		PageSize:   1 << 16,
		PageFill:   0.9,
		TxMaxSize:  1 << 24, // 16 MB,
		ReadOnly:   false,
		NoSync:     false,
		NoGrowSync: false,
		Logger:     log.Disabled,
	}
)

type Table struct {
	engine     *engine.Engine       // engine access
	schema     *schema.Schema       // table schema
	tableId    uint64               // unique tagged name hash
	pkindex    int                  // field index for primary key (if any)
	opts       engine.TableOptions  // copy of config options
	db         store.DB             // lower-level KV store (e.g. boltdb or badger)
	key        []byte               // name of the data bucket
	isZeroCopy bool                 // storage reads are zero copy (copy to safe references)
	noClose    bool                 // don't close underlying store db on Close
	state      engine.ObjectState   // volatile state, synced with catalog
	indexes    []engine.IndexEngine // list of indexes
	metrics    engine.TableMetrics  // usage statistics
	log        log.Logger
}

func NewTable() engine.TableEngine {
	return &Table{}
}

func (t *Table) Create(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	// require primary key
	pki := s.PkIndex()
	if pki < 0 {
		return engine.ErrNoPk
	}

	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup store
	t.engine = e
	t.schema = s
	t.tableId = s.TaggedHash(types.ObjectTagTable)
	t.pkindex = pki
	t.opts = DefaultTableOptions.Merge(opts)
	t.key = []byte(name)
	t.state = engine.NewObjectState()
	t.metrics = engine.NewTableMetrics(name)
	t.db = opts.DB
	t.noClose = true
	t.log = opts.Logger

	// create db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Creating LSM table %q with opts %#v", path, t.opts)
		db, err := store.Create(t.opts.Driver, path, t.opts.ToDriverOpts())
		if err != nil {
			return fmt.Errorf("creating table %s: %v", typ, err)
		}
		err = db.SetManifest(store.Manifest{
			Name:    name,
			Schema:  typ,
			Version: int(s.Version()),
		})
		if err != nil {
			_ = db.Close()
			return err
		}
		t.db = db
		t.noClose = false
	}
	t.isZeroCopy = t.db.IsZeroCopyRead()

	// init table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	if _, err := store.CreateBucket(tx, t.key, engine.ErrTableExists); err != nil {
		return err
	}

	// init state storage
	if err := t.state.Store(ctx, tx, name); err != nil {
		return err
	}

	t.log.Debugf("Created table %s", typ)
	return nil
}

func (t *Table) Open(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup table
	t.engine = e
	t.schema = s
	t.tableId = s.TaggedHash(types.ObjectTagTable)
	t.pkindex = s.PkIndex()
	t.opts = DefaultTableOptions.Merge(opts)
	t.key = []byte(name)
	t.metrics = engine.NewTableMetrics(name)
	t.metrics.TupleCount = int64(t.state.NRows)
	t.db = opts.DB
	t.noClose = true
	t.log = opts.Logger

	// open db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Opening LSM table %q with opts %#v", path, t.opts)
		db, err := store.Open(t.opts.Driver, path, t.opts.ToDriverOpts())
		if err != nil {
			t.log.Errorf("opening table %s: %v", typ, err)
			return engine.ErrNoTable
		}
		t.db = db
		t.noClose = false

		// check manifest matches
		mft, err := t.db.Manifest()
		if err != nil {
			t.log.Errorf("missing manifest: %v", err)
			_ = t.Close(ctx)
			return engine.ErrDatabaseCorrupt
		}
		err = mft.Validate(name, "*", typ, -1)
		if err != nil {
			t.log.Errorf("schema mismatch: %v", err)
			_ = t.Close(ctx)
			return schema.ErrSchemaMismatch
		}
	}
	t.isZeroCopy = t.db.IsZeroCopyRead()

	// check table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return err
	}
	b := tx.Bucket(t.key)
	if b == nil {
		t.log.Error("missing table data: %v", engine.ErrNoBucket)
		tx.Rollback()
		_ = t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}
	stats := b.Stats()
	t.metrics.TotalSize = int64(stats.Size) // estimate only

	// load state
	if err := t.state.Load(ctx, tx, t.schema.Name()); err != nil {
		t.log.Error("missing table state: %v", err)
		tx.Rollback()
		t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	t.log.Debugf("Table %s opened with %d rows", typ, t.state.NRows)
	return nil
}

func (t *Table) Close(ctx context.Context) (err error) {
	if !t.noClose && t.db != nil {
		t.log.Debugf("Closing table %s", t.schema.TypeLabel(t.engine.Namespace()))
		err = t.db.Close()
		t.db = nil
	}
	t.engine = nil
	t.schema = nil
	t.tableId = 0
	t.pkindex = 0
	t.key = nil
	t.noClose = false
	t.isZeroCopy = false
	t.opts = engine.TableOptions{}
	t.metrics = engine.TableMetrics{}
	t.indexes = nil
	return
}

func (t *Table) Schema() *schema.Schema {
	return t.schema
}

func (t *Table) State() engine.ObjectState {
	return t.state
}

func (t *Table) Indexes() []engine.IndexEngine {
	return t.indexes
}

func (t *Table) name() string {
	return t.schema.Name()
}

func (t *Table) Metrics() engine.TableMetrics {
	m := t.metrics
	m.TupleCount = int64(t.state.NRows)
	return m
}

func (t *Table) Drop(ctx context.Context) error {
	typ := t.schema.TypeLabel(t.engine.Namespace())
	if t.noClose {
		tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
		if err != nil {
			return err
		}
		t.log.Debugf("dropping table %s", typ)
		if err := tx.Root().DeleteBucket(t.key); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	path := t.db.Path()
	t.db.Close()
	t.db = nil
	t.log.Debugf("dropping table %s with path %s", typ, path)
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

func (t *Table) Sync(_ context.Context) error {
	return nil
}

func (t *Table) Compact(ctx context.Context) error {
	return t.db.GC(ctx, t.opts.PageFill)
}

func (t *Table) Truncate(ctx context.Context) error {
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	if err := tx.Root().DeleteBucket(t.key); err != nil {
		return err
	}
	if _, err := tx.Root().CreateBucket(t.key); err != nil {
		return err
	}
	t.state.Reset()
	if err := t.state.Store(ctx, tx, t.schema.Name()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	t.metrics.DeletedTuples += int64(t.state.NRows)
	t.metrics.TupleCount = 0
	return nil
}

func (t *Table) UseIndex(idx engine.IndexEngine) {
	t.indexes = append(t.indexes, idx)
}

func (t *Table) UnuseIndex(idx engine.IndexEngine) {
	idxId := idx.Schema().TaggedHash(types.ObjectTagIndex)
	t.indexes = slices.DeleteFunc(t.indexes, func(v engine.IndexEngine) bool {
		return v.Schema().TaggedHash(types.ObjectTagIndex) == idxId
	})
}

// low-level interface for KV storage access
func (t *Table) getTx(tx store.Tx, key []byte) []byte {
	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return nil
	}
	buf := bucket.Get(key)
	if buf == nil {
		return nil
	}
	atomic.AddInt64(&t.metrics.BytesRead, int64(len(buf)))
	return buf
}

func (t *Table) putTx(tx store.Tx, key, val []byte) ([]byte, error) {
	prevSize, sz := -1, len(key)+len(val)
	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return nil, engine.ErrNoBucket
	}
	buf := bucket.Get(key)
	if buf != nil {
		prevSize = len(buf) + len(key)
	} else {
		t.state.NRows++
	}
	err := bucket.Put(key, val)
	if err != nil {
		return nil, err
	}
	if prevSize >= 0 {
		// update
		atomic.AddInt64(&t.metrics.UpdatedTuples, 1)
		atomic.AddInt64(&t.metrics.TotalSize, int64(sz-prevSize))
	} else {
		// insert
		atomic.AddInt64(&t.metrics.InsertedTuples, 1)
		atomic.AddInt64(&t.metrics.TupleCount, 1)
		atomic.AddInt64(&t.metrics.TotalSize, int64(sz))
	}
	atomic.AddInt64(&t.metrics.BytesWritten, int64(sz))
	return buf, nil
}

func (t *Table) delTx(tx store.Tx, key []byte) ([]byte, error) {
	prevSize := -1
	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return nil, engine.ErrNoBucket
	}
	buf := bucket.Get(key)
	if buf != nil {
		prevSize = len(buf)
		t.state.NRows--
	}
	err := bucket.Delete(key)
	if err == nil && prevSize >= 0 {
		atomic.AddInt64(&t.metrics.TupleCount, -1)
		atomic.AddInt64(&t.metrics.DeletedTuples, 1)
		atomic.AddInt64(&t.metrics.TotalSize, -int64(prevSize))
	}
	return buf, err
}
