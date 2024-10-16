// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/journal"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

var _ engine.TableEngine = (*Table)(nil)

func init() {
	engine.RegisterTableFactory(engine.TableKindPack, NewTable)
}

var (
	DefaultTableOptions = engine.TableOptions{
		Driver:      "bolt",
		PackSize:    1 << 16, // 64k
		JournalSize: 1 << 17, // 128k
		PageSize:    1 << 16, // 64kB
		PageFill:    0.9,
		TxMaxSize:   1 << 24, // 16 MB,
		ReadOnly:    false,
		NoSync:      false,
		NoGrowSync:  false,
		Logger:      log.Disabled,
	}
)

type Table struct {
	mu      sync.RWMutex         // global table lock (syncs r/w access, single writer)
	engine  *engine.Engine       // engine access
	schema  *schema.Schema       // ordered list of table fields as central type info
	tableId uint64               // unique tagged name hash
	pkindex int                  // field index for primary key (if any)
	opts    engine.TableOptions  // copy of config options
	db      store.DB             // lower-level storage (e.g. boltdb wrapper)
	state   engine.ObjectState   // volatile state
	indexes []engine.IndexEngine // list of indexes
	stats   *stats.StatsIndex    // in-memory list of pack and block info
	journal *journal.Journal     // in-memory data not yet written to packs
	metrics engine.TableMetrics  // metrics statistics
	log     log.Logger
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
	t.state = engine.NewObjectState()
	t.metrics = engine.NewTableMetrics(name)
	t.stats = stats.NewStatsIndex(pki, t.opts.PackSize)
	t.journal = journal.NewJournal(s, t.opts.JournalSize)
	t.db = opts.DB
	t.log = opts.Logger

	// create db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Creating pack table %q with opts %#v", path, t.opts)
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
			db.Close()
			return err
		}
		t.db = db
	}

	// init table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	for _, v := range [][]byte{
		pack.DataKeySuffix,
		pack.MetaKeySuffix,
		pack.StatsKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(name), v...)
		if _, err := store.CreateBucket(tx, key, engine.ErrTableExists); err != nil {
			return err
		}
	}

	// TODO: replace with WAL stream
	jsz, tsz, err := t.journal.StoreLegacy(ctx, tx, t.schema.Name())
	if err != nil {
		return err
	}
	t.metrics.JournalDiskSize = int64(jsz)
	t.metrics.TombstoneDiskSize = int64(tsz)
	t.metrics.JournalTuplesThreshold = int64(opts.JournalSize)
	t.metrics.TombstoneTuplesThreshold = int64(opts.JournalSize)

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
	t.metrics = engine.NewTableMetrics(name)
	t.metrics.TupleCount = int64(t.state.NRows)
	t.stats = stats.NewStatsIndex(s.PkIndex(), t.opts.PackSize)
	t.journal = journal.NewJournal(s, t.opts.JournalSize)
	t.db = opts.DB
	t.log = opts.Logger

	// open db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Opening pack table %q with opts %#v", path, t.opts)
		db, err := store.Open(t.opts.Driver, path, t.opts.ToDriverOpts())
		if err != nil {
			t.log.Errorf("opening table %s: %v", typ, err)
			return engine.ErrNoTable
		}
		t.db = db

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

	// check table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return err
	}
	for _, v := range [][]byte{
		pack.DataKeySuffix,
		pack.MetaKeySuffix,
		pack.StatsKeySuffix,
		engine.StateKeySuffix,
	} {
		if tx.Bucket(append([]byte(name), v...)) == nil {
			t.log.Error("missing table data: %v", engine.ErrNoBucket)
			tx.Rollback()
			t.Close(ctx)
			return engine.ErrDatabaseCorrupt
		}
	}

	// TODO: maybe refactor

	// load state
	if err := t.state.Load(ctx, tx, t.schema.Name()); err != nil {
		t.log.Error("missing table state: %v", err)
		tx.Rollback()
		t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	// load stats
	t.log.Debugf("Loading package stats for %s", typ)
	n, err := t.stats.Load(ctx, tx, t.schema.Name())
	if err != nil {
		// TODO: rebuild corrupt stats here
		tx.Rollback()
		t.Close(ctx)
		return err
	}
	t.metrics.MetaBytesRead += int64(n)
	t.metrics.TupleCount = int64(t.state.NRows)
	t.metrics.PacksCount = int64(t.stats.Len())
	t.metrics.MetaSize = int64(t.stats.HeapSize())
	t.metrics.TotalSize = int64(t.stats.TableSize())

	// FIXME: reconstruct journal from WAL instead of load in legacy mode
	err = t.journal.Open(ctx, tx, t.schema.Name())
	if err != nil {
		tx.Rollback()
		t.Close(ctx)
		return fmt.Errorf("Open journal for table %s: %v", typ, err)
	}

	t.log.Debugf("Table %s opened with %d rows, %d journal rows, seq=%d",
		typ, t.state.NRows, t.journal.Len(), t.state.Sequence)

	// t.DumpType(os.Stdout)
	// t.DumpMetadata(os.Stdout, types.DumpModeHex)
	// t.DumpMetadataDetail(os.Stdout, types.DumpModeHex)

	return nil
}

func (t *Table) Close(ctx context.Context) (err error) {
	if t.db != nil {
		t.log.Debugf("Closing table %s", t.schema.TypeLabel(t.engine.Namespace()))
		err = t.db.Close()
		t.db = nil
	}
	t.engine = nil
	t.schema = nil
	t.tableId = 0
	t.pkindex = 0
	t.opts = engine.TableOptions{}
	t.metrics = engine.TableMetrics{}
	t.indexes = nil
	t.stats.Reset()
	t.journal.Close()
	t.journal = nil
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

func (t *Table) Metrics() engine.TableMetrics {
	m := t.metrics
	m.TupleCount = int64(t.state.NRows)
	return m
}

func (t *Table) Drop(ctx context.Context) error {
	typ := t.schema.TypeLabel(t.engine.Namespace())
	path := t.db.Path()
	t.journal.Close()
	t.db.Close()
	t.db = nil
	t.stats.Reset()
	t.log.Debugf("dropping table %s with path %s", typ, path)
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

func (t *Table) Sync(ctx context.Context) error {
	// FIXME: refactor legacy
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}

	// store journal
	if err := t.storeJournal(ctx); err != nil {
		return err
	}

	// store state
	if err := t.state.Store(ctx, tx, t.schema.Name()); err != nil {
		return err
	}

	// store stats
	n, err := t.stats.Store(ctx, tx, t.schema.Name(), t.opts.PageFill)
	if err != nil {
		return err
	}
	atomic.AddInt64(&t.metrics.MetaBytesWritten, int64(n))
	atomic.StoreInt64(&t.metrics.PacksCount, int64(t.stats.Len()))
	atomic.StoreInt64(&t.metrics.MetaSize, int64(t.stats.HeapSize()))

	return nil
}

func (t *Table) Truncate(ctx context.Context) error {
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	t.journal.Reset()
	t.stats.Reset()
	for _, v := range [][]byte{
		pack.DataKeySuffix,
		pack.MetaKeySuffix,
		pack.StatsKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(t.schema.Name()), v...)
		if err := tx.Root().DeleteBucket(key); err != nil {
			return err
		}
		if _, err := tx.Root().CreateBucket(key); err != nil {
			return err
		}
	}
	t.state.Reset()
	if err := t.state.Store(ctx, tx, t.schema.Name()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	atomic.AddInt64(&t.metrics.DeletedTuples, int64(t.state.NRows))
	atomic.StoreInt64(&t.metrics.TupleCount, 0)
	atomic.StoreInt64(&t.metrics.MetaSize, 0)
	atomic.StoreInt64(&t.metrics.JournalSize, 0)
	atomic.StoreInt64(&t.metrics.TombstoneDiskSize, 0)
	atomic.StoreInt64(&t.metrics.PacksCount, 0)
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
