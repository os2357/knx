// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/store"
	_ "blockwatch.cc/knoxdb/internal/store/mem"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/cache/rclru"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

const TEST_DB_NAME = "test"

func NewTestDatabaseOptions(t *testing.T, driver string) DatabaseOptions {
	return DatabaseOptions{
		Path:       t.TempDir(),
		Namespace:  "cx.bwd.knoxdb.testdb",
		Driver:     driver,
		PageSize:   4096,
		PageFill:   1.0,
		CacheSize:  1 << 20,
		NoSync:     false,
		NoGrowSync: false,
		ReadOnly:   false,
		Logger:     log.Log,
	}
}

func NewTestEngine(opts DatabaseOptions) *Engine {
	path := filepath.Join(opts.Path, TEST_DB_NAME)
	e := &Engine{
		path: path,
		cache: CacheManager{
			blocks:  rclru.NewNoCache[CacheKeyType, *block.Block](),
			buffers: rclru.NewNoCache[CacheKeyType, *Buffer](),
		},
		tables:  make(map[uint64]TableEngine),
		stores:  make(map[uint64]StoreEngine),
		indexes: make(map[uint64]IndexEngine),
		enums:   make(map[uint64]*schema.EnumDictionary),
		txs:     make(TxList, 0),
		xmin:    0,
		xnext:   1,
		dbId:    types.TaggedHash(types.ObjectTagDatabase, TEST_DB_NAME),
		opts:    opts,
		cat:     NewCatalog(TEST_DB_NAME),
		log:     opts.Logger,
	}
	var err error
	e.wal, err = wal.Create(wal.WalOptions{
		Seed:           0,
		Path:           path,
		MaxSegmentSize: 1024,
		RecoveryMode:   wal.RecoveryModeTruncate,
		Logger:         opts.Logger,
	})
	if err != nil {
		panic(err)
	}
	e.xlog = wal.NewCommitLog().WithLogger(opts.Logger)
	err = e.xlog.Open(path, e.wal)
	if err != nil {
		panic(err)
	}
	return e
}

func OpenTestEngine(opts DatabaseOptions) *Engine {
	path := filepath.Join(opts.Path, TEST_DB_NAME)
	e := &Engine{
		path: path,
		cache: CacheManager{
			blocks:  rclru.NewNoCache[CacheKeyType, *block.Block](),
			buffers: rclru.NewNoCache[CacheKeyType, *Buffer](),
		},
		tables:  make(map[uint64]TableEngine),
		stores:  make(map[uint64]StoreEngine),
		indexes: make(map[uint64]IndexEngine),
		enums:   make(map[uint64]*schema.EnumDictionary),
		txs:     make(TxList, 0),
		xmin:    0,
		xnext:   1,
		dbId:    types.TaggedHash(types.ObjectTagDatabase, TEST_DB_NAME),
		opts:    opts,
		cat:     NewCatalog(TEST_DB_NAME),
		log:     opts.Logger,
	}

	var err error
	e.wal, err = wal.Open(0, wal.WalOptions{
		Seed:           0,
		Path:           path,
		MaxSegmentSize: 1024,
		RecoveryMode:   wal.RecoveryModeTruncate,
		Logger:         opts.Logger,
	})
	if err != nil {
		panic(err)
	}
	e.xlog = wal.NewCommitLog().WithLogger(opts.Logger)
	err = e.xlog.Open(path, e.wal)
	if err != nil {
		panic(err)
	}
	return e
}

func TestCatalogCreate(t *testing.T) {
	e := NewTestEngine(NewTestDatabaseOptions(t, "mem"))
	ctx, commit, _ := e.WithTransaction(context.Background())
	require.NoError(t, e.cat.Create(ctx, e.opts))
	require.NoError(t, commit())

	// worst case we get a tx deadlock, so we check using Eventually()
	require.Eventually(t, func() bool {
		err := e.cat.db.View(func(tx store.Tx) error {
			for _, key := range [][]byte{
				databaseKey,
				schemasKey,
				optionsKey,
				tablesKey,
				indexesKey,
				viewsKey,
				enumsKey,
				storesKey,
				snapshotsKey,
				streamsKey,
			} {
				t.Log("Check bucket", string(key))
				require.NotNil(t, tx.Bucket(key))
			}
			return nil
		})
		require.NoError(t, err)
		return true
	}, time.Second/4, time.Second/8)

	require.NoError(t, e.cat.Close(context.Background()))
}

func TestCatalogOpen(t *testing.T) {
	// create first engine
	e := NewTestEngine(NewTestDatabaseOptions(t, "mem"))
	ctx, commit, _ := e.WithTransaction(context.Background())

	// create catalog (requires write tx)
	require.NoError(t, e.cat.Create(ctx, e.opts))
	require.NoError(t, commit())
	require.NoError(t, e.cat.Close(context.Background()))

	// create new engine
	e = OpenTestEngine(e.opts)
	ctx, _, abort := e.WithTransaction(context.Background())
	require.NoError(t, e.cat.Open(ctx, e.opts))

	// worst case we get a tx deadlock, so we check using Eventually()
	require.Eventually(t, func() bool {
		err := e.cat.db.View(func(tx store.Tx) error {
			for _, key := range [][]byte{
				databaseKey,
				schemasKey,
				optionsKey,
				tablesKey,
				indexesKey,
				viewsKey,
				enumsKey,
				storesKey,
				snapshotsKey,
				streamsKey,
			} {
				t.Log("Check bucket", string(key))
				require.NotNil(t, tx.Bucket(key))
			}
			return nil
		})
		require.NoError(t, err)
		return true
	}, time.Second/4, time.Second/8)

	require.NoError(t, abort())
	require.NoError(t, e.cat.Close(context.Background()))
}

func TestCatalogMissingTx(t *testing.T) {
	e := NewTestEngine(NewTestDatabaseOptions(t, "mem"))
	require.Error(t, e.cat.Create(context.Background(), e.opts))
	require.NoError(t, e.cat.Close(context.Background()))
}

func WithCatalog(t *testing.T) (context.Context, *Engine, *Catalog, func() error) {
	ctx := context.Background()
	e := NewTestEngine(NewTestDatabaseOptions(t, "mem"))
	tctx, commit, _ := e.WithTransaction(ctx)
	require.NoError(t, e.cat.Create(tctx, e.opts))
	require.NoError(t, commit())
	return ctx, e, e.cat, func() error { return e.cat.Close(ctx) }
}

type TestTable struct {
	Id uint64 `knox:"id,pk"`
	F1 int    `knox:"f1"`
}

func TestCatalogAddTable(t *testing.T) {
	ctx, eng, cat, close := WithCatalog(t)
	defer close()
	tctx, commit, abort := eng.WithTransaction(ctx)
	defer abort()
	s, err := schema.SchemaOf(&TestTable{})
	require.NoError(t, err)
	t.Log("Table", s)
	opts := TableOptions{
		Engine:   "pack",
		Driver:   "mem",
		PageSize: 1024,
	}
	require.NoError(t, cat.AddTable(tctx, 1, s, opts))
	require.NoError(t, commit())

	// list tables
	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	keys, err := cat.ListTables(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], uint64(1))

	// get table
	s2, opts2, err := cat.GetTable(tctx, 1)
	require.NoError(t, err)
	require.NotNil(t, s2)
	require.Equal(t, s2.Name(), s.Name())
	require.Equal(t, s2.Hash(), s.Hash())
	require.Equal(t, opts2, opts)
	require.NoError(t, abort())

	// drop table
	tctx, commit, abort = eng.WithTransaction(ctx)
	defer abort()
	require.NoError(t, cat.DropTable(tctx, 1))
	require.NoError(t, commit())

	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	keys, err = cat.ListTables(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 0)
	require.NoError(t, abort())

	// drop unknown table
	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	require.Error(t, cat.DropTable(tctx, 1))
}

func TestCatalogAddIndex(t *testing.T) {
	ctx, eng, cat, close := WithCatalog(t)
	defer close()
	tctx, commit, abort := eng.WithTransaction(ctx)
	defer abort()
	s, err := schema.SchemaOf(&TestTable{})
	require.NoError(t, err)
	s.WithName(s.Name() + "_index")
	t.Log("Index", s)
	opts := IndexOptions{
		Engine:   "pack",
		Driver:   "mem",
		PageSize: 1024,
	}
	require.NoError(t, cat.AddIndex(tctx, 2, 1, s, opts))
	require.NoError(t, commit())

	// list indexes
	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	keys, err := cat.ListIndexes(tctx, 1)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], uint64(2))

	// get index
	s2, opts2, err := cat.GetIndex(tctx, 2)
	require.NoError(t, err)
	require.NotNil(t, s2)
	require.Equal(t, s2.Name(), s.Name())
	require.Equal(t, s2.Hash(), s.Hash())
	require.Equal(t, opts2, opts)
	require.NoError(t, abort())

	// drop index
	tctx, commit, abort = eng.WithTransaction(ctx)
	defer abort()
	require.NoError(t, cat.DropIndex(tctx, 2))
	require.NoError(t, commit())

	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	keys, err = cat.ListIndexes(tctx, 1)
	require.NoError(t, err)
	require.Len(t, keys, 0)
	require.NoError(t, abort())

	// drop unknown index
	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	require.Error(t, cat.DropIndex(tctx, 1))
}

func TestCatalogAddStore(t *testing.T) {
	ctx, eng, cat, close := WithCatalog(t)
	defer close()
	tctx, commit, abort := eng.WithTransaction(ctx)
	defer abort()
	s, err := schema.SchemaOf(&TestTable{})
	require.NoError(t, err)
	s.WithName(s.Name() + "_store")
	t.Log("Store", s)
	opts := StoreOptions{
		Driver:   "mem",
		PageSize: 1024,
	}
	require.NoError(t, cat.AddStore(tctx, 1, s, opts))
	require.NoError(t, commit())

	// list stores
	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	keys, err := cat.ListStores(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], uint64(1))

	// get store
	s2, opts2, err := cat.GetStore(tctx, 1)
	require.NoError(t, err)
	require.NotNil(t, s2)
	require.Equal(t, s2.Name(), s.Name())
	require.Equal(t, s2.Hash(), s.Hash())
	require.Equal(t, opts2, opts)
	require.NoError(t, abort())

	// drop store
	tctx, commit, abort = eng.WithTransaction(ctx)
	defer abort()
	require.NoError(t, cat.DropStore(tctx, 1))
	require.NoError(t, commit())

	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	keys, err = cat.ListStores(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 0)
	require.NoError(t, abort())

	// drop unknown store
	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	require.Error(t, cat.DropStore(tctx, 1))
}

func TestCatalogAddEnum(t *testing.T) {
	ctx, eng, cat, close := WithCatalog(t)
	defer close()
	tctx, commit, abort := eng.WithTransaction(ctx)
	defer abort()
	enum := schema.NewEnumDictionary("enum")
	enum.Append("a", "b", "c")
	require.NoError(t, cat.AddEnum(tctx, enum))
	require.NoError(t, commit())

	// list enums
	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	keys, err := cat.ListEnums(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], enum.Tag())

	// get enum
	enum2, err := cat.GetEnum(tctx, enum.Tag())
	require.NoError(t, err)
	require.NotNil(t, enum2)
	require.Equal(t, enum2.Name(), enum.Name())
	require.Equal(t, enum2.Tag(), enum.Tag())
	require.Equal(t, enum2.Len(), enum.Len())
	require.NoError(t, abort())

	// drop enum
	tctx, commit, abort = eng.WithTransaction(ctx)
	defer abort()
	require.NoError(t, cat.DropEnum(tctx, enum.Tag()))
	require.NoError(t, commit())

	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	keys, err = cat.ListEnums(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 0)
	require.NoError(t, abort())

	// drop unknown enum
	tctx, _, abort = eng.WithTransaction(ctx)
	defer abort()
	require.Error(t, cat.DropEnum(tctx, 1))
}
