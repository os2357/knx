// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// TODO Design
// - handle schema evolution (latest schema is referenced by object, list of prev schemas?)
// - foreign tables + engines
// - enums
// - views
// - snapshots
// - streams
//
// buckets
// - options: key=name_hash, val=options
// - schemas: key=name_hash:schema_hash, val=schema
// - database
//   - name
//   - createdAt
//   - lastTxId -> state
// - tables
//   - name_hash
//     - name
//     - schema_hash
//     - last_id -> state
//     - num_tuples -> state
//     - opts -> options
// - stores
//   - name_hash
//     - name
//     - schema_hash
//     - num_keys -> state
//     - opts -> options
// - indexes
//   - name_hash
//     - name
//     - schema_hash
//     - table_hash
//     - type
//     - status (empty,rebuilding,ready) -> state
//     - opts -> options
// - enums
//   - name_hash
//     - name
//     - data (string values)
// - views (todo)
//   - name_hash
//     - name
//     - schema_hash
//     - query
//     - opts -> options
// - snapshots (todo)
// - streams (todo)
//

const (
	CATALOG_NAME    = "_catalog.db"
	CATALOG_TYPE    = "knoxdb.schemas.catalog.v1"
	CATALOG_VERSION = 1
)

var (
	// buckets
	databaseKey  = []byte("database")  // name, created_at, last_txid
	schemasKey   = []byte("schemas")   // tag:schema => serialized schema
	optionsKey   = []byte("options")   // tag => serialized options (db, table, store, view, ..)
	tablesKey    = []byte("tables")    // tag => name=str, schema=u64
	indexesKey   = []byte("indexes")   // tag => name=str, schema=u64, table=u64, status=u8
	viewsKey     = []byte("views")     // key => name=str, schema=u64, data=query
	enumsKey     = []byte("enums")     // key => name=str, data=package (id, string)
	storesKey    = []byte("stores")    // key => name=str, schema=u64
	snapshotsKey = []byte("snapshots") // key => serialized snapshot config
	streamsKey   = []byte("streams")   // key => serialized stream config

	// keys
	schemaKey = []byte("schema")
	tableKey  = []byte("table")
	nameKey   = []byte("name")
	statusKey = []byte("status")
	dataKey   = []byte("data")
)

var DefaultDatabaseOptions = DatabaseOptions{
	Path:            "./db",
	Driver:          "bolt",
	PageSize:        1024,
	PageFill:        0.8,
	CacheSize:       16 * 1 << 20,
	WalSegmentSize:  128 << 20,
	WalRecoveryMode: wal.RecoveryModeTruncate,
}

// knoxdb.schemas.catalog.v1
type Catalog struct {
	mu   sync.RWMutex
	db   store.DB
	name string
}

func NewCatalog(name string) *Catalog {
	return &Catalog{
		name: name,
	}
}

func (c *Catalog) Create(ctx context.Context, opts DatabaseOptions) error {
	path := filepath.Join(opts.Path, c.name, CATALOG_NAME)
	opts.Logger.Debugf("Creating catalog at %s", path)
	db, err := store.Create(opts.Driver, path, opts.ToDriverOpts())
	if err != nil {
		if store.IsError(err, store.ErrDbExists) {
			return fmt.Errorf("%s: creating catalog: %w", c.name, ErrDatabaseExists)
		}
		return fmt.Errorf("%s: creating catalog: %w", c.name, err)
	}
	err = db.SetManifest(store.Manifest{
		Name:    c.name,
		Schema:  CATALOG_TYPE,
		Version: CATALOG_VERSION,
	})
	if err != nil {
		_ = db.Close()
		return err
	}
	c.db = db

	// init table storage
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
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
		if _, err := tx.Root().CreateBucket(key); err != nil {
			return err
		}
	}

	return nil
}

func (c *Catalog) Open(ctx context.Context, opts DatabaseOptions) error {
	opts = DefaultDatabaseOptions.Merge(opts)
	path := filepath.Join(opts.Path, c.name, CATALOG_NAME)

	opts.Logger.Debugf("Opening catalog at %s", path)
	db, err := store.Open(opts.Driver, path, opts.ToDriverOpts())
	if err != nil {
		if store.IsError(err, store.ErrDbDoesNotExist) {
			return ErrNoDatabase
		}
		opts.Logger.Errorf("opening catalog %s: %v", c.name, err)
		return ErrDatabaseCorrupt
	}

	mft, err := db.Manifest()
	if err != nil {
		opts.Logger.Errorf("missing manifest: %v", err)
		return ErrDatabaseCorrupt
	}
	err = mft.Validate(c.name, "*", CATALOG_TYPE, CATALOG_VERSION)
	if err != nil {
		opts.Logger.Errorf("schema mismatch: %v", err)
		return schema.ErrSchemaMismatch
	}
	c.db = db

	return nil
}

func (c *Catalog) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil
	}
	err := c.db.Close()
	if err != nil {
		return err
	}
	c.db = nil
	return nil
}

func (c *Catalog) GetSchema(ctx context.Context, key uint64) (*schema.Schema, error) {
	tx, err := GetTransaction(ctx).CatalogTx(c.db, false)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket(schemasKey)
	if bucket == nil {
		return nil, ErrDatabaseCorrupt
	}
	buf := bucket.Get(Key64Bytes(key))
	if buf == nil {
		return nil, ErrNoKey
	}
	s := schema.NewSchema()
	if err := s.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return s, nil
}

func (c *Catalog) PutSchema(ctx context.Context, s *schema.Schema) error {
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(schemasKey)
	if bucket == nil {
		return ErrDatabaseCorrupt
	}
	buf, err := s.MarshalBinary()
	if err != nil {
		return err
	}
	return bucket.Put(Key64Bytes(s.Hash()), buf)
}

func (c *Catalog) DelSchema(ctx context.Context, key uint64) error {
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(schemasKey)
	if bucket == nil {
		return ErrDatabaseCorrupt
	}
	return bucket.Delete(Key64Bytes(key))
}

func (c *Catalog) GetOptions(ctx context.Context, key uint64, opts any) error {
	s, err := schema.SchemaOf(opts)
	if err != nil {
		return err
	}
	tx, err := GetTransaction(ctx).CatalogTx(c.db, false)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(optionsKey)
	if bucket == nil {
		return ErrDatabaseCorrupt
	}
	buf := bucket.Get(Key64Bytes(key))
	if buf == nil {
		return ErrNoKey
	}
	dec := schema.NewDecoder(s)
	if err := dec.Decode(buf, opts); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) PutOptions(ctx context.Context, key uint64, opts any) error {
	s, err := schema.SchemaOf(opts)
	if err != nil {
		return err
	}
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(optionsKey)
	if bucket == nil {
		return ErrDatabaseCorrupt
	}
	enc := schema.NewEncoder(s)
	buf, err := enc.Encode(opts, nil)
	if err != nil {
		return err
	}
	return bucket.Put(Key64Bytes(key), buf)
}

func (c *Catalog) DelOptions(ctx context.Context, key uint64) error {
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(optionsKey)
	if bucket == nil {
		return ErrDatabaseCorrupt
	}
	return bucket.Delete(Key64Bytes(key))
}

func (c *Catalog) ListTables(ctx context.Context) ([]uint64, error) {
	return c.listKeys(ctx, tablesKey)
}

func (c *Catalog) GetTable(ctx context.Context, key uint64) (s *schema.Schema, o TableOptions, err error) {
	var tx store.Tx
	tx, err = GetTransaction(ctx).CatalogTx(c.db, false)
	if err != nil {
		return
	}
	bucket := tx.Bucket(tablesKey)
	if bucket == nil {
		err = ErrDatabaseCorrupt
		return
	}
	bucket = bucket.Bucket(Key64Bytes(key))
	if bucket == nil {
		err = ErrNoTable
		return
	}
	skey := bucket.Get(schemaKey)
	if skey == nil {
		err = ErrNoKey
		return
	}
	s, err = c.GetSchema(ctx, Key64(skey))
	if err != nil {
		return
	}
	err = c.GetOptions(ctx, key, &o)
	return
}

func (c *Catalog) AddTable(ctx context.Context, key uint64, s *schema.Schema, o TableOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.PutSchema(ctx, s); err != nil {
		return err
	}

	if err := c.PutOptions(ctx, key, &o); err != nil {
		return err
	}

	// create table bucket, add table name and current schema
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(tablesKey)
	if bucket == nil {
		return ErrDatabaseCorrupt
	}
	bucket, err = bucket.CreateBucketIfNotExists(Key64Bytes(key))
	if err != nil {
		return err
	}
	if err := bucket.Put(schemaKey, Key64Bytes(s.Hash())); err != nil {
		return err
	}
	if err := bucket.Put(nameKey, []byte(s.Name())); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) DropTable(ctx context.Context, key uint64) error {
	// TODO: we don't have a reference to previous schema versions/hashes for removal
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	tables := tx.Bucket(tablesKey)
	if tables == nil {
		return ErrDatabaseCorrupt
	}
	bucket := tables.Bucket(Key64Bytes(key))
	if bucket == nil {
		return ErrNoTable
	}
	skey := bucket.Get(schemaKey)
	if skey == nil {
		return ErrNoKey
	}
	if err := tables.DeleteBucket(Key64Bytes(key)); err != nil {
		return err
	}
	if err := c.DelOptions(ctx, key); err != nil {
		return err
	}
	if err := c.DelSchema(ctx, Key64(skey)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) GetIndex(ctx context.Context, key uint64) (s *schema.Schema, o IndexOptions, err error) {
	var tx store.Tx
	tx, err = GetTransaction(ctx).CatalogTx(c.db, false)
	if err != nil {
		return
	}
	indexes := tx.Bucket(indexesKey)
	if indexes == nil {
		err = ErrDatabaseCorrupt
		return
	}
	bucket := indexes.Bucket(Key64Bytes(key))
	if bucket == nil {
		err = ErrNoIndex
		return
	}
	skey := bucket.Get(schemaKey)
	if skey == nil {
		err = ErrNoKey
		return
	}
	s, err = c.GetSchema(ctx, Key64(skey))
	if err != nil {
		return
	}
	err = c.GetOptions(ctx, key, &o)
	return
}

func (c *Catalog) ListIndexes(ctx context.Context, key uint64) ([]uint64, error) {
	tx, err := GetTransaction(ctx).CatalogTx(c.db, false)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket(indexesKey)
	if bucket == nil {
		return nil, ErrDatabaseCorrupt
	}
	res := make([]uint64, 0)
	err = bucket.ForEachBucket(func(k []byte, b store.Bucket) error {
		tkey := b.Get(tableKey)
		if tkey == nil {
			return ErrNoKey
		}
		if Key64(tkey) == key {
			res = append(res, Key64(k))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Catalog) AddIndex(ctx context.Context, ikey, tkey uint64, s *schema.Schema, o IndexOptions) error {
	if err := c.PutSchema(ctx, s); err != nil {
		return err
	}

	if err := c.PutOptions(ctx, ikey, &o); err != nil {
		return err
	}
	// create index bucket, add index name, current schema, table hash
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(indexesKey)
	if bucket == nil {
		return ErrDatabaseCorrupt
	}
	bucket, err = bucket.CreateBucket(Key64Bytes(ikey))
	if err != nil {
		return err
	}
	if err := bucket.Put(schemaKey, Key64Bytes(s.Hash())); err != nil {
		return err
	}
	if err := bucket.Put(nameKey, []byte(s.Name())); err != nil {
		return err
	}
	if err := bucket.Put(tableKey, Key64Bytes(tkey)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) DropIndex(ctx context.Context, key uint64) error {
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	indexes := tx.Bucket(indexesKey)
	if indexes == nil {
		return ErrDatabaseCorrupt
	}
	bucket := indexes.Bucket(Key64Bytes(key))
	if bucket == nil {
		return ErrNoIndex
	}
	skey := bucket.Get(schemaKey)
	if skey == nil {
		return ErrNoKey
	}
	if err := indexes.DeleteBucket(Key64Bytes(key)); err != nil {
		return err
	}
	if err := c.DelOptions(ctx, key); err != nil {
		return err
	}
	if err := c.DelSchema(ctx, Key64(skey)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) ListStores(ctx context.Context) ([]uint64, error) {
	return c.listKeys(ctx, storesKey)
}

func (c *Catalog) GetStore(ctx context.Context, key uint64) (s *schema.Schema, o StoreOptions, err error) {
	var tx store.Tx
	tx, err = GetTransaction(ctx).CatalogTx(c.db, false)
	if err != nil {
		return
	}
	stores := tx.Bucket(storesKey)
	if stores == nil {
		err = ErrDatabaseCorrupt
		return
	}
	bucket := stores.Bucket(Key64Bytes(key))
	if bucket == nil {
		err = ErrNoStore
		return
	}
	skey := bucket.Get(schemaKey)
	if skey == nil {
		err = ErrNoKey
		return
	}
	s, err = c.GetSchema(ctx, Key64(skey))
	if err != nil {
		return
	}
	err = c.GetOptions(ctx, key, &o)
	return
}

func (c *Catalog) AddStore(ctx context.Context, key uint64, s *schema.Schema, o StoreOptions) error {
	if err := c.PutSchema(ctx, s); err != nil {
		return err
	}

	if err := c.PutOptions(ctx, key, &o); err != nil {
		return err
	}

	// create store bucket, add name and current schema
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(storesKey)
	if bucket == nil {
		return ErrDatabaseCorrupt
	}
	bucket, err = bucket.CreateBucket(Key64Bytes(key))
	if err != nil {
		return err
	}
	if err := bucket.Put(schemaKey, Key64Bytes(s.Hash())); err != nil {
		return err
	}
	if err := bucket.Put(nameKey, []byte(s.Name())); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) DropStore(ctx context.Context, key uint64) error {
	// TODO: we don't have a reference to previous schema versions/hashes for removal
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	stores := tx.Bucket(storesKey)
	if stores == nil {
		return ErrDatabaseCorrupt
	}
	bucket := stores.Bucket(Key64Bytes(key))
	if bucket == nil {
		return ErrNoStore
	}
	skey := bucket.Get(schemaKey)
	if skey == nil {
		return ErrNoKey
	}
	if err := stores.DeleteBucket(Key64Bytes(key)); err != nil {
		return err
	}
	if err := c.DelOptions(ctx, key); err != nil {
		return err
	}
	if err := c.DelSchema(ctx, Key64(skey)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) ListEnums(ctx context.Context) ([]uint64, error) {
	return c.listKeys(ctx, enumsKey)
}

func (c *Catalog) GetEnum(ctx context.Context, key uint64) (e *schema.EnumDictionary, err error) {
	var tx store.Tx
	tx, err = GetTransaction(ctx).CatalogTx(c.db, false)
	if err != nil {
		return
	}
	stores := tx.Bucket(enumsKey)
	if stores == nil {
		err = ErrDatabaseCorrupt
		return
	}
	bucket := stores.Bucket(Key64Bytes(key))
	if bucket == nil {
		err = ErrNoStore
		return
	}
	name := bucket.Get(nameKey)
	data := bucket.Get(dataKey)
	if name == nil || data == nil {
		err = ErrNoKey
		return
	}
	e = schema.NewEnumDictionary(string(name))
	err = e.UnmarshalBinary(data)
	return
}

func (c *Catalog) PutEnum(ctx context.Context, e *schema.EnumDictionary) error {
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	enums := tx.Bucket(enumsKey)
	if enums == nil {
		return ErrDatabaseCorrupt
	}
	bucket := enums.Bucket(Key64Bytes(e.Tag()))
	if bucket == nil {
		return ErrDatabaseCorrupt
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	return bucket.Put(dataKey, buf)
}

func (c *Catalog) AddEnum(ctx context.Context, e *schema.EnumDictionary) error {
	// create enum bucket, add enum name and data
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	enums := tx.Bucket(enumsKey)
	if enums == nil {
		return ErrDatabaseCorrupt
	}
	bucket, err := enums.CreateBucket(Key64Bytes(e.Tag()))
	if err != nil {
		return err
	}
	if err := bucket.Put(nameKey, []byte(e.Name())); err != nil {
		return err
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	if err := bucket.Put(dataKey, buf); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) DropEnum(ctx context.Context, key uint64) error {
	tx, err := GetTransaction(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	enums := tx.Bucket(enumsKey)
	if enums == nil {
		return ErrDatabaseCorrupt
	}
	if err := enums.DeleteBucket(Key64Bytes(key)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) listKeys(ctx context.Context, bucketKey []byte) ([]uint64, error) {
	tx, err := GetTransaction(ctx).CatalogTx(c.db, false)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket(bucketKey)
	if bucket == nil {
		return nil, ErrDatabaseCorrupt
	}
	res := make([]uint64, 0)
	err = bucket.ForEachBucket(func(k []byte, b store.Bucket) error {
		res = append(res, Key64(k))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
