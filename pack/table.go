// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - materialized views for storing expensive query results
// - complex conditions using AND/OR and brackets
// - ASC/DESC query ORDER with LIMIT
// - complex conditions using AND/OR and brackets
// - arithmetic expressions and selectors (sum, mean, median, std, min, max, topN, bottomN)
//

// Query features
// - GROUP BY and HAVING (special condition to filter groups after aggregation)
// - aggregate functions sum, mean, median, std,
// - selectors (first, last, min, max, topN, bottomN)
// - arithmetic expressions (simple math)
// - PARTITION BY analytics (keep rows unlike GROUP BY which aggregates)

// Performance and safety
// - thread safety
// - concurrent/background pack compaction/storage using shadow journal
// - concurrent index build
// - flush journal after each insert/update/delete for data persistence is sloooow
// - auto-create indexes when index keyword is used in struct tag for CreateTable
// - more indexes (b-tree?)

// Design concepts
// - columnar design with type-specific multi-level compression
// - column groups (i.e. matrix to keep relation within a row)
// - zonemaps keeping min/max for each column in a partition
// - buffer pools for packs and slices
// - pack caches and cache-sensitive pack query scheduler

package pack

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/cache"
	"blockwatch.cc/knoxdb/cache/lru"
	"blockwatch.cc/knoxdb/store"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

const (
	idFieldName             = "I"
	defaultCacheSize        = 128 // keep 128 unpacked partitions in memory (per table/index)
	defaultPackSizeLog2     = 16  // 64k entries per partition
	defaultJournalFillLevel = 50  // keep space for extension
)

var (
	optsKey             = []byte("_options")
	fieldsKey           = []byte("_fields")
	metaKey             = []byte("_meta")
	infoKey             = []byte("_packs")
	indexesKey          = []byte("_indexes")
	journalKey   uint32 = 0xFFFFFFFF
	tombstoneKey uint32 = 0xFFFFFFFE

	DefaultOptions = Options{
		PackSizeLog2:    defaultPackSizeLog2, // 64k entries
		JournalSizeLog2: 17,                  // 128k entries
		CacheSize:       defaultCacheSize,    // in packs
		FillLevel:       90,                  // boltdb fill level to limit reallocations
	}
	NoOptions = Options{}
)

type Tombstone struct {
	Id uint64 `knox:"I,pk,snappy"`
}

type Options struct {
	PackSizeLog2    int `json:"pack_size_log2"`
	JournalSizeLog2 int `json:"journal_size_log2"`
	CacheSize       int `json:"cache_size"`
	FillLevel       int `json:"fill_level"`
}

func (o *Options) MergeDefaults() {
	o.CacheSize = util.NonZero(o.CacheSize, DefaultOptions.CacheSize)
	o.PackSizeLog2 = util.NonZero(o.PackSizeLog2, DefaultOptions.PackSizeLog2)
	o.JournalSizeLog2 = util.NonZero(o.JournalSizeLog2, DefaultOptions.JournalSizeLog2)
	o.FillLevel = util.NonZero(o.FillLevel, DefaultOptions.FillLevel)
}

func (o Options) Check() error {
	if o.PackSizeLog2 < 10 || o.PackSizeLog2 > 22 {
		return fmt.Errorf("PackSizeLog2 %d out of range [10, 22]", o.PackSizeLog2)
	}
	if o.JournalSizeLog2 < 10 || o.JournalSizeLog2 > 22 {
		return fmt.Errorf("JournalSizeLog2 %d out of range [10, 22]", o.JournalSizeLog2)
	}
	if o.CacheSize > 64*1024 {
		return fmt.Errorf("CacheSize %d out of range [0, 64k]", o.CacheSize)
	}
	if o.FillLevel < 10 || o.FillLevel > 100 {
		return fmt.Errorf("FillLevel %d out of range [10, 100]", o.FillLevel)
	}
	return nil
}

type TableMeta struct {
	Sequence uint64 `json:"sequence"`
	Rows     int64  `json:"rows"`
	dirty    bool   `json:"-"`
}

type Table struct {
	name      string
	opts      Options
	fields    FieldList
	indexes   IndexList
	meta      TableMeta
	db        *DB
	cache     cache.Cache // keep decoded packs for query/updates
	journal   *Package    // insert/update log
	tombstone *Package    // delete log
	packidx   *PackIndex  // in-memory list of pack and block info
	key       []byte
	metakey   []byte
	packPool  *sync.Pool // buffer pool for new packages
	pkPool    *sync.Pool
	stats     TableStats
	mu        sync.RWMutex
}

func (d *DB) CreateTable(name string, fields FieldList, opts Options) (*Table, error) {
	opts.MergeDefaults()
	if err := opts.Check(); err != nil {
		return nil, err
	}
	t := &Table{
		name:   name,
		opts:   opts,
		fields: fields,
		meta: TableMeta{
			Sequence: 0,
		},
		db:      d,
		indexes: make(IndexList, 0),
		packidx: NewPackIndex(nil, fields.PkIndex()),
		key:     []byte(name),
		metakey: []byte(name + "_meta"),
		pkPool: &sync.Pool{
			New: func() interface{} { return make([]uint64, 0) },
		},
	}
	t.packPool = &sync.Pool{
		New: t.makePackage,
	}
	err := d.db.Update(func(dbTx store.Tx) error {
		b := dbTx.Bucket(t.key)
		if b != nil {
			return ErrTableExists
		}
		_, err := dbTx.Root().CreateBucketIfNotExists(t.key)
		if err != nil {
			return err
		}
		meta, err := dbTx.Root().CreateBucketIfNotExists(t.metakey)
		if err != nil {
			return err
		}
		_, err = meta.CreateBucketIfNotExists(infoKey)
		if err != nil {
			return err
		}
		buf, err := json.Marshal(t.opts)
		if err != nil {
			return err
		}
		err = meta.Put(optsKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(t.fields)
		if err != nil {
			return err
		}
		err = meta.Put(fieldsKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(t.indexes)
		if err != nil {
			return err
		}
		err = meta.Put(indexesKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = meta.Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.journal = NewPackage(1 << uint(t.opts.JournalSizeLog2))
		if err := t.journal.InitFields(fields, nil); err != nil {
			return err
		}
		t.journal.key = journalKey
		_, err = storePackTx(dbTx, t.metakey, t.journal.Key(), t.journal, defaultJournalFillLevel)
		if err != nil {
			return err
		}
		t.tombstone = NewPackage(1 << uint(t.opts.JournalSizeLog2))
		if err := t.tombstone.InitType(Tombstone{}); err != nil {
			return err
		}
		t.tombstone.key = tombstoneKey
		_, err = storePackTx(dbTx, t.metakey, t.tombstone.Key(), t.tombstone, defaultJournalFillLevel)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if t.opts.CacheSize > 0 {
		t.cache, err = lru.New2QWithEvict(int(t.opts.CacheSize), t.onEvictedPackage)
		if err != nil {
			return nil, err
		}
	} else {
		t.cache = cache.NewNoCache()
	}
	log.Debugf("Created table %s", name)
	d.tables[name] = t
	return t, nil
}

func (d *DB) CreateTableIfNotExists(name string, fields FieldList, opts Options) (*Table, error) {
	t, err := d.CreateTable(name, fields, opts)
	if err != nil {
		if err != ErrTableExists {
			return nil, err
		}
		t, err = d.Table(name, opts)
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}

func (d *DB) DropTable(name string) error {
	t, err := d.Table(name)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	idxnames := make([]string, len(t.indexes))
	for i, idx := range t.indexes {
		idxnames[i] = idx.Name
	}
	for _, v := range idxnames {
		if err := t.DropIndex(v); err != nil {
			return err
		}
	}
	t.cache.Purge()
	err = d.db.Update(func(dbTx store.Tx) error {
		err = dbTx.Root().DeleteBucket([]byte(name))
		if err != nil {
			return err
		}
		return dbTx.Root().DeleteBucket([]byte(name + "_meta"))
	})
	if err != nil {
		return err
	}
	delete(d.tables, t.name)
	t = nil
	return nil
}

func (d *DB) Table(name string, opts ...Options) (*Table, error) {
	if t, ok := d.tables[name]; ok {
		return t, nil
	}
	if len(opts) > 0 {
		log.Debugf("Opening table %s with opts %#v", name, opts[0])
	} else {
		log.Debugf("Opening table %s with default opts", name)
	}
	t := &Table{
		name:    name,
		db:      d,
		key:     []byte(name),
		metakey: []byte(name + "_meta"),
		pkPool: &sync.Pool{
			New: func() interface{} { return make([]uint64, 0) },
		},
	}
	t.packPool = &sync.Pool{
		New: t.makePackage,
	}
	err := d.db.View(func(dbTx store.Tx) error {
		b := dbTx.Bucket(t.metakey)
		if b == nil {
			return ErrNoTable
		}
		buf := b.Get(optsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing options for table %s", name)
		}
		err := json.Unmarshal(buf, &t.opts)
		if err != nil {
			return err
		}
		buf = b.Get(fieldsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing fields for table %s", name)
		}
		err = json.Unmarshal(buf, &t.fields)
		if err != nil {
			return fmt.Errorf("pack: cannot read fields for table %s: %v", name, err)
		}
		buf = b.Get(indexesKey)
		if buf == nil {
			return fmt.Errorf("pack: missing indexes for table %s", name)
		}
		err = json.Unmarshal(buf, &t.indexes)
		if err != nil {
			return fmt.Errorf("pack: cannot read indexes for table %s: %v", name, err)
		}
		buf = b.Get(metaKey)
		if buf == nil {
			return fmt.Errorf("pack: missing metadata for table %s", name)
		}
		err = json.Unmarshal(buf, &t.meta)
		if err != nil {
			return fmt.Errorf("pack: cannot read metadata for table %s: %v", name, err)
		}
		t.journal, err = loadPackTx(dbTx, t.metakey, bytekey(journalKey), nil)
		if err != nil {
			return fmt.Errorf("pack: cannot open journal for table %s: %v", name, err)
		}
		t.journal.InitFields(t.fields, nil)
		log.Debugf("pack: %s table loaded journal with %d entries", name, t.journal.Len())
		t.tombstone, err = loadPackTx(dbTx, t.metakey, bytekey(tombstoneKey), nil)
		if err != nil {
			return fmt.Errorf("pack: cannot open tombstone for table %s: %v", name, err)
		}
		t.tombstone.InitType(Tombstone{})
		log.Debugf("pack: %s table loaded tombstone with %d entries", name, t.tombstone.Len())

		return t.loadPackInfo(dbTx)
	})
	if err != nil {
		return nil, err
	}
	cacheSize := t.opts.CacheSize
	if len(opts) > 0 {
		cacheSize = opts[0].CacheSize
		if opts[0].JournalSizeLog2 > 0 {
			t.opts.JournalSizeLog2 = opts[0].JournalSizeLog2
		}
	}
	if cacheSize > 0 {
		t.cache, err = lru.New2QWithEvict(int(cacheSize), t.onEvictedPackage)
		if err != nil {
			return nil, err
		}
	} else {
		t.cache = cache.NewNoCache()
	}

	needFlush := make([]*Index, 0)
	for _, idx := range t.indexes {
		if len(opts) > 1 {
			if err := t.OpenIndex(idx, opts[1]); err != nil {
				return nil, err
			}
		} else {
			if err := t.OpenIndex(idx); err != nil {
				return nil, err
			}
		}
		if idx.journal.Len() > 0 {
			needFlush = append(needFlush, idx)
		}
	}

	// FIXME: change index lookups to also use index journal
	// flush any previously stored index data; this is necessary because
	// index lookups are only implemented for non-journal packs
	if len(needFlush) > 0 {
		tx, err := t.db.Tx(true)
		if err != nil {
			return nil, err
		}

		defer tx.Rollback()
		for _, idx := range needFlush {
			if err := idx.FlushTx(context.Background(), tx); err != nil {
				return nil, err
			}
		}

		tx.Commit()
	}
	d.tables[name] = t
	return t, nil
}

func (t *Table) loadPackInfo(dbTx store.Tx) error {
	b := dbTx.Bucket(t.metakey)
	if b == nil {
		return ErrNoTable
	}
	packs := make(PackInfoList, 0)
	bi := b.Bucket(infoKey)
	if bi != nil {
		log.Debugf("pack: %s table loading package info from bucket", t.name)
		c := bi.Cursor()
		var err error
		for ok := c.First(); ok; ok = c.Next() {
			info := PackInfo{}
			err = info.UnmarshalBinary(c.Value())
			if err != nil {
				break
			}
			packs = append(packs, info)
			atomic.AddInt64(&t.stats.MetaBytesRead, int64(len(c.Value())))
		}
		if err != nil {
			packs = packs[:0]
			log.Errorf("pack: info decode for table %s pack %x: %v", t.name, c.Key(), err)
		} else {
			t.packidx = NewPackIndex(packs, t.fields.PkIndex())
			log.Debugf("pack: %s table loaded index data for %d packs", t.name, t.packidx.Len())
			return nil
		}
	}
	log.Warnf("pack: Corrupt or missing pack info for table %s! Scanning table. This may take a long time...", t.name)
	c := dbTx.Bucket(t.key).Cursor()
	pkg, err := t.journal.Clone(false, 0)
	if err != nil {
		return err
	}
	for ok := c.First(); ok; ok = c.Next() {
		err := pkg.UnmarshalBinary(c.Value())
		if err != nil {
			return fmt.Errorf("pack: cannot read %s/%x: %v", t.name, c.Key(), err)
		}
		pkg.SetKey(c.Key())
		// ignore journal and tombstone
		switch pkg.key {
		case journalKey, tombstoneKey:
			continue
		}
		info := pkg.Info()
		err = info.UpdateStats(pkg)
		if err != nil {
			log.Errorf("pack: table scan failed: %v", err)
			return err
		}
		packs = append(packs, info)
		atomic.AddInt64(&t.stats.MetaBytesRead, int64(len(c.Value())))
	}
	t.packidx = NewPackIndex(packs, t.fields.PkIndex())
	log.Debugf("pack: %s table scanned %d packages", t.name, t.packidx.Len())
	return nil
}

func (t *Table) storePackInfo(dbTx store.Tx) error {
	b := dbTx.Bucket(t.metakey)
	if b == nil {
		return ErrNoTable
	}
	hb := b.Bucket(infoKey)
	// remove headers for deleted packs, if any
	for _, v := range t.packidx.removed {
		log.Debugf("pack: %s table removing pack info %x", t.name, v)
		hb.Delete(bytekey(v))
	}
	t.packidx.removed = t.packidx.removed[:0]

	// store headers for new/updated packs
	for i := range t.packidx.packs {
		if !t.packidx.packs[i].dirty {
			continue
		}
		buf, err := t.packidx.packs[i].MarshalBinary()
		if err != nil {
			return err
		}
		if err := hb.Put(t.packidx.packs[i].KeyBytes(), buf); err != nil {
			return err
		}
		t.packidx.packs[i].dirty = false
		atomic.AddInt64(&t.stats.MetaBytesWritten, int64(len(buf)))
	}
	return nil
}

func (t *Table) Fields() FieldList {
	return t.fields
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Database() *DB {
	return t.db
}

func (t *Table) Options() Options {
	return t.opts
}

func (t *Table) Indexes() IndexList {
	return t.indexes
}

func (t *Table) Lock() {
	t.mu.Lock()
}

func (t *Table) Unlock() {
	t.mu.Unlock()
}

func (t *Table) Stats() TableStats {
	var s TableStats = t.stats
	s.TupleCount = t.meta.Rows
	s.PacksCount = int64(t.packidx.Len())
	s.PacksCached = int64(t.cache.Len())
	for _, idx := range t.indexes {
		s.IndexPacksCount += int64(idx.packidx.Len())
		s.IndexPacksCached += int64(idx.cache.Len())
	}
	return s
}

func (t *Table) NextSequence() uint64 {
	t.meta.Sequence++
	t.meta.dirty = true
	return t.meta.Sequence
}

func (t *Table) Insert(ctx context.Context, val interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.insertJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	// save journal and tombstone
	// 	if t.journal.IsDirty() {
	// 		tx, err := t.db.Tx(true)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		// be panic safe
	// 		defer tx.Rollback()
	// 		if err := t.flushJournalTx(ctx, tx); err != nil {
	// 			return err
	// 		}
	// 		// commit storage transaction
	// 		return tx.Commit()
	// 	}
	// }
	return nil
}

// unsafe when used concurrently, need to obtain lock _before_ starting bolt tx
func (t *Table) InsertTx(ctx context.Context, tx *Tx, val interface{}) error {
	if err := t.insertJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	//  else {
	// 	if err := t.flushJournalTx(ctx, tx); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func (t *Table) insertJournal(val interface{}) error {
	var batch []Item
	if v, ok := val.([]Item); ok {
		batch = v
	} else if i, ok := val.(Item); ok {
		batch = []Item{i}
	} else {
		return fmt.Errorf("pack: type %T does not implement Item interface", val)
	}
	atomic.AddInt64(&t.stats.InsertCalls, 1)

	var (
		lastmax  uint64
		needSort bool
		count    int64
	)
	if t.journal.Len() > 0 {
		lastmax, _ = t.journal.Uint64At(t.journal.pkindex, t.journal.Len()-1)
	}

	for _, v := range batch {
		// generate primary key when missing
		id := v.ID()
		if id == 0 {
			id = t.NextSequence()
			v.SetID(id)
		} else {
			// pk must not exist; also make sure we don't auto-generate any future
			// sequence number that may collide with this insert, yet allow out-of-order
			// inserts in general to support user-generated ids on update/insert/rollback
			if id > t.meta.Sequence {
				t.meta.Sequence = id
				t.meta.dirty = true
			}
		}
		needSort = needSort || id < lastmax
		lastmax = util.MaxU64(lastmax, id)

		// append to journal (this may or may not be sorted, depending on user input)
		if err := t.journal.Push(v); err != nil {
			return err
		}
		count++
	}
	t.meta.Rows += count
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.InsertedTuples, count)

	// keep journal sorted by pk
	if needSort {
		if err := t.journal.PkSort(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) InsertRow(ctx context.Context, row Row) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.appendPackIntoJournal(ctx, row.res.pkg, row.n, 1); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	// FIXME
	// 	// in-memory journal inserts are fast, but unsafe for data durability
	// 	//
	// 	// save journal and tombstone
	// 	if t.journal.IsDirty() {
	// 		tx, err := t.db.Tx(true)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		// be panic safe
	// 		defer tx.Rollback()
	// 		if err := t.flushJournalTx(ctx, tx); err != nil {
	// 			return err
	// 		}
	// 		// commit storage transaction
	// 		return tx.Commit()
	// 	}
	// }
	return nil
}

func (t *Table) InsertResult(ctx context.Context, res *Result) error {
	if res == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.appendPackIntoJournal(ctx, res.pkg, 0, res.pkg.Len()); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 		// save journal and tombstone
	// 		if t.journal.IsDirty() {
	// 			tx, err := t.db.Tx(true)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			// be panic safe
	// 			defer tx.Rollback()
	// 			if err := t.flushJournalTx(ctx, tx); err != nil {
	// 				return err
	// 			}
	// 			// commit storage transaction
	// 			return tx.Commit()
	// 		}
	// 	}
	return nil
}

// FIXME: only works for same table schema, requires pkg to be sorted by pk
func (t *Table) appendPackIntoJournal(ctx context.Context, pkg *Package, pos, n int) error {
	if pkg.Len() == 0 {
		return nil
	}

	firstid, _ := pkg.Uint64At(pkg.pkindex, 0)
	lastid, _ := pkg.Uint64At(pkg.pkindex, pkg.Len()-1)

	if firstid < t.meta.Sequence {
		return fmt.Errorf("pack: out-of-order pack insert is not supported %d < %d",
			firstid, t.meta.Sequence)
	}

	if err := t.journal.AppendFrom(pkg, pos, n, true); err != nil {
		return err
	}

	t.meta.Sequence = lastid
	t.meta.Rows += int64(n)
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.InsertCalls, 1)
	atomic.AddInt64(&t.stats.InsertedTuples, int64(n))
	return nil

}

func (t *Table) Update(ctx context.Context, val interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.updateJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	// save journal and tombstone
	// 	if t.journal.IsDirty() {
	// 		tx, err := t.db.Tx(true)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		// be panic safe
	// 		defer tx.Rollback()
	// 		if err := t.flushJournalTx(ctx, tx); err != nil {
	// 			return err
	// 		}
	// 		// commit storage transaction
	// 		return tx.Commit()
	// 	}
	// }
	return nil
}

func (t *Table) UpdateTx(ctx context.Context, tx *Tx, val interface{}) error {
	if err := t.updateJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	if err := t.flushJournalTx(ctx, tx); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (t *Table) updateJournal(val interface{}) error {
	var batch []Item
	if v, ok := val.([]Item); ok {
		batch = v
	} else if i, ok := val.(Item); ok {
		batch = []Item{i}
	} else {
		return fmt.Errorf("type %T does not implement Item interface", val)
	}

	atomic.AddInt64(&t.stats.UpdateCalls, 1)
	atomic.AddInt64(&t.stats.UpdatedTuples, int64(len(batch)))

	// sort for improved update performance
	SortItems(batch)

	var (
		lastmax  uint64
		needSort bool
	)

	// get the original journal pk slice for lookups
	col, _ := t.journal.Column(t.journal.pkindex)
	pk, _ := col.([]uint64)

	if len(pk) > 0 {
		lastmax = pk[len(pk)-1]
	}

	for i, v := range batch {
		// require primary key
		id := v.ID()
		if id == 0 {
			return fmt.Errorf("pack: missing primary key on item %d %#v", i, v)
		}

		// update records when already in original journal, otherwise append
		if off := vec.Uint64Slice(pk).Index(id, 0); off > -1 {
			if err := t.journal.ReplaceAt(off, v); err != nil {
				return err
			}
		} else {
			// FIXME: pk must exist
			needSort = needSort || id < lastmax
			lastmax = util.MaxU64(lastmax, id)
			if err := t.journal.Push(v); err != nil {
				return err
			}
		}
	}

	// FIXME: probably a noop since batch is sorted already
	if needSort {
		if err := t.journal.PkSort(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) Delete(ctx context.Context, q Query) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return 0, ctx.Err()
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	q.Fields = FieldList{t.Fields().Pk()}
	res, err := t.QueryTx(ctx, tx, q)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	col, err := res.Uint64Column(t.Fields().Pk().Name)
	if err != nil {
		return 0, err
	}

	if err := t.DeleteIdsTx(ctx, tx, col); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return int64(len(col)), nil
}

func (t *Table) DeleteIds(ctx context.Context, val []uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.deleteJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	// save journal and tombstone
	// 	if t.journal.IsDirty() {
	// 		tx, err := t.db.Tx(true)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		// be panic safe
	// 		defer tx.Rollback()
	// 		if err := t.flushJournalTx(ctx, tx); err != nil {
	// 			return err
	// 		}
	// 		// commit storage transaction
	// 		return tx.Commit()
	// 	}
	// }

	return nil
}

func (t *Table) DeleteIdsTx(ctx context.Context, tx *Tx, val []uint64) error {
	if err := t.deleteJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	if err := t.flushJournalTx(ctx, tx); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func (t *Table) deleteJournal(ids []uint64) error {
	atomic.AddInt64(&t.stats.DeleteCalls, 1)

	var count int64
	for _, v := range ids {
		// require primary key
		if v == 0 {
			continue
		}

		if err := t.tombstone.Push(Tombstone{Id: v}); err != nil {
			return err
		}
		count++
	}

	// Note: we don't check if ids actually exist, so row counter may be off
	// until journal/tombstone are flushed
	atomic.AddInt64(&t.stats.DeletedTuples, count)
	t.meta.Rows -= count
	t.meta.dirty = true

	// keep tombstone sorted
	if err := t.tombstone.PkSort(); err != nil {
		return err
	}
	return nil
}

func (t *Table) Close() error {
	log.Debugf("pack: closing %s table with %d/%d records", t.name,
		t.journal.Len(), t.tombstone.Len())
	t.mu.Lock()
	defer t.mu.Unlock()

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	// store table metadata
	if t.meta.dirty {
		buf, err := json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = tx.tx.Bucket(t.metakey).Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.meta.dirty = false
	}

	// save journal and tombstone
	if t.journal.IsDirty() {
		_, err = tx.storePack(t.metakey, t.journal.Key(), t.journal, defaultJournalFillLevel)
		if err != nil {
			return err
		}
	}
	if t.tombstone.IsDirty() {
		_, err = tx.storePack(t.metakey, t.tombstone.Key(), t.tombstone, defaultJournalFillLevel)
		if err != nil {
			return err
		}
	}

	// store pack headers
	if err := t.storePackInfo(tx.tx); err != nil {
		return err
	}

	// close indexes
	for _, idx := range t.indexes {
		if err := idx.CloseTx(tx); err != nil {
			return err
		}
	}

	// commit storage transaction
	return tx.Commit()
}

func (t *Table) FlushJournal(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if err := t.flushJournalTx(ctx, tx); err != nil {
		return err
	}

	// store table metadata
	if t.meta.dirty {
		buf, err := json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = tx.tx.Bucket(t.metakey).Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.meta.dirty = false
	}

	return tx.Commit()
}

func (t *Table) flushJournalTx(ctx context.Context, tx *Tx) error {
	if t.journal.IsDirty() {
		n, err := tx.storePack(t.metakey, t.journal.Key(), t.journal, defaultJournalFillLevel)
		if err != nil {
			return err
		}
		atomic.AddInt64(&t.stats.JournalFlushedTuples, int64(t.journal.Len()))
		atomic.AddInt64(&t.stats.JournalPacksStored, 1)
		atomic.AddInt64(&t.stats.JournalBytesWritten, int64(n))
	}
	if t.tombstone.IsDirty() {
		n, err := tx.storePack(t.metakey, t.tombstone.Key(), t.tombstone, defaultJournalFillLevel)
		if err != nil {
			return err
		}
		atomic.AddInt64(&t.stats.TombstoneFlushedTuples, int64(t.tombstone.Len()))
		atomic.AddInt64(&t.stats.TombstonePacksStored, 1)
		atomic.AddInt64(&t.stats.TombstoneBytesWritten, int64(n))
	}
	return nil
}

func (t *Table) Flush(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if err := t.flushTx(ctx, tx); err != nil {
		return err
	}

	return tx.Commit()
}

// TODO
// - make concurrency safe to be called from background writer
// - allow step-wise execution (flush x number of journal entries per call)
// - support context cancellation
//
// merge journal entries into data partitions, repack, store, and update all indexes
func (t *Table) flushTx(ctx context.Context, tx *Tx) error {
	var (
		nParts, nBytes, nUpd, nAdd, nDel int                    // total stats counters
		pUpd, pAdd, pDel                 int                    // per-pack stats counters
		start                            time.Time = time.Now() // logging
	)

	// use id slices for faster lookups
	col, _ := t.tombstone.Column(0)
	dead, _ := col.([]uint64)
	col, _ = t.journal.Column(t.journal.pkindex)
	pk, _ := col.([]uint64)

	atomic.AddInt64(&t.stats.FlushCalls, 1)
	atomic.AddInt64(&t.stats.FlushedTuples, int64(t.journal.Len()+t.tombstone.Len()))

	// mark deleted entries in journal by setting id to zero
	// Note: both tombstone and journal id columns are sorted already
	log.Debugf("flush: %s table %d journal and %d tombstone records", t.name, len(pk), len(dead))
	for j, d, jl, dl := 0, 0, len(pk), len(dead); j < jl && d < dl; {
		for d < dl && dead[d] < pk[j] {
			d++
		}
		if d == dl {
			break
		}
		for j < jl && dead[d] > pk[j] {
			j++
		}
		if j == jl {
			break
		}
		if dead[d] == pk[j] {
			pk[j] = 0   // mark as processed (0 is safe to use here
			dead[d] = 0 // because its an invalid pk value)
			d++
			j++
			nDel++
		}
	}

	// walk journal/tombstone updates and group updates by pack
	var (
		pkg                            *Package // current target pack
		pkgsz                          int      = 1 << uint(t.opts.PackSizeLog2)
		jpos, tpos, nextpack, lastpack int      // slice or pack offset
		jlen, tlen                     int      = len(pk), len(dead)
		needSort                       bool
		nextmax, lastmax               uint64
		err                            error
	)

	// This algorithm works like a merge-sort over a sequence of sorted packs.
	for {
		// stop when all journal and tombstone entries have been processed
		if jpos >= jlen && tpos >= tlen {
			break
		}

		// skip deleted journal entries
		for ; jpos < jlen && pk[jpos] == 0; jpos++ {
		}

		// skip deleted tomstone entries
		for ; tpos < tlen && dead[tpos] == 0; tpos++ {
		}

		// init on each iteration, either from journal or tombstone
		var nextid uint64
		switch true {
		case jpos < jlen && tpos < tlen:
			nextid = util.MinU64(pk[jpos], dead[tpos])
		case jpos < jlen && tpos >= tlen:
			nextid = pk[jpos]
		case jpos >= jlen && tpos < tlen:
			nextid = dead[tpos]
		default:
			// should not happen
			break
		}

		// find best pack for insert/update/delete; skip when we're already
		// appending to a new pack
		if lastpack < t.packidx.Len() {
			nextpack, _, nextmax = t.findBestPack(nextid)
		}

		// store last pack when nextpack changes
		if lastpack != nextpack && pkg != nil {
			// saving a pack also deletes empty packs from storage!
			if pkg.IsDirty() {
				if needSort {
					pkg.PkSort()
				}
				n, err := t.storePack(tx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				// commit storage tx after each N written packs
				if tx.Pending() >= txMaxSize {
					// store pack headers
					if err := t.storePackInfo(tx.tx); err != nil {
						return err
					}
					if err := tx.CommitAndContinue(); err != nil {
						return err
					}
					// TODO: for a safe return we must also
					// - clear written journal/tombstone entries
					// - flush index (or implement index journal lookup)
					// - write table metadata and pack headers
					//
					// // check context before next turn
					// if interruptRequested(ctx) {
					// 	return ctx.Err()
					// }
				}
				// update next values after pack index has changed
				nextpack, _, nextmax = t.findBestPack(nextid)
			}
			// prepare for next pack
			lastpack = nextpack
			needSort = false
			lastmax = 0
			pkg = nil
			pAdd = 0
			pDel = 0
			pUpd = 0
		}

		// load or create the next pack
		if pkg == nil {
			if nextpack < t.packidx.Len() {
				pkg, err = t.loadPack(tx, t.packidx.packs[nextpack].Key, true, nil)
				if err != nil && err != ErrPackNotFound {
					return err
				}
				// keep largest id value to check if pack needs sort before storing it
				lastmax = nextmax
			}
			// start new pack
			if pkg == nil {
				lastpack = t.packidx.Len()
				lastmax = 0
				pkg = t.packPool.Get().(*Package)
				pkg.key = t.packidx.NextKey()
				pkg.cached = false
			}
		}

		// process tombstone entries for this pack
		if tpos < tlen && lastmax > 0 {
			// keep package position pointer outside the for loops
			var ppos int
			for ; tpos < tlen; tpos++ {
				// next pk to delete
				pkid := dead[tpos]

				// skip already processed tombstone entries
				if pkid == 0 {
					continue
				}

				// stop on pack boundary
				if pkid > lastmax {
					break
				}

				// find the next matching pkid to clear
				for plen := pkg.Len(); ppos < plen; ppos++ {
					rowid, _ := pkg.Uint64At(pkg.pkindex, ppos)
					if rowid > pkid {
						// pkid should have been found by now, if not found,
						// it does not exist in this pack, so we clear the
						// tombstone entry to avoid an infinite loop
						dead[tpos] = 0
						break
					}
					if rowid == pkid {
						// update indexes
						for _, idx := range t.indexes {
							if err := idx.RemoveTx(tx, pkg, ppos, 1); err != nil {
								return err
							}
						}
						// remove entry from pack
						pkg.Delete(ppos, 1)
						dead[tpos] = 0
						nDel++
						pDel++
						break
					}
				}
			}
		}

		// process journal entries for this pack
		for lastoffset := 0; jpos < jlen; jpos++ {
			// next pk to insert or update
			pkid := pk[jpos]

			// skip deleted journal entries
			if pkid == 0 {
				continue
			}

			// stop on pack boundary
			if best, _, _ := t.findBestPack(pkid); best != lastpack {
				break
			}

			// packs are sorted by pk, so we can safely skip ahead
			if offs := pkg.PkIndex(pkid, lastoffset); offs > -1 {
				// update existing row
				lastoffset = offs
				// replace index entries when data has changed
				for _, idx := range t.indexes {
					if !idx.Field.Type.EqualPacksAt(
						pkg, idx.Field.Index, offs,
						t.journal, idx.Field.Index, jpos,
					) {
						// remove index for original data
						if err := idx.RemoveTx(tx, pkg, offs, 1); err != nil {
							return err
						}
						// add new index entry
						if err := idx.AddTx(tx, t.journal, jpos, 1); err != nil {
							return err
						}
					}
				}

				// overwrite original
				if err := pkg.ReplaceFrom(t.journal, offs, jpos, 1); err != nil {
					return err
				}
				nUpd++
				pUpd++
			} else {
				// FIXME: will fragment when pks are non-monotone and previous packs
				//        are full (the next created pack will be appended at end of
				//        list). This especially hurts when deletion of a middle
				//        pack is combined with re-inserting its values later.
				if pkg.Len() >= pkgsz {
					bmin, bmax := t.packidx.MinMax(lastpack)
					// allow ooo-inserts by splitting full packs
					if lastpack < t.packidx.Len() && pkid > bmin && pkid < bmax {
						// warn, but continue appending below
						log.Warnf("flush: %s table splitting full pack %x (%d/%d) with min=%d max=%d on out-of-order insert pk %d",
							t.name, pkg.Key(), lastpack, t.packidx.Len(), bmin, bmax, pkid)
						if needSort {
							pkg.PkSort()
							needSort = false
						}
						n, err := t.splitPack(tx, pkg)
						if err != nil {
							return err
						}
						nParts++
						nBytes += n
						// commit tx after each N written packs
						if tx.Pending() >= txMaxSize {
							if err := t.storePackInfo(tx.tx); err != nil {
								return err
							}
							if err := tx.CommitAndContinue(); err != nil {
								return err
							}
							// TODO: for a safe return we must also
							// - clear written journal/tombstone entries
							// - flush index (or implement index journal lookup)
							// - write table metadata and pack headers
							//
							// // check context before next turn
							// if interruptRequested(ctx) {
							// 	return ctx.Err()
							// }
						}
						break

					} else {
						// store the pack here to update/insert it's headers into
						// t.packidx so that subsequent calls to findBestPack() in the
						// outer loop use fresh information
						if needSort {
							pkg.PkSort()
							needSort = false
						}
						n, err := t.storePack(tx, pkg)
						if err != nil {
							return err
						}
						nParts++
						nBytes += n
						// commit tx after each N written packs
						if tx.Pending() >= txMaxSize {
							if err := t.storePackInfo(tx.tx); err != nil {
								return err
							}
							if err := tx.CommitAndContinue(); err != nil {
								return err
							}
							// TODO: for a safe return we must also
							// - clear written journal/tombstone entries
							// - flush index (or implement index journal lookup)
							// - write table metadata and pack headers
							//
							// // check context before next turn
							// if interruptRequested(ctx) {
							// 	return ctx.Err()
							// }
						}
						break
					}
				}
				// append new row
				if err := pkg.AppendFrom(t.journal, jpos, 1, false); err != nil {
					return err
				}
				needSort = needSort || pkid < lastmax
				lastmax = util.MaxU64(lastmax, pkid)
				lastoffset = pkg.Len() - 1
				nAdd++
				pAdd++
				// add to indexes
				for _, idx := range t.indexes {
					if err := idx.AddTx(tx, pkg, lastoffset, 1); err != nil {
						return err
					}
				}
			}
		}
	}

	// store last processed pack
	if pkg != nil && pkg.IsDirty() {
		if needSort {
			pkg.PkSort()
		}
		n, err := t.storePack(tx, pkg)
		if err != nil {
			return err
		}
		nParts++
		nBytes += n
	}

	log.Debugf("flush: %s table %d packs add=%d del=%d total_size=%s in %s",
		t.name, nParts, nAdd, nDel, util.ByteSize(nBytes), time.Since(start))

	// flush indexes
	for _, idx := range t.indexes {
		if err := idx.FlushTx(ctx, tx); err != nil {
			return err
		}
	}

	// adjust row count if non-existing ids were inserted into tombstone
	if tlen > nDel {
		t.meta.Rows += int64(tlen - nDel)
		t.meta.dirty = true
	}

	// store table metadata
	if t.meta.dirty {
		buf, err := json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = tx.tx.Bucket(t.metakey).Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.meta.dirty = false
	}

	// store pack headers
	if err := t.storePackInfo(tx.tx); err != nil {
		return err
	}

	// clear journal and tombstone
	t.journal.Clear()
	t.tombstone.Clear()

	// save journal and tombstone
	return t.flushJournalTx(ctx, tx)
}

// Use pack index to find closest match for placing pkval based on min/max of the
// pk column. Handles gaps in the pk sequence inside packs and gaps between packs.
// Note that pk values are user-defined, so they may contain gaps and insert/update/
// delete may happen anywhere in a pack.
//
// Attention!
//
// Placement does not support clean out-of-order pk inserts or deletion+reinsert
// of the same keys. This will lead to pack fragmentation. See flushTx for more
// details.
//
//
// The placement algorithm works as follows:
// - keep lastpack when no pack exists (effectively == 0)
// - choose pack with pack.min <= val <= pack.max
// - choose pack with closest max < val
// - when val < min of first pack, choose first pack
//
func (t Table) findBestPack(pkval uint64) (int, uint64, uint64) {
	// will return 0 when list is empty, this ensures we initially stick
	// to the first pack until it's full
	bestpack, min, max := t.packidx.Best(pkval)

	// insert/update placement into an exsting pack's range always stays with this pack

	// hacker's delight trick for unsigned range checks
	// see https://stackoverflow.com/questions/17095324/fastest-way-to-determine-if-an-integer-is-between-two-integers-inclusive-with
	// pkval >= min && pkval <= max
	if t.packidx.Len() == 0 || pkval-min <= max-min {
		return bestpack, min, max
	}

	// make sure there's room in the selected pack
	if t.packidx.packs[bestpack].NValues >= 1<<uint(t.opts.PackSizeLog2) {
		return t.packidx.Len(), 0, 0 // triggers new pack creation
	}

	return bestpack, min, max
}

func (t *Table) Lookup(ctx context.Context, ids []uint64) (*Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return nil, ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return nil, err
	}
	res, err := t.LookupTx(ctx, tx, ids)
	tx.Rollback()
	return res, err
}

// unsafe when called concurrently! lock table _before_ starting bolt tx!
func (t *Table) LookupTx(ctx context.Context, tx *Tx, ids []uint64) (*Result, error) {
	res := &Result{
		fields: t.Fields(),                  // we return all fields
		pkg:    t.packPool.Get().(*Package), // clone full table structure
		table:  t,
	}

	atomic.AddInt64(&t.stats.QueryCalls, 1)

	q := NewQuery(t.name+".lookup", t)

	// make sorted and unique copy of ids and strip any zero (i.e. illegal) ids
	ids = vec.UniqueUint64Slice(ids)
	if len(ids) > 0 && ids[0] == 0 {
		ids = ids[1:]
	}
	maxRows := len(ids)

	// keep max lookup id
	var maxNonZeroId uint64
	if maxRows > 0 {
		maxNonZeroId = ids[maxRows-1]
	} else {
		return res, nil
	}

	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		q.Close()
	}()

	// since journal can contain deleted entries, mark them (i.e. overwrite
	// pkid with 0 == illegal value)
	if t.tombstone.Len() > 0 {
		for i, v := range ids {
			if v == 0 || t.tombstone.PkIndex(v, 0) >= 0 {
				ids[i] = 0
			} else {
				maxNonZeroId = v
			}
		}
	}

	// lookup journal first (Note: its sorted by pk)
	pkidx := t.journal.pkindex
	col, _ := t.journal.Column(pkidx)
	pk, _ := col.([]uint64)

	if t.journal.Len() > 0 {
		var last int
		for i, v := range ids {
			// no more matches in journal?
			if pk[len(pk)-1] < v || pk[last] > maxNonZeroId {
				break
			}
			j := t.journal.PkIndex(v, last)

			// not in journal
			if j < 0 {
				continue
			}

			// on match, copy result from journal
			if err := res.pkg.AppendFrom(t.journal, j, 1, true); err != nil {
				res.Close()
				return nil, err
			}
			q.stats.RowsMatched++

			// mark id as processed (set 0)
			ids[i] = 0
			last = j
		}
	}
	q.stats.JournalTime = time.Since(q.lap)

	// everything found in journal?, return early
	if maxRows == q.stats.RowsMatched {
		return res, nil
	}

	if q.stats.RowsMatched > 0 || t.tombstone.Len() > 0 {
		clean := make([]uint64, 0, len(ids))
		for _, v := range ids {
			if v != 0 {
				clean = append(clean, v)
				maxNonZeroId = v
			}
		}
		ids = clean
	}

	if len(ids) == 0 {
		return res, nil
	}

	// optimize for lookup of most recently added values
	q.lap = time.Now()
	var nextid int
	for _, nextpack := range q.MakePackLookupSchedule(ids, false) {
		// stop when all inputs are matched
		if maxRows == q.stats.RowsMatched {
			break
		}

		// stop when context is canceled
		if util.InterruptRequested(ctx) {
			res.Close()
			return nil, ctx.Err()
		}

		// continue with next pack

		// check pack headers again because now we have stripped some values
		// from the id lookup slice, so we may know better if the pack
		// matches or not

		// extract min/max values from pack header's pk column
		_, max := t.packidx.MinMax(nextpack)
		pkg, err := t.loadPack(tx, t.packidx.packs[nextpack].Key, true, q.reqfields)
		if err != nil {
			res.Close()
			return nil, err
		}
		q.stats.PacksScanned++

		col, _ := pkg.Column(pkidx)
		pk, _ := col.([]uint64)

		// packs are sorted by pk, ids does not contain zero values
		last := 0
		for i, v := range ids[nextid:] {
			// no more matches in this pack?
			if max < v || pk[last] > maxNonZeroId {
				break
			}
			j := pkg.PkIndex(v, last)

			// not in pack
			if j < 0 {
				continue
			}

			// on match, copy result from journal
			if err := res.pkg.AppendFrom(pkg, j, 1, true); err != nil {
				res.Close()
				return nil, err
			}
			nextid = i
			q.stats.RowsMatched++
			last = j
		}
	}
	q.stats.ScanTime = time.Since(q.lap)
	return res, nil
}

func (t *Table) Query(ctx context.Context, q Query) (*Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return nil, ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()
	if q.Order == OrderAsc {
		return t.QueryTx(ctx, tx, q)
	} else {
		return t.QueryTxDesc(ctx, tx, q)
	}
}

// NOTE
// ! This is not a proper query planning and execution engine. There's no
//   cost estimation, no ordering/sorting of conditions by cost and
//   no step-by-step or concurrent sub-query execution.
// ! unsafe when called concurrently! lock table _before_ starting bolt tx!
// ! All this algorithm does is
//   - match/lookup primary key values for indexed fields
//   - intersect multiple primary key lists to find common matches
//   - scan zonemaps of all partition headers
//   - scan relevant partitions and copy matching rows into result
//
func (t *Table) QueryTx(ctx context.Context, tx *Tx, q Query) (*Result, error) {
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	// check conditions match table
	if err := q.Compile(t); err != nil {
		return nil, err
	}

	// prepare journal match
	var jbits *vec.BitSet
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		q.Close()
		if jbits != nil {
			jbits.Close()
		}
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	jbits = q.Conditions.MatchPack(t.journal, PackInfo{})
	q.stats.JournalTime = time.Since(q.lap)

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return nil, err
	}

	// prepare result package
	pkg := t.packPool.Get().(*Package)
	pkg.KeepFields(q.reqfields)
	pkg.UpdateAliasesFrom(q.reqfields)

	res := &Result{
		fields: q.reqfields,
		pkg:    pkg,
		table:  t,
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return res, nil
	}

	// PACK SCAN (either using found pk ids or non-indexed conditions)
	// scan packs only if (a) index match returned any results or (b) no index exists
	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(false) {
			if util.InterruptRequested(ctx) {
				res.Close()
				return nil, ctx.Err()
			}

			// load pack from cache or storage, will be recycled on cache eviction
			pkg, err := t.loadPack(tx, t.packidx.packs[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				res.Close()
				return nil, err
			}
			q.stats.PacksScanned++

			// identify and copy matches
			bits := q.Conditions.MatchPack(pkg, t.packidx.packs[p])
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				for i := idx; i < idx+length; i++ {
					// skip broken entries
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					// skip deleted entries
					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					src := pkg
					index := i

					// when exists, use row version found in journal
					if jbits.Count() > 0 {
						if j := t.journal.PkIndex(pkid, 0); j >= 0 {
							// cross-check the journal row actually matches the cond
							if !jbits.IsSet(j) {
								continue
							}

							// remove match bit
							jbits.Clear(j)
							src = t.journal
							index = j
						}
					}

					if err := res.pkg.AppendFrom(src, index, 1, true); err != nil {
						bits.Close()
						res.Close()
						return nil, err
					}
					q.stats.RowsMatched++

					if q.Limit > 0 && q.stats.RowsMatched == q.Limit {
						bits.Close()
						break packloop
					}
				}
			}
			bits.Close()
		}
		q.stats.ScanTime = time.Since(q.lap)
		q.lap = time.Now()
	}

	// finalize on limit
	if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
		return res, nil
	}

	// after all packs have been scanned, add remaining rows from journal, if any
	for idx, length := jbits.Run(0); idx >= 0; idx, length = jbits.Run(idx + length) {
		for i := idx; i < idx+length; i++ {
			// skip broken entries
			pkid, err := t.journal.Uint64At(t.journal.pkindex, i)
			if err != nil {
				continue
			}

			// skip deleted entries
			if t.tombstone.PkIndex(pkid, 0) >= 0 {
				continue
			}

			if err := res.pkg.AppendFrom(t.journal, i, 1, true); err != nil {
				res.Close()
				return nil, err
			}
			q.stats.RowsMatched++

			if q.Limit > 0 && q.stats.RowsMatched == q.Limit {
				break
			}
		}
	}
	q.stats.JournalTime = time.Since(q.lap)

	return res, nil
}

// DESCENDING pk order algorithm
func (t *Table) QueryTxDesc(ctx context.Context, tx *Tx, q Query) (*Result, error) {
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	// check conditions match table
	if err := q.Compile(t); err != nil {
		return nil, err
	}

	// prepare journal query
	var jbits *vec.BitSet
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		q.Close()
		if jbits != nil {
			jbits.Close()
		}
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	// reverse the bitfield order for descending walk
	jbits = q.Conditions.MatchPack(t.journal, PackInfo{}).Reverse()
	q.stats.JournalTime = time.Since(q.lap)

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return nil, err
	}

	// prepare result package
	pkg := t.packPool.Get().(*Package)
	pkg.KeepFields(q.reqfields)
	pkg.UpdateAliasesFrom(q.reqfields)

	res := &Result{
		fields: q.reqfields,
		pkg:    pkg,
		table:  t,
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return res, nil
	}

	// find max pk across all saved packs (we assume any journal entry greater than this max
	// is new and hasn't been saved before; this assumption breaks when user-defined pk
	// values are smaller, so a user must flush the journal before query)
	_, maxPackedPk := t.packidx.GlobalMinMax()

	// before scanning packs, add 'new' rows from journal (i.e. pk > maxPackedPk),
	// walk in descending order
	for idx, length := jbits.Run(jbits.Len() - 1); idx >= 0; idx, length = jbits.Run(idx - length) {
		for i := idx; i > idx-length; i-- {
			// skip broken entries
			pkid, err := t.journal.Uint64At(t.journal.pkindex, i)
			if err != nil {
				continue
			}

			// skip previously stored entries (will be processed later)
			if pkid <= maxPackedPk {
				continue
			}

			// skip deleted entries
			if t.tombstone.PkIndex(pkid, 0) >= 0 {
				continue
			}

			if err := res.pkg.AppendFrom(t.journal, i, 1, true); err != nil {
				res.Close()
				return nil, err
			}
			q.stats.RowsMatched++
			jbits.Clear(i)

			if q.Limit > 0 && q.stats.RowsMatched == q.Limit {
				break
			}
		}
	}
	q.stats.JournalTime = time.Since(q.lap)

	// REVERSE PACK SCAN (either using found pk ids or non-indexed conditions)
	// reverse-scan packs only if (a) index match returned any results or (b) no index exists
	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(true) {
			if util.InterruptRequested(ctx) {
				res.Close()
				return nil, ctx.Err()
			}

			// load pack from cache or storage, will be recycled on cache eviction
			pkg, err := t.loadPack(tx, t.packidx.packs[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				res.Close()
				return nil, err
			}
			q.stats.PacksScanned++

			// identify and copy matches
			bits := q.Conditions.MatchPack(pkg, t.packidx.packs[p]).Reverse()
			for idx, length := bits.Run(bits.Len() - 1); idx >= 0; idx, length = bits.Run(idx - length) {
				for i := idx; i > idx-length; i-- {
					// skip broken entries
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					// skip deleted entries
					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					src := pkg
					index := i

					// when exists, use row from journal
					if jbits.Count() > 0 {
						if j := t.journal.PkIndex(pkid, 0); j >= 0 {
							// cross-check if the journal row actually matches the cond
							if !jbits.IsSet(j) {
								continue
							}
							jbits.Clear(j)
							src = t.journal
							index = j
						}
					}

					if err := res.pkg.AppendFrom(src, index, 1, true); err != nil {
						bits.Close()
						res.Close()
						return nil, err
					}
					q.stats.RowsMatched++

					if q.Limit > 0 && q.stats.RowsMatched == q.Limit {
						bits.Close()
						break packloop
					}
				}
			}
			bits.Close()
		}
		q.stats.ScanTime = time.Since(q.lap)
		q.lap = time.Now()
	}

	return res, nil
}

func (t *Table) Count(ctx context.Context, q Query) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return 0, ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()
	return t.CountTx(ctx, tx, q)
}

func (t *Table) CountTx(ctx context.Context, tx *Tx, q Query) (int64, error) {
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	if err := q.Compile(t); err != nil {
		return 0, err
	}

	var jbits *vec.BitSet

	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.stats.RowsMatched))
		jbits.Close()
		q.Close()
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	jbits = q.Conditions.MatchPack(t.journal, PackInfo{})
	q.stats.JournalTime = time.Since(q.lap)

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return 0, err
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return 0, nil
	}

	// PACK SCAN (either using found pk ids or non-indexed conditions)
	// scan packs only when index match returned any results of when no index exists
	if !q.IsEmptyMatch() {
		q.lap = time.Now()
		for _, p := range q.MakePackSchedule(false) {
			if util.InterruptRequested(ctx) {
				return int64(q.stats.RowsMatched), ctx.Err()
			}

			// load pack from cache or storage, will be recycled on cache eviction
			pkg, err := t.loadPack(tx, t.packidx.packs[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				return 0, err
			}
			q.stats.PacksScanned++

			// identify and count matches
			bits := q.Conditions.MatchPack(pkg, t.packidx.packs[p])
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				for i := idx; i < idx+length; i++ {
					// skip broken entries
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					// skip deleted entries
					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					// when exists, clear from journal bitmask
					if jbits.Count() > 0 {
						if j := t.journal.PkIndex(pkid, 0); j >= 0 {
							// cross-check if journal row actually matches the cond
							if !jbits.IsSet(j) {
								continue
							}
							jbits.Clear(j)
						}
					}

					q.stats.RowsMatched++
				}
			}
			bits.Close()
		}
		q.stats.ScanTime = time.Since(q.lap)
	}

	// after all packs have been scanned, add remaining rows from journal, if any
	q.stats.RowsMatched += int(jbits.Count())

	return int64(q.stats.RowsMatched), nil
}

func (t *Table) Stream(ctx context.Context, q Query, fn func(r Row) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if q.Order == OrderAsc {
		return t.StreamTx(ctx, tx, q, fn)
	} else {
		return t.StreamTxDesc(ctx, tx, q, fn)
	}
}

// Similar to QueryTx but returns each match via callback function to allow stream
// processing at low memory overheads.
func (t *Table) StreamTx(ctx context.Context, tx *Tx, q Query, fn func(r Row) error) error {
	atomic.AddInt64(&t.stats.StreamCalls, 1)

	if err := q.Compile(t); err != nil {
		return err
	}

	// prepare journal query
	var jbits *vec.BitSet
	defer func() {
		atomic.AddInt64(&t.stats.StreamedTuples, int64(q.stats.RowsMatched))
		if jbits != nil {
			jbits.Close()
		}
		q.Close()
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	jbits = q.Conditions.MatchPack(t.journal, PackInfo{})
	q.stats.JournalTime = time.Since(q.lap)

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return err
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return nil
	}

	// prepare result
	res := Result{fields: q.reqfields}

	// PACK SCAN (either using found pk ids or non-indexed conditions)
	// scan packs only when (a) index match returned any results or (b) when no index exists
	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(false) {
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}

			// load pack from cache or storage, will be recycled on cache eviction
			pkg, err := t.loadPack(tx, t.packidx.packs[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				return err
			}
			q.stats.PacksScanned++

			// identify and forward matches
			bits := q.Conditions.MatchPack(pkg, t.packidx.packs[p])
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				for i := idx; i < idx+length; i++ {
					// skip broken entries
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					// skip deleted entries
					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					// default to pack row
					res.pkg = pkg
					index := i

					// when exist, use journal row
					if jbits.Count() > 0 {
						if j := t.journal.PkIndex(pkid, 0); j >= 0 {
							// cross-check if journal row actually matches the cond
							if !jbits.IsSet(j) {
								continue
							}
							res.pkg = t.journal
							index = j
							jbits.Clear(j)
						}
					}

					// forward match
					if err := fn(Row{res: &res, n: index}); err != nil {
						bits.Close()
						return err
					}
					res.pkg = nil
					q.stats.RowsMatched++

					if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
						bits.Close()
						break packloop
					}
				}
			}
			bits.Close()
		}
		q.stats.ScanTime = time.Since(q.lap)
		q.lap = time.Now()
	}

	if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
		return nil
	}

	// after all packs have been scanned, add remaining rows from journal, if any
	res.pkg = t.journal
	for idx, length := jbits.Run(0); idx >= 0; idx, length = jbits.Run(idx + length) {
		for i := idx; i < idx+length; i++ {
			// skip broken entries
			pkid, err := t.journal.Uint64At(t.journal.pkindex, i)
			if err != nil {
				continue
			}

			// skip deleted entries
			if t.tombstone.PkIndex(pkid, 0) >= 0 {
				continue
			}

			// safety check
			// if !q.Conditions.MatchAt(t.journal, i) {
			// 	log.Errorf("MISMATCH in journal at pos %d with bitmap len=%d cnt=%d %x", i, jbits.Size(), jbits.Count(), jbits.Bytes())
			// 	dumper := t.journal.Clone(false, 1)
			// 	if err := dumper.AppendFrom(t.journal, i, 1, true); err != nil {
			// 		log.Error(err)
			// 	}
			// 	dumper.DumpData(os.Stdout, DumpModeHex, t.fields.Aliases())
			// 	log.Errorf("STREAM: %s table query %s with %d conditions", t.name, q.Name, len(q.Conditions))
			// 	for ic, c := range q.Conditions {
			// 		log.Errorf("STREAM: cond %d proc=%t match=%t: %v", ic, c.processed, q.Conditions[ic].MatchAt(t.journal, i), c.String())
			// 	}
			// }

			// forward match
			if err := fn(Row{res: &res, n: i}); err != nil {
				return err
			}
			q.stats.RowsMatched++

			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				return nil
			}
		}
	}
	q.stats.JournalTime += time.Since(q.lap)

	return nil
}

// DESCENDING order stream
func (t *Table) StreamTxDesc(ctx context.Context, tx *Tx, q Query, fn func(r Row) error) error {
	atomic.AddInt64(&t.stats.StreamCalls, 1)

	if err := q.Compile(t); err != nil {
		return err
	}

	// prepare journal query
	var jbits *vec.BitSet
	defer func() {
		atomic.AddInt64(&t.stats.StreamedTuples, int64(q.stats.RowsMatched))
		if jbits != nil {
			jbits.Close()
		}
		q.Close()
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	// reverse the bitfield order for descending walk
	jbits = q.Conditions.MatchPack(t.journal, PackInfo{}).Reverse()
	q.stats.JournalTime = time.Since(q.lap)

	// maybe run index query
	if err := q.QueryIndexes(ctx, tx); err != nil {
		return err
	}

	// early return
	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return nil
	}

	// find max pk across all saved packs (we assume any journal entry greater than this max
	// is new and hasn't been saved before; this assumption breaks when user-defined pk
	// values are smaller, so a user must flush the journal before query)
	_, maxPackedPk := t.packidx.GlobalMinMax()

	// prepare result
	res := Result{fields: q.reqfields}

	// before scanning packs, add 'new' rows from journal (i.e. pk > maxPackedPk),
	// walk in descending order
	res.pkg = t.journal

	for idx, length := jbits.Run(jbits.Len() - 1); idx >= 0; idx, length = jbits.Run(idx - length) {
		for i := idx; i > idx-length; i-- {
			// skip broken entries
			pkid, err := t.journal.Uint64At(t.journal.pkindex, i)
			if err != nil {
				continue
			}

			// skip previously stored entries (will be processed later)
			if pkid <= maxPackedPk {
				continue
			}

			// skip deleted entries
			if t.tombstone.PkIndex(pkid, 0) >= 0 {
				continue
			}

			// forward match
			if err := fn(Row{res: &res, n: i}); err != nil {
				return err
			}
			q.stats.RowsMatched++

			// clear matching bit
			jbits.Clear(i)

			if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
				return nil
			}
		}
	}
	q.stats.JournalTime += time.Since(q.lap)

	// reverse-scan packs only when (a) index match returned any results or (b) when no index exists
	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(true) {
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}

			// load pack from cache or storage, will be recycled on cache eviction
			pkg, err := t.loadPack(tx, t.packidx.packs[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				return err
			}
			q.stats.PacksScanned++

			// identify and forward matches
			bits := q.Conditions.MatchPack(pkg, t.packidx.packs[p]).Reverse()
			for idx, length := bits.Run(bits.Len() - 1); idx >= 0; idx, length = bits.Run(idx - length) {
				for i := idx; i > idx-length; i-- {
					// skip broken entries
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					// skip deleted entries
					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					res.pkg = pkg
					index := i

					// when exist, use journal row
					if jbits.Count() > 0 {
						if j := t.journal.PkIndex(pkid, 0); j >= 0 {
							if !jbits.IsSet(j) {
								continue
							}
							res.pkg = t.journal
							index = j
							jbits.Clear(j)
						}
					}

					// forward match
					if err := fn(Row{res: &res, n: index}); err != nil {
						bits.Close()
						return err
					}
					res.pkg = nil
					q.stats.RowsMatched++

					if q.Limit > 0 && q.stats.RowsMatched >= q.Limit {
						bits.Close()
						break packloop
					}
				}
			}
			bits.Close()
		}
		q.stats.ScanTime = time.Since(q.lap)
		q.lap = time.Now()
	}

	return nil
}

func (t *Table) StreamLookup(ctx context.Context, ids []uint64, fn func(r Row) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return t.StreamLookupTx(ctx, tx, ids, fn)
}

func (t *Table) StreamLookupTx(ctx context.Context, tx *Tx, ids []uint64, fn func(r Row) error) error {
	atomic.AddInt64(&t.stats.StreamCalls, 1)
	q := NewQuery(t.name+".stream-lookup", t)

	// make sorted and unique copy of ids and strip any zero (i.e. illegal) ids
	ids = vec.UniqueUint64Slice(ids)
	if len(ids) > 0 && ids[0] == 0 {
		ids = ids[1:]
	}
	maxRows := len(ids)

	// keep max lookup id
	var maxNonZeroId uint64
	if maxRows > 0 {
		maxNonZeroId = ids[maxRows-1]
	} else {
		return nil
	}

	defer func() {
		atomic.AddInt64(&t.stats.StreamedTuples, int64(q.stats.RowsMatched))
		q.Close()
	}()

	// mark all deleted entries (overwrite with 0 == illegal value)
	if t.tombstone.Len() > 0 {
		for i, v := range ids {
			if v == 0 || t.tombstone.PkIndex(v, 0) >= 0 {
				ids[i] = 0
			} else {
				maxNonZeroId = v
			}
		}
	}

	res := Result{
		fields: t.Fields(),
		pkg:    t.journal,
	}

	// lookup journal first (Note: its sorted by pk)
	pkidx := t.journal.pkindex
	col, _ := t.journal.Column(pkidx)
	pk, _ := col.([]uint64)

	if t.journal.Len() > 0 {
		var last int
		for i, v := range ids {
			// assuming all queried values are valid (i.e. > 0)
			// no more matches in journal?
			if pk[len(pk)-1] < v || pk[last] > maxNonZeroId {
				break
			}

			// not in journal
			j := t.journal.PkIndex(v, last)
			if j < 0 {
				continue
			}

			// forward match
			if err := fn(Row{res: &res, n: j}); err != nil {
				return err
			}
			q.stats.RowsMatched++

			// mark id as processed (set 0)
			ids[i] = 0
			last = j
		}
		q.stats.JournalTime = time.Since(q.lap)
	}

	// everything found in journal?, return early
	if maxRows == q.stats.RowsMatched {
		return nil
	}

	// remove zero values from id list
	if q.stats.RowsMatched > 0 || t.tombstone.Len() > 0 {
		clean := make([]uint64, 0, len(ids))
		for _, v := range ids {
			if v != 0 {
				clean = append(clean, v)
				maxNonZeroId = v
			}
		}
		ids = clean
	}

	if len(ids) == 0 {
		return nil
	}

	// PACK SCAN, schedule uses fast range checks and schould be perfect
	var nextid int
	q.lap = time.Now()
	for _, nextpack := range q.MakePackLookupSchedule(ids, false) {
		// stop when all inputs are matched
		if maxRows == q.stats.RowsMatched {
			break
		}

		if util.InterruptRequested(ctx) {
			return ctx.Err()
		}

		pkg, err := t.loadPack(tx, t.packidx.packs[nextpack].Key, true, q.reqfields)
		if err != nil {
			return err
		}
		res.pkg = pkg
		q.stats.PacksScanned++
		col, _ := pkg.Column(pkidx)
		pk, _ := col.([]uint64)

		// packs are sorted by pk
		last := 0

		// we use pack max value to break early
		_, max := t.packidx.MinMax(nextpack)

		// loop over the remaining (unresolved) list of pks
		for _, v := range ids[nextid:] {
			// no more matches in this pack?
			if max < v || pk[last] > maxNonZeroId {
				break
			}

			// not in pack
			j := pkg.PkIndex(v, last)
			if j < 0 {
				continue
			}

			// forward match
			if err := fn(Row{res: &res, n: j}); err != nil {
				return err
			}

			nextid++
			q.stats.RowsMatched++
			last = j
		}
	}
	q.stats.ScanTime = time.Since(q.lap)
	return nil
}

// merges non-full packs to minimize total pack count, also re-establishes a
// sequential/gapless pack key order when packs have been deleted
func (t *Table) Compact(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	// check if compaction is possible
	if t.packidx.Len() <= 1 {
		return nil
	}

	// check if compaction is required, either because packs are non-sequential
	// or not full (except the last)
	var (
		maxsz                 int = 1 << uint(t.opts.PackSizeLog2)
		srcSize               int64
		nextpack              uint32
		needCompact           bool
		total, moved, written int64
	)
	for i, v := range t.packidx.packs {
		needCompact = needCompact || v.Key > nextpack                             // sequence gap
		needCompact = needCompact || (i < t.packidx.Len()-1 && v.NValues < maxsz) // non-full pack (except the last)
		nextpack++
		total += int64(v.NValues)
		srcSize += int64(v.Size)
	}
	if !needCompact {
		log.Infof("pack: %s table %d packs / %d rows already compact", t.name, t.packidx.Len(), total)
		return nil
	}

	// check if compaction precondition is satisfied
	// - no out-of-order min/max ranges across sorted pack keys exist

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var (
		dstPack, srcPack *Package
		dstSize          int64
		dstIndex         int
		lastMaxPk        uint64
		isNewPack        bool
	)

	log.Infof("pack: compacting %s table %d packs / %d rows", t.name, t.packidx.Len(), total)

	// This algorithm walks the table's pack list in pack key order and
	// collects/compacts contents in row id (pk) order. Note that pk order may
	// differ from pack order if out-of-order inserts ever happened. In such case
	// this algorithm may abort or skip such packs to preserve the invariant
	// of non-overlapping pk ranges between packs.
	//
	// Gaps in pack key sequence are filled with new packs created on the fly.
	// When source packs are emptied during the process, they are immediatly removed
	// from KV storage and header list, but may be re-added subsequently.
	//
	for {
		// load next dst pack
		if dstPack == nil {
			dstKey := uint32(dstIndex)

			// handle existing pack keys
			if dstKey == t.packidx.packs[dstIndex].Key {
				// skip full packs
				if t.packidx.packs[dstIndex].NValues == maxsz {
					log.Debugf("pack: skipping full dst pack %x", dstKey)
					dstIndex++
					continue
				}
				// skip out of order packs
				pmin, pmax := t.packidx.MinMax(dstIndex)
				if pmin < lastMaxPk {
					log.Debugf("pack: skipping out-of-order dst pack %x", dstKey)
					dstIndex++
					continue
				}

				log.Debugf("pack: loading dst pack %d:%x", dstIndex, dstKey)
				dstPack, err = t.loadPack(tx, dstKey, false, nil)
				if err != nil {
					return err
				}
				lastMaxPk = pmax
				isNewPack = false
			} else {
				// handle gaps in key sequence
				// clone new pack from journal
				log.Debugf("pack: creating new dst pack %d:%x", dstIndex, dstKey)
				dstPack = t.packPool.Get().(*Package)
				dstPack.key = dstKey
				isNewPack = true
			}
		}

		// search for the next src pack that
		// - has a larger key than the current destination pack AND
		// - has the smallest min pk higher than the current destination's max pk
		if srcPack == nil {
			minSlice, _ := t.packidx.MinMaxSlices()
			var startIndex, srcIndex int = dstIndex, -1
			var lastmin uint64 = math.MaxUint64
			if isNewPack {
				startIndex--
			}
			for i := startIndex; i < len(minSlice); i++ {
				currmin := minSlice[i]
				if currmin <= lastMaxPk {
					continue
				}
				if lastmin > currmin {
					lastmin = currmin
					srcIndex = i
				}
			}

			// stop when no more source pack was found
			if srcIndex < 0 {
				break
			}

			ph := t.packidx.packs[srcIndex]
			log.Debugf("pack: loading src pack %d:%x", srcIndex, ph.Key)
			srcPack, err = t.loadPack(tx, ph.Key, false, nil)
			if err != nil {
				return err
			}
		}

		// Guarantees at this point:
		// - dstPack has free space
		// - srcPack is not empty

		// determine free space in destination
		free := maxsz - dstPack.Len()
		cp := util.Min(free, srcPack.Len())
		moved += int64(cp)

		// move data from src to dst
		log.Debugf("pack: moving %d/%d rows from pack %x to %x", cp, srcPack.Len(),
			srcPack.key, dstPack.key)
		if err := dstPack.AppendFrom(srcPack, 0, cp, true); err != nil {
			return err
		}
		if err := srcPack.Delete(0, cp); err != nil {
			return err
		}
		total += int64(cp)
		lastMaxPk, err = dstPack.Uint64At(dstPack.pkindex, dstPack.Len()-1)
		if err != nil {
			return err
		}

		// write dst when full
		if dstPack.Len() == maxsz {
			// this may extend the pack header list when dstPack is new
			log.Debugf("pack: storing full dst pack %x", dstPack.key)
			n, err := t.storePack(tx, dstPack)
			if err != nil {
				return err
			}
			dstSize += int64(n)
			dstIndex++
			written += int64(maxsz)

			// will load or create another output pack in next iteration
			dstPack = nil
		}

		if srcPack.Len() == 0 {
			log.Debugf("pack: deleting empty src pack %x", srcPack.key)
		}

		// store or delete source pack
		if _, err := t.storePack(tx, srcPack); err != nil {
			return err
		}

		// load new src in next iteration (or stop there)
		srcPack = nil

		// commit tx after each N written packs
		if tx.Pending() >= txMaxSize {
			if err := t.storePackInfo(tx.tx); err != nil {
				return err
			}
			if err := tx.CommitAndContinue(); err != nil {
				return err
			}
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}
		}
	}

	// store the last dstPack
	if dstPack != nil {
		log.Debugf("pack: storing last dst pack %x", dstPack.key)
		n, err := t.storePack(tx, dstPack)
		if err != nil {
			return err
		}
		dstSize += int64(n)
		written += int64(dstPack.Len())
	}

	log.Infof("pack: compacted %d/%d rows from %s table into %d packs (%s ->> %s)",
		moved, written, t.name, t.packidx.Len(), util.ByteSize(srcSize), util.ByteSize(dstSize))

	// store pack headers
	if err := t.storePackInfo(tx.tx); err != nil {
		return err
	}

	return tx.Commit()
}

func bytekey(key uint32) []byte {
	var buf [4]byte
	bigEndian.PutUint32(buf[:], key)
	return buf[:]
}

func (t Table) cachekey(key []byte) string {
	return t.name + "/" + hex.EncodeToString(key)
}

func (t *Table) loadPack(tx *Tx, id uint32, touch bool, fields FieldList) (*Package, error) {
	// determine of we need to load a full pack or a stripped version with less fields
	stripped := len(fields) > 0 && len(fields) < len(t.Fields())
	key := bytekey(id)

	// try cache lookup for the full pack first
	cachefn := t.cache.Peek
	if touch {
		cachefn = t.cache.Get
	}
	cachekey := t.cachekey(key)
	if cached, ok := cachefn(cachekey); ok {
		atomic.AddInt64(&t.stats.PackCacheHits, 1)
		return cached.(*Package), nil
	}
	if stripped {
		// try cache lookup for stripped packs
		//
		// FIXME: this caching scheme results in duplicate pack blocks
		//        being cached under different keys! instead we should
		//        cache individual data blocks rather than entire packs!
		cachekey += "#" + fields.Key()
		if cached, ok := cachefn(cachekey); ok {
			atomic.AddInt64(&t.stats.PackCacheHits, 1)
			return cached.(*Package), nil
		}
	}

	// if not found, load from storage using a pre-allocated pack as buffer
	atomic.AddInt64(&t.stats.PackCacheMisses, 1)
	var (
		err error
	)
	// fetch full pack from pool or create new full pack
	pkg := t.packPool.Get().(*Package)
	// skip undesired fields while loading
	if stripped {
		pkg = pkg.KeepFields(fields)
	}
	pkg, err = tx.loadPack(t.key, key, pkg)
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&t.stats.PacksLoaded, 1)
	atomic.AddInt64(&t.stats.PackBytesRead, int64(pkg.size))

	// FIXME: add dynamic data
	// pkg.key = id
	// pkg.tinfo = t.journal.tinfo
	// pkg.InitMetadata(t.fields)
	// ?? does this work?
	// ?? is this even necessary given the t.packPool.Get() above?
	pkg.InitFields(t.fields, t.journal.tinfo)

	pkg.cached = touch
	// store in cache
	if touch {
		updated, _ := t.cache.Add(cachekey, pkg)
		if updated {
			atomic.AddInt64(&t.stats.PackCacheUpdates, 1)
		} else {
			atomic.AddInt64(&t.stats.PackCacheInserts, 1)
		}
	}
	return pkg, nil
}

func (t *Table) storePack(tx *Tx, pkg *Package) (int, error) {
	key := pkg.Key()
	cachekey := t.cachekey(key)
	if pkg.Len() > 0 {
		// build header statistics
		info := pkg.Info()
		err := info.UpdateStats(pkg)
		if err != nil {
			return 0, err
		}

		// write to disk
		n, err := tx.storePack(t.key, key, pkg, t.opts.FillLevel)
		if err != nil {
			return 0, err
		}

		// update header statistics
		info.Size = n
		t.packidx.AddOrUpdate(info)

		// handle caching below to avoid the pack beeing free'd early
		atomic.AddInt64(&t.stats.PacksStored, 1)
		atomic.AddInt64(&t.stats.PackBytesWritten, int64(n))
		if pkg.cached {
			inserted, _ := t.cache.ContainsOrAdd(cachekey, pkg)
			if inserted {
				atomic.AddInt64(&t.stats.PackCacheInserts, 1)
			} else {
				atomic.AddInt64(&t.stats.PackCacheUpdates, 1)
			}
		}
		// remove all stripped packs from cache
		prefix := cachekey + "#"
		for _, v := range t.cache.Keys() {
			if strings.HasPrefix(v.(string), prefix) {
				t.cache.Remove(v)
			}
		}
		return n, nil
	}

	// If pack is empty

	// drop from index first because cache removal below recycle and clear the pack
	t.packidx.Remove(pkg.key)

	// remove from storage
	if err := tx.deletePack(t.key, key); err != nil {
		return 0, err
	}

	// remove from cache, returns back to pool
	t.cache.Remove(cachekey)

	// also remove all stripped packs from cache
	prefix := cachekey + "#"
	for _, v := range t.cache.Keys() {
		if strings.HasPrefix(v.(string), prefix) {
			t.cache.Remove(v)
		}
	}
	return 0, nil
}

// Note: pack must have been storted before splitting
func (t *Table) splitPack(tx *Tx, pkg *Package) (int, error) {
	// move half of the packs contents to a new pack (don't cache the new pack
	// to avoid possible eviction of the pack we are currently splitting!)
	newpkg := t.packPool.Get().(*Package)
	newpkg.cached = false
	half := pkg.Len() / 2
	if err := newpkg.AppendFrom(pkg, half, pkg.Len()-half, true); err != nil {
		return 0, err
	}
	if err := pkg.Delete(half, pkg.Len()-half); err != nil {
		return 0, err
	}

	// store both packs to update stats, this also stores the initial pack
	// on first split which may have not been stored yet
	_, err := t.storePack(tx, pkg)
	if err != nil {
		return 0, err
	}

	// save the new pack
	newpkg.key = t.packidx.NextKey()
	n, err := t.storePack(tx, newpkg)
	if err != nil {
		return 0, err
	}
	t.recyclePackage(newpkg)
	return n, nil
}

func (t *Table) makePackage() interface{} {
	atomic.AddInt64(&t.stats.PacksAlloc, 1)
	pkg, _ := t.journal.Clone(false, 1<<uint(t.opts.PackSizeLog2))
	return pkg
}

func (t *Table) onEvictedPackage(key, val interface{}) {
	pkg := val.(*Package)
	pkg.cached = false
	atomic.AddInt64(&t.stats.PackCacheEvictions, 1)
	t.recyclePackage(pkg)
}

func (t *Table) recyclePackage(pkg *Package) {
	if pkg == nil || pkg.cached {
		return
	}
	// don't recycle stripped packs
	if pkg.stripped {
		pkg.Release()
		return
	}
	// don't recycle oversized packs
	if c := pkg.Cap(); c <= 0 || c > 1<<uint(t.opts.PackSizeLog2) {
		pkg.Release()
		return
	}
	pkg.Clear()
	atomic.AddInt64(&t.stats.PacksRecycled, 1)
	t.packPool.Put(pkg)
}

func (t *Table) Size() TableSizeStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	var sz TableSizeStats
	for _, idx := range t.indexes {
		sz.IndexSize += idx.Size().TotalSize
	}
	for _, v := range t.cache.Keys() {
		val, ok := t.cache.Peek(v)
		if !ok {
			continue
		}
		pkg := val.(*Package)
		sz.CacheSize += pkg.HeapSize()
	}
	sz.JournalSize = t.journal.HeapSize()
	sz.TombstoneSize = t.tombstone.HeapSize()
	sz.TotalSize = sz.JournalSize + sz.TombstoneSize + sz.IndexSize + sz.CacheSize
	return sz
}
