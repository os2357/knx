// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"time"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"
)

type DatabaseOptions struct {
	Path       string     // local filesystem
	Namespace  string     // unique db identifier
	Driver     string     // bolt, badger, mem, ...
	PageSize   int        // boltdb
	PageFill   float64    // boltdb
	CacheSize  int        // in bytes
	NoSync     bool       // boltdb, no fsync on transactions (dangerous)
	NoGrowSync bool       // boltdb, skip fsync+alloc on grow
	ReadOnly   bool       // read-only tx and no schema changes
	Logger     log.Logger `knox:"-"`
}

func (o DatabaseOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		drvOpts := bolt.Options{
			// open timeout when file is locked
			Timeout: time.Second,

			// faster for large databases
			FreelistType: bolt.FreelistMapType,

			// User-controlled options
			//
			// skip fsync (DANGEROUS on crashes, but better performance for bulk load)
			NoSync: o.NoSync,
			//
			// skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
			NoGrowSync: o.NoGrowSync,
			//
			// don't fsync freelist (improves write performance at the cost of full
			// database scan on start-up)
			NoFreelistSync: o.NoSync,
			//
			// PageSize overrides the default OS page size.
			PageSize: o.PageSize,
			//
			// v1.4 (currently in alpha)
			// Logger: o.Logger,
		}
		o.Logger.Debug("Bolt DB config")
		o.Logger.Debugf("  Readonly         %t", o.ReadOnly)
		o.Logger.Debugf("  No-Sync          %t", o.NoSync)
		o.Logger.Debugf("  No-Grow-Sync     %t", o.NoGrowSync)
		o.Logger.Debugf("  Pagesize         %d", o.PageSize)
		if o.NoSync {
			o.Logger.Warnf("Enabled NOSYNC mode. Database will not be safe on crashes!")
		}
		return &drvOpts
	default:
		return nil
	}
}

func (o DatabaseOptions) Merge(o2 DatabaseOptions) DatabaseOptions {
	o.Path = util.NonZero(o2.Path, o.Path)
	o.Namespace = util.NonZero(o2.Namespace, o.Namespace)
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.CacheSize = util.NonZero(o2.CacheSize, o.CacheSize)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.NoGrowSync = o2.NoGrowSync
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
}

func (o DatabaseOptions) MarshalBinary() ([]byte, error) {
	enc := schema.NewGenericEncoder[DatabaseOptions]()
	return enc.Encode(o, nil)
}

func (o *DatabaseOptions) UnmarshalBinary(buf []byte) error {
	dec := schema.NewGenericDecoder[DatabaseOptions]()
	_, err := dec.Decode(buf, o)
	return err
}

type TableOptions struct {
	Engine       TableKind  // pack, lsm, parquet, csv, remote
	Driver       string     // bolt, badger, mem, ...
	PackSize     int        // pack engine
	JournalSize  int        // pack engine
	PageSize     int        // boltdb
	PageFill     float64    // boltdb
	ReadOnly     bool       // read-only tx and no schema changes
	TxMaxSize    int        // maximum write size of low-level dbfile transactions
	TrackChanges bool       // enable CDC
	NoSync       bool       // boltdb, no fsync on transactions (dangerous)
	NoGrowSync   bool       // boltdb, skip fsync+alloc on grow
	DB           store.DB   `knox:"-"` // shared low-level store implementation
	Logger       log.Logger `knox:"-"` // custom logger
}

func (o TableOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		drvOpts := bolt.Options{
			// open timeout when file is locked
			Timeout: time.Second,

			// faster for large databases
			FreelistType: bolt.FreelistMapType,

			// User-controlled options
			//
			// skip fsync (DANGEROUS on crashes, but better performance for bulk load)
			NoSync: o.NoSync,
			//
			// skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
			NoGrowSync: o.NoGrowSync,
			//
			// don't fsync freelist (improves write performance at the cost of full
			// database scan on start-up)
			NoFreelistSync: o.NoSync,
			//
			// PageSize overrides the default OS page size.
			PageSize: o.PageSize,
		}
		o.Logger.Debug("Bolt DB config")
		o.Logger.Debugf("  Readonly         %t", o.ReadOnly)
		o.Logger.Debugf("  No-Sync          %t", o.NoSync)
		o.Logger.Debugf("  No-Grow-Sync     %t", o.NoGrowSync)
		o.Logger.Debugf("  Pagesize         %d", o.PageSize)
		if o.NoSync {
			o.Logger.Warnf("Enabled NOSYNC mode. Database will not be safe on crashes!")
		}
		return &drvOpts
	default:
		return nil
	}
}

func (o TableOptions) Merge(o2 TableOptions) TableOptions {
	o.Engine = util.NonZero(o2.Engine, o.Engine)
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PackSize = util.NonZero(o2.PackSize, o.PackSize)
	o.JournalSize = util.NonZero(o2.JournalSize, o.JournalSize)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.TxMaxSize = util.NonZero(o2.TxMaxSize, o.TxMaxSize)
	o.ReadOnly = o2.ReadOnly
	o.TrackChanges = o2.TrackChanges
	o.NoSync = o2.NoSync
	o.NoGrowSync = o2.NoGrowSync
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
}

func (o TableOptions) MarshalBinary() ([]byte, error) {
	enc := schema.NewGenericEncoder[TableOptions]()
	return enc.Encode(o, nil)
}

func (o *TableOptions) UnmarshalBinary(buf []byte) error {
	dec := schema.NewGenericDecoder[TableOptions]()
	_, err := dec.Decode(buf, o)
	return err
}

type StoreOptions struct {
	Driver     string     // bolt, badger, mem, ...
	PageSize   int        // boltdb
	PageFill   float64    // boltdb
	ReadOnly   bool       // read-only tx only
	NoSync     bool       // boltdb, no fsync on transactions (dangerous)
	NoGrowSync bool       // boltdb, skip fsync+alloc on grow
	TxMaxSize  int        // maximum write size of low-level dbfile transactions
	DB         store.DB   `knox:"-"` // shared low-level store implementation
	Logger     log.Logger `knox:"-"` // custom logger
}

func (o StoreOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		drvOpts := bolt.Options{
			// open timeout when file is locked
			Timeout: time.Second,

			// faster for large databases
			FreelistType: bolt.FreelistMapType,

			// User-controlled options
			//
			// skip fsync (DANGEROUS on crashes, but better performance for bulk load)
			NoSync: o.NoSync,
			//
			// skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
			NoGrowSync: o.NoGrowSync,
			//
			// don't fsync freelist (improves write performance at the cost of full
			// database scan on start-up)
			NoFreelistSync: o.NoSync,
			//
			// PageSize overrides the default OS page size.
			PageSize: o.PageSize,
		}
		o.Logger.Debug("Bolt DB config")
		o.Logger.Debugf("  Readonly         %t", o.ReadOnly)
		o.Logger.Debugf("  No-Sync          %t", o.NoSync)
		o.Logger.Debugf("  No-Grow-Sync     %t", o.NoGrowSync)
		o.Logger.Debugf("  Pagesize         %d", o.PageSize)
		if o.NoSync {
			o.Logger.Warnf("Enabled NOSYNC mode. Database will not be safe on crashes!")
		}
		return &drvOpts
	default:
		return nil
	}
}

func (o StoreOptions) Merge(o2 StoreOptions) StoreOptions {
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.TxMaxSize = util.NonZero(o2.TxMaxSize, o.TxMaxSize)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.NoGrowSync = o2.NoGrowSync
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	if o2.DB != nil {
		o.DB = o2.DB
	}
	return o
}

func (o StoreOptions) MarshalBinary() ([]byte, error) {
	enc := schema.NewGenericEncoder[StoreOptions]()
	return enc.Encode(o, nil)
}

func (o *StoreOptions) UnmarshalBinary(buf []byte) error {
	dec := schema.NewGenericDecoder[StoreOptions]()
	_, err := dec.Decode(buf, o)
	return err
}

type IndexOptions struct {
	Engine      IndexKind       // pack, lsm
	Driver      string          // bolt, badger, mem, ...
	Type        types.IndexType // hash, int, composite, bloom, bfuse, bits
	PackSize    int             // pack engine
	JournalSize int             // pack engine
	PageSize    int             // boltdb
	PageFill    float64         // boltdb
	ReadOnly    bool            // read-only tx and no schema changes
	TxMaxSize   int             // maximum write size of low-level dbfile transactions
	NoSync      bool            // boltdb, no fsync on transactions (dangerous)
	NoGrowSync  bool            // boltdb, skip fsync+alloc on grow
	DB          store.DB        `knox:"-"` // shared low-level store implementation
	Logger      log.Logger      `knox:"-"` // custom logger
}

func (o IndexOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		drvOpts := bolt.Options{
			// open timeout when file is locked
			Timeout: time.Second,

			// faster for large databases
			FreelistType: bolt.FreelistMapType,

			// User-controlled options
			//
			// skip fsync (DANGEROUS on crashes, but better performance for bulk load)
			NoSync: o.NoSync,
			//
			// skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
			NoGrowSync: o.NoGrowSync,
			//
			// don't fsync freelist (improves write performance at the cost of full
			// database scan on start-up)
			NoFreelistSync: o.NoSync,
			//
			// PageSize overrides the default OS page size.
			PageSize: o.PageSize,
		}
		o.Logger.Debug("Bolt DB config")
		o.Logger.Debugf("  Readonly         %t", o.ReadOnly)
		o.Logger.Debugf("  No-Sync          %t", o.NoSync)
		o.Logger.Debugf("  No-Grow-Sync     %t", o.NoGrowSync)
		o.Logger.Debugf("  Pagesize         %d", o.PageSize)
		if o.NoSync {
			o.Logger.Warnf("Enabled NOSYNC mode. Database will not be safe on crashes!")
		}
		return &drvOpts
	default:
		return nil
	}
}

func (o IndexOptions) Merge(o2 IndexOptions) IndexOptions {
	o.Engine = util.NonZero(o2.Engine, o.Engine)
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.Type = util.NonZero(o2.Type, o.Type)
	o.PackSize = util.NonZero(o2.PackSize, o.PackSize)
	o.JournalSize = util.NonZero(o2.JournalSize, o.JournalSize)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.TxMaxSize = util.NonZero(o2.TxMaxSize, o.TxMaxSize)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.NoGrowSync = o2.NoGrowSync
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
}

func (o IndexOptions) MarshalBinary() ([]byte, error) {
	enc := schema.NewGenericEncoder[IndexOptions]()
	return enc.Encode(o, nil)
}

func (o *IndexOptions) UnmarshalBinary(buf []byte) error {
	dec := schema.NewGenericDecoder[IndexOptions]()
	_, err := dec.Decode(buf, o)
	return err
}
