// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
)

func (e *Engine) Enums() schema.EnumRegistry {
	return e.enums
}

func (e *Engine) EnumNames() []string {
	names := make([]string, 0, len(e.enums))
	for _, v := range e.enums {
		names = append(names, v.Name())
	}
	return names
}

func (e *Engine) NumEnums() int {
	return len(e.enums)
}

func (e *Engine) UseEnum(name string) (*schema.EnumDictionary, error) {
	enum, ok := e.enums[types.TaggedHash(types.ObjectTagEnum, name)]
	if !ok {
		return nil, ErrNoEnum
	}
	return enum, nil
}

func (e *Engine) GetEnum(hash uint64) (*schema.EnumDictionary, bool) {
	enum, ok := e.enums[hash]
	return enum, ok
}

func (e *Engine) CreateEnum(ctx context.Context, name string) (*schema.EnumDictionary, error) {
	// check name is unique
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	if _, ok := e.enums[tag]; ok {
		return nil, ErrEnumExists
	}

	// open write transaction
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// create
	enum := schema.NewEnumDictionary(name)

	// register commit callback
	GetTransaction(ctx).OnCommit(func(ctx context.Context) error {
		e.enums[tag] = enum
		return nil
	})

	// store in catalog
	if err := e.cat.AddEnum(ctx, enum); err != nil {
		return nil, err
	}

	// write wal
	obj := &EnumObject{id: tag, name: name}
	if err := e.writeWalRecord(ctx, wal.RecordTypeInsert, obj); err != nil {
		return nil, err
	}

	// commit (note: noop when called with outside tx)
	if err := commit(); err != nil {
		return nil, err
	}

	return enum, nil
}

func (e *Engine) DropEnum(ctx context.Context, name string) error {
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	if _, ok := e.enums[tag]; !ok {
		return ErrNoEnum
	}

	// open transaction
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// register commit callback
	// GetTransaction(ctx).OnCommit(func(ctx context.Context) error {
	//  ???
	// 	return nil
	// })

	// write wal
	obj := &EnumObject{id: tag, name: name}
	if err := e.writeWalRecord(ctx, wal.RecordTypeDelete, obj); err != nil {
		return err
	}

	// remove enum from catalog
	if err := e.cat.DropEnum(ctx, tag); err != nil {
		return err
	}

	delete(e.enums, tag)

	return commit()
}

func (e *Engine) ExtendEnum(ctx context.Context, name string, vals ...string) error {
	tag := types.TaggedHash(types.ObjectTagEnum, name)
	enum, ok := e.enums[tag]
	if !ok {
		return ErrNoEnum
	}

	// open transaction
	ctx, commit, abort := e.WithTransaction(ctx)
	defer abort()

	// write wal
	obj := &EnumObject{id: tag, name: name, vals: vals}
	if err := e.writeWalRecord(ctx, wal.RecordTypeUpdate, obj); err != nil {
		return err
	}

	// extend enum
	if err := enum.Append(vals...); err != nil {
		return err
	}

	// store enum data
	if err := e.cat.PutEnum(ctx, enum); err != nil {
		return err
	}

	return commit()
}

func (e *Engine) openEnums(ctx context.Context) error {
	// iterate catalog
	keys, err := e.cat.ListEnums(ctx)
	if err != nil {
		return err
	}

	for _, key := range keys {
		enum, err := e.cat.GetEnum(ctx, key)
		if err != nil {
			return err
		}
		e.enums[key] = enum
	}

	return nil
}
