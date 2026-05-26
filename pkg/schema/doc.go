// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

// Package schema defines type system management for database tables with
// two main purposes
//
// - defining structure and configuration for database tables
// - encoding/decoding binary records and accessing data in binary records without decoding
//
// Schemas can either be created programmatically from types `Schema` and `Field` or
// by adding struct tag `knox` to a user-defined struct and then calling `SchemaOf`.
// The following struct tag features are available
//
// ```
// pk            mark this field as primary key (also generates primary key index)
// index={type}  generate index over this field (hash, int, composite)
// fields={a+b}  list of composite index fields
// extra={a+b}   list of extra include fields for index
// filter={type} use db column filter (bits, bloom, bfuse)
// zip={type}    use extra compression (snappy, lz4, zstd, none, (empty))
// array={num}   treat as fixed length array (string only)
// scale={num}   scale factor (for decimal and time types only)
// id={num}      override id value
// enum          mark as enum
// metadata      mark as metadata
// null          mark as nullable
// timebase      mark as event time source
// timestamp     type timestamp in nanoseconds
// date          type date in unix days
// time          type time in seconds
// ```
//
// A schema is a list of immutable fields with properties like name, data type,
// and type specific options (decimal scale, fixed length). Each field is unique and
// identified by an immutable id value. Type and id may not change, but schema
// evolution is possible in several ways:
//
// - the name of a field may be changed
// - a new field may be added
// - an existing fields can be marked as deleted
//
// Each change produces a new version of the schema which is identified by a
// unique hash value.
//
// Internally, field flags are used to represent properties such as
// - primary key: the field is used as primary key (must be uint64 type)
// - indexed: a database index will be created for this field
// - enum: the field is an enum type with a private EnumDictionary
// - deleted: the field is deleted and no longer used
// - metadata: the field is not used for encoding and decoding data
// - nullable: values can be null
// - timebase: timestamp field used as event time source for stream processing
// - action: CDC action metadata field
//
// Flags define how fields are used by record encoders and decoders:
// - `visible` means a field is used when encoding/decoding binary records from Go structs;
//    a visible field is never deleted or internal
// - `active` means the field (internal or not) is in active use, i.e. it is not deleted
// - `metadata` means all non deleted internal fields
//
// Scale factor
//
// Time scales
// - `ns` store time in nanosecond resolution
// - `us` stores time in microsecond resolution
// - `ms` stores time in millisecond resolution
// - `s` stores time in second resolution
//
// Decimal (fixed-point scales)
// - `Decimal32` values from `0..9`
// - `Decimal64` values from `0..18`
// - `Decimal128` values from `0..38`
// - `Decimal256` values from `0..76`
