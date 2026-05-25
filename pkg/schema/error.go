// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"errors"
)

var (
	ErrDuplicateName  = errors.New("schema: duplicate name")
	ErrInvalidField   = errors.New("schema: invalid field")
	ErrInvalidValue   = errors.New("schema: invalid value type")
	ErrInvalidResult  = errors.New("schema: invalid result type")
	ErrShortBuffer    = errors.New("schema: short buffer")
	ErrSchemaMismatch = errors.New("schema: mismatch")
	ErrDeletePrimary  = errors.New("schema: cannot delete primary key field")
	ErrRenameEnum     = errors.New("schema: cannot rename enum field")
	ErrNoMeta         = errors.New("schema: missing metadata fields")
)
