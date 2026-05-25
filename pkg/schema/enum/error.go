// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package enum

import (
	"errors"
)

var (
	ErrEnumDuplicate = errors.New("duplicate enum value")
	ErrEnumTooLong   = errors.New("enum value too long")
	ErrEnumFull      = errors.New("enum capacity exhausted")
	ErrEnumNoCode    = errors.New("enum code out of bounds")
)
