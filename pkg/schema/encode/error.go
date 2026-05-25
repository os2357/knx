// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"errors"
)

var (
	ErrNilValue         = errors.New("encode: nil value")
	ErrInvalidValue     = errors.New("encode: invalid value")
	ErrInvalidValueType = errors.New("encode: invalid value type")
	ErrInvalidField     = errors.New("encode: invalid field")
	ErrOverflow         = errors.New("encode: integer overflow")
	ErrShortValue       = errors.New("encode: value too short")
	ErrShortBuffer      = errors.New("encode: short buffer")
	ErrEnumUndefined    = errors.New("encode: missing enum dictionary")
)
