// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package avx2

import (
	"blockwatch.cc/knoxdb/internal/bitset/generic"
)

var (
	And      = generic.And
	AndFlag  = generic.AndFlag
	AndNot   = generic.AndNot
	Or       = generic.Or
	OrFlag   = generic.OrFlag
	Xor      = generic.Xor
	Neg      = generic.Neg
	Reverse  = generic.Reverse
	PopCount = generic.PopCount
	Run      = generic.Run
	Indexes  = generic.Indexes
)
