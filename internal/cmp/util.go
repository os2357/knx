// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

// The compiler optimizes a few patterns including this form.
// See issue 6011. https://tip.golang.org/src/cmd/compile/internal/ssa/phiopt.go
func Bool2byte(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
