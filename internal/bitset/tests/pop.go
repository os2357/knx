// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Test-usage only

package tests

type PopTest struct {
	Name      string
	Source    []byte
	SourceStr string
	Result    []byte
	ResultStr string
	Size      int
	Count     int
}

var PopCases = []PopTest{
	PopTest{
		Name:   "zeros_7",
		Source: []byte{0x0},
		Result: []byte{0x0},
		Size:   7,
		Count:  0,
	},
	PopTest{
		Name:   "ones_7",
		Source: []byte{0x7f},
		Result: []byte{0x7f},
		Size:   7,
		Count:  7,
	},
	PopTest{
		Name:   "fa_7",
		Source: []byte{0xfa},
		Result: []byte{0x7a},
		Size:   7,
		Count:  5,
	},
	PopTest{
		Name:   "f9_7",
		Source: []byte{0xf9},
		Result: []byte{0x79},
		Size:   7,
		Count:  5,
	},
}
