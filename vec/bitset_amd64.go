// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

import (
// "fmt"
)

var lengthTable = []uint8{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
}

var decodeTable = []uint32{
	0, 0, 0, 0, 0, 0, 0, 0, /* 0x00 (00000000) */
	1, 0, 0, 0, 0, 0, 0, 0, /* 0x01 (00000001) */
	2, 0, 0, 0, 0, 0, 0, 0, /* 0x02 (00000010) */
	1, 2, 0, 0, 0, 0, 0, 0, /* 0x03 (00000011) */
	3, 0, 0, 0, 0, 0, 0, 0, /* 0x04 (00000100) */
	1, 3, 0, 0, 0, 0, 0, 0, /* 0x05 (00000101) */
	2, 3, 0, 0, 0, 0, 0, 0, /* 0x06 (00000110) */
	1, 2, 3, 0, 0, 0, 0, 0, /* 0x07 (00000111) */
	4, 0, 0, 0, 0, 0, 0, 0, /* 0x08 (00001000) */
	1, 4, 0, 0, 0, 0, 0, 0, /* 0x09 (00001001) */
	2, 4, 0, 0, 0, 0, 0, 0, /* 0x0A (00001010) */
	1, 2, 4, 0, 0, 0, 0, 0, /* 0x0B (00001011) */
	3, 4, 0, 0, 0, 0, 0, 0, /* 0x0C (00001100) */
	1, 3, 4, 0, 0, 0, 0, 0, /* 0x0D (00001101) */
	2, 3, 4, 0, 0, 0, 0, 0, /* 0x0E (00001110) */
	1, 2, 3, 4, 0, 0, 0, 0, /* 0x0F (00001111) */
	5, 0, 0, 0, 0, 0, 0, 0, /* 0x10 (00010000) */
	1, 5, 0, 0, 0, 0, 0, 0, /* 0x11 (00010001) */
	2, 5, 0, 0, 0, 0, 0, 0, /* 0x12 (00010010) */
	1, 2, 5, 0, 0, 0, 0, 0, /* 0x13 (00010011) */
	3, 5, 0, 0, 0, 0, 0, 0, /* 0x14 (00010100) */
	1, 3, 5, 0, 0, 0, 0, 0, /* 0x15 (00010101) */
	2, 3, 5, 0, 0, 0, 0, 0, /* 0x16 (00010110) */
	1, 2, 3, 5, 0, 0, 0, 0, /* 0x17 (00010111) */
	4, 5, 0, 0, 0, 0, 0, 0, /* 0x18 (00011000) */
	1, 4, 5, 0, 0, 0, 0, 0, /* 0x19 (00011001) */
	2, 4, 5, 0, 0, 0, 0, 0, /* 0x1A (00011010) */
	1, 2, 4, 5, 0, 0, 0, 0, /* 0x1B (00011011) */
	3, 4, 5, 0, 0, 0, 0, 0, /* 0x1C (00011100) */
	1, 3, 4, 5, 0, 0, 0, 0, /* 0x1D (00011101) */
	2, 3, 4, 5, 0, 0, 0, 0, /* 0x1E (00011110) */
	1, 2, 3, 4, 5, 0, 0, 0, /* 0x1F (00011111) */
	6, 0, 0, 0, 0, 0, 0, 0, /* 0x20 (00100000) */
	1, 6, 0, 0, 0, 0, 0, 0, /* 0x21 (00100001) */
	2, 6, 0, 0, 0, 0, 0, 0, /* 0x22 (00100010) */
	1, 2, 6, 0, 0, 0, 0, 0, /* 0x23 (00100011) */
	3, 6, 0, 0, 0, 0, 0, 0, /* 0x24 (00100100) */
	1, 3, 6, 0, 0, 0, 0, 0, /* 0x25 (00100101) */
	2, 3, 6, 0, 0, 0, 0, 0, /* 0x26 (00100110) */
	1, 2, 3, 6, 0, 0, 0, 0, /* 0x27 (00100111) */
	4, 6, 0, 0, 0, 0, 0, 0, /* 0x28 (00101000) */
	1, 4, 6, 0, 0, 0, 0, 0, /* 0x29 (00101001) */
	2, 4, 6, 0, 0, 0, 0, 0, /* 0x2A (00101010) */
	1, 2, 4, 6, 0, 0, 0, 0, /* 0x2B (00101011) */
	3, 4, 6, 0, 0, 0, 0, 0, /* 0x2C (00101100) */
	1, 3, 4, 6, 0, 0, 0, 0, /* 0x2D (00101101) */
	2, 3, 4, 6, 0, 0, 0, 0, /* 0x2E (00101110) */
	1, 2, 3, 4, 6, 0, 0, 0, /* 0x2F (00101111) */
	5, 6, 0, 0, 0, 0, 0, 0, /* 0x30 (00110000) */
	1, 5, 6, 0, 0, 0, 0, 0, /* 0x31 (00110001) */
	2, 5, 6, 0, 0, 0, 0, 0, /* 0x32 (00110010) */
	1, 2, 5, 6, 0, 0, 0, 0, /* 0x33 (00110011) */
	3, 5, 6, 0, 0, 0, 0, 0, /* 0x34 (00110100) */
	1, 3, 5, 6, 0, 0, 0, 0, /* 0x35 (00110101) */
	2, 3, 5, 6, 0, 0, 0, 0, /* 0x36 (00110110) */
	1, 2, 3, 5, 6, 0, 0, 0, /* 0x37 (00110111) */
	4, 5, 6, 0, 0, 0, 0, 0, /* 0x38 (00111000) */
	1, 4, 5, 6, 0, 0, 0, 0, /* 0x39 (00111001) */
	2, 4, 5, 6, 0, 0, 0, 0, /* 0x3A (00111010) */
	1, 2, 4, 5, 6, 0, 0, 0, /* 0x3B (00111011) */
	3, 4, 5, 6, 0, 0, 0, 0, /* 0x3C (00111100) */
	1, 3, 4, 5, 6, 0, 0, 0, /* 0x3D (00111101) */
	2, 3, 4, 5, 6, 0, 0, 0, /* 0x3E (00111110) */
	1, 2, 3, 4, 5, 6, 0, 0, /* 0x3F (00111111) */
	7, 0, 0, 0, 0, 0, 0, 0, /* 0x40 (01000000) */
	1, 7, 0, 0, 0, 0, 0, 0, /* 0x41 (01000001) */
	2, 7, 0, 0, 0, 0, 0, 0, /* 0x42 (01000010) */
	1, 2, 7, 0, 0, 0, 0, 0, /* 0x43 (01000011) */
	3, 7, 0, 0, 0, 0, 0, 0, /* 0x44 (01000100) */
	1, 3, 7, 0, 0, 0, 0, 0, /* 0x45 (01000101) */
	2, 3, 7, 0, 0, 0, 0, 0, /* 0x46 (01000110) */
	1, 2, 3, 7, 0, 0, 0, 0, /* 0x47 (01000111) */
	4, 7, 0, 0, 0, 0, 0, 0, /* 0x48 (01001000) */
	1, 4, 7, 0, 0, 0, 0, 0, /* 0x49 (01001001) */
	2, 4, 7, 0, 0, 0, 0, 0, /* 0x4A (01001010) */
	1, 2, 4, 7, 0, 0, 0, 0, /* 0x4B (01001011) */
	3, 4, 7, 0, 0, 0, 0, 0, /* 0x4C (01001100) */
	1, 3, 4, 7, 0, 0, 0, 0, /* 0x4D (01001101) */
	2, 3, 4, 7, 0, 0, 0, 0, /* 0x4E (01001110) */
	1, 2, 3, 4, 7, 0, 0, 0, /* 0x4F (01001111) */
	5, 7, 0, 0, 0, 0, 0, 0, /* 0x50 (01010000) */
	1, 5, 7, 0, 0, 0, 0, 0, /* 0x51 (01010001) */
	2, 5, 7, 0, 0, 0, 0, 0, /* 0x52 (01010010) */
	1, 2, 5, 7, 0, 0, 0, 0, /* 0x53 (01010011) */
	3, 5, 7, 0, 0, 0, 0, 0, /* 0x54 (01010100) */
	1, 3, 5, 7, 0, 0, 0, 0, /* 0x55 (01010101) */
	2, 3, 5, 7, 0, 0, 0, 0, /* 0x56 (01010110) */
	1, 2, 3, 5, 7, 0, 0, 0, /* 0x57 (01010111) */
	4, 5, 7, 0, 0, 0, 0, 0, /* 0x58 (01011000) */
	1, 4, 5, 7, 0, 0, 0, 0, /* 0x59 (01011001) */
	2, 4, 5, 7, 0, 0, 0, 0, /* 0x5A (01011010) */
	1, 2, 4, 5, 7, 0, 0, 0, /* 0x5B (01011011) */
	3, 4, 5, 7, 0, 0, 0, 0, /* 0x5C (01011100) */
	1, 3, 4, 5, 7, 0, 0, 0, /* 0x5D (01011101) */
	2, 3, 4, 5, 7, 0, 0, 0, /* 0x5E (01011110) */
	1, 2, 3, 4, 5, 7, 0, 0, /* 0x5F (01011111) */
	6, 7, 0, 0, 0, 0, 0, 0, /* 0x60 (01100000) */
	1, 6, 7, 0, 0, 0, 0, 0, /* 0x61 (01100001) */
	2, 6, 7, 0, 0, 0, 0, 0, /* 0x62 (01100010) */
	1, 2, 6, 7, 0, 0, 0, 0, /* 0x63 (01100011) */
	3, 6, 7, 0, 0, 0, 0, 0, /* 0x64 (01100100) */
	1, 3, 6, 7, 0, 0, 0, 0, /* 0x65 (01100101) */
	2, 3, 6, 7, 0, 0, 0, 0, /* 0x66 (01100110) */
	1, 2, 3, 6, 7, 0, 0, 0, /* 0x67 (01100111) */
	4, 6, 7, 0, 0, 0, 0, 0, /* 0x68 (01101000) */
	1, 4, 6, 7, 0, 0, 0, 0, /* 0x69 (01101001) */
	2, 4, 6, 7, 0, 0, 0, 0, /* 0x6A (01101010) */
	1, 2, 4, 6, 7, 0, 0, 0, /* 0x6B (01101011) */
	3, 4, 6, 7, 0, 0, 0, 0, /* 0x6C (01101100) */
	1, 3, 4, 6, 7, 0, 0, 0, /* 0x6D (01101101) */
	2, 3, 4, 6, 7, 0, 0, 0, /* 0x6E (01101110) */
	1, 2, 3, 4, 6, 7, 0, 0, /* 0x6F (01101111) */
	5, 6, 7, 0, 0, 0, 0, 0, /* 0x70 (01110000) */
	1, 5, 6, 7, 0, 0, 0, 0, /* 0x71 (01110001) */
	2, 5, 6, 7, 0, 0, 0, 0, /* 0x72 (01110010) */
	1, 2, 5, 6, 7, 0, 0, 0, /* 0x73 (01110011) */
	3, 5, 6, 7, 0, 0, 0, 0, /* 0x74 (01110100) */
	1, 3, 5, 6, 7, 0, 0, 0, /* 0x75 (01110101) */
	2, 3, 5, 6, 7, 0, 0, 0, /* 0x76 (01110110) */
	1, 2, 3, 5, 6, 7, 0, 0, /* 0x77 (01110111) */
	4, 5, 6, 7, 0, 0, 0, 0, /* 0x78 (01111000) */
	1, 4, 5, 6, 7, 0, 0, 0, /* 0x79 (01111001) */
	2, 4, 5, 6, 7, 0, 0, 0, /* 0x7A (01111010) */
	1, 2, 4, 5, 6, 7, 0, 0, /* 0x7B (01111011) */
	3, 4, 5, 6, 7, 0, 0, 0, /* 0x7C (01111100) */
	1, 3, 4, 5, 6, 7, 0, 0, /* 0x7D (01111101) */
	2, 3, 4, 5, 6, 7, 0, 0, /* 0x7E (01111110) */
	1, 2, 3, 4, 5, 6, 7, 0, /* 0x7F (01111111) */
	8, 0, 0, 0, 0, 0, 0, 0, /* 0x80 (10000000) */
	1, 8, 0, 0, 0, 0, 0, 0, /* 0x81 (10000001) */
	2, 8, 0, 0, 0, 0, 0, 0, /* 0x82 (10000010) */
	1, 2, 8, 0, 0, 0, 0, 0, /* 0x83 (10000011) */
	3, 8, 0, 0, 0, 0, 0, 0, /* 0x84 (10000100) */
	1, 3, 8, 0, 0, 0, 0, 0, /* 0x85 (10000101) */
	2, 3, 8, 0, 0, 0, 0, 0, /* 0x86 (10000110) */
	1, 2, 3, 8, 0, 0, 0, 0, /* 0x87 (10000111) */
	4, 8, 0, 0, 0, 0, 0, 0, /* 0x88 (10001000) */
	1, 4, 8, 0, 0, 0, 0, 0, /* 0x89 (10001001) */
	2, 4, 8, 0, 0, 0, 0, 0, /* 0x8A (10001010) */
	1, 2, 4, 8, 0, 0, 0, 0, /* 0x8B (10001011) */
	3, 4, 8, 0, 0, 0, 0, 0, /* 0x8C (10001100) */
	1, 3, 4, 8, 0, 0, 0, 0, /* 0x8D (10001101) */
	2, 3, 4, 8, 0, 0, 0, 0, /* 0x8E (10001110) */
	1, 2, 3, 4, 8, 0, 0, 0, /* 0x8F (10001111) */
	5, 8, 0, 0, 0, 0, 0, 0, /* 0x90 (10010000) */
	1, 5, 8, 0, 0, 0, 0, 0, /* 0x91 (10010001) */
	2, 5, 8, 0, 0, 0, 0, 0, /* 0x92 (10010010) */
	1, 2, 5, 8, 0, 0, 0, 0, /* 0x93 (10010011) */
	3, 5, 8, 0, 0, 0, 0, 0, /* 0x94 (10010100) */
	1, 3, 5, 8, 0, 0, 0, 0, /* 0x95 (10010101) */
	2, 3, 5, 8, 0, 0, 0, 0, /* 0x96 (10010110) */
	1, 2, 3, 5, 8, 0, 0, 0, /* 0x97 (10010111) */
	4, 5, 8, 0, 0, 0, 0, 0, /* 0x98 (10011000) */
	1, 4, 5, 8, 0, 0, 0, 0, /* 0x99 (10011001) */
	2, 4, 5, 8, 0, 0, 0, 0, /* 0x9A (10011010) */
	1, 2, 4, 5, 8, 0, 0, 0, /* 0x9B (10011011) */
	3, 4, 5, 8, 0, 0, 0, 0, /* 0x9C (10011100) */
	1, 3, 4, 5, 8, 0, 0, 0, /* 0x9D (10011101) */
	2, 3, 4, 5, 8, 0, 0, 0, /* 0x9E (10011110) */
	1, 2, 3, 4, 5, 8, 0, 0, /* 0x9F (10011111) */
	6, 8, 0, 0, 0, 0, 0, 0, /* 0xA0 (10100000) */
	1, 6, 8, 0, 0, 0, 0, 0, /* 0xA1 (10100001) */
	2, 6, 8, 0, 0, 0, 0, 0, /* 0xA2 (10100010) */
	1, 2, 6, 8, 0, 0, 0, 0, /* 0xA3 (10100011) */
	3, 6, 8, 0, 0, 0, 0, 0, /* 0xA4 (10100100) */
	1, 3, 6, 8, 0, 0, 0, 0, /* 0xA5 (10100101) */
	2, 3, 6, 8, 0, 0, 0, 0, /* 0xA6 (10100110) */
	1, 2, 3, 6, 8, 0, 0, 0, /* 0xA7 (10100111) */
	4, 6, 8, 0, 0, 0, 0, 0, /* 0xA8 (10101000) */
	1, 4, 6, 8, 0, 0, 0, 0, /* 0xA9 (10101001) */
	2, 4, 6, 8, 0, 0, 0, 0, /* 0xAA (10101010) */
	1, 2, 4, 6, 8, 0, 0, 0, /* 0xAB (10101011) */
	3, 4, 6, 8, 0, 0, 0, 0, /* 0xAC (10101100) */
	1, 3, 4, 6, 8, 0, 0, 0, /* 0xAD (10101101) */
	2, 3, 4, 6, 8, 0, 0, 0, /* 0xAE (10101110) */
	1, 2, 3, 4, 6, 8, 0, 0, /* 0xAF (10101111) */
	5, 6, 8, 0, 0, 0, 0, 0, /* 0xB0 (10110000) */
	1, 5, 6, 8, 0, 0, 0, 0, /* 0xB1 (10110001) */
	2, 5, 6, 8, 0, 0, 0, 0, /* 0xB2 (10110010) */
	1, 2, 5, 6, 8, 0, 0, 0, /* 0xB3 (10110011) */
	3, 5, 6, 8, 0, 0, 0, 0, /* 0xB4 (10110100) */
	1, 3, 5, 6, 8, 0, 0, 0, /* 0xB5 (10110101) */
	2, 3, 5, 6, 8, 0, 0, 0, /* 0xB6 (10110110) */
	1, 2, 3, 5, 6, 8, 0, 0, /* 0xB7 (10110111) */
	4, 5, 6, 8, 0, 0, 0, 0, /* 0xB8 (10111000) */
	1, 4, 5, 6, 8, 0, 0, 0, /* 0xB9 (10111001) */
	2, 4, 5, 6, 8, 0, 0, 0, /* 0xBA (10111010) */
	1, 2, 4, 5, 6, 8, 0, 0, /* 0xBB (10111011) */
	3, 4, 5, 6, 8, 0, 0, 0, /* 0xBC (10111100) */
	1, 3, 4, 5, 6, 8, 0, 0, /* 0xBD (10111101) */
	2, 3, 4, 5, 6, 8, 0, 0, /* 0xBE (10111110) */
	1, 2, 3, 4, 5, 6, 8, 0, /* 0xBF (10111111) */
	7, 8, 0, 0, 0, 0, 0, 0, /* 0xC0 (11000000) */
	1, 7, 8, 0, 0, 0, 0, 0, /* 0xC1 (11000001) */
	2, 7, 8, 0, 0, 0, 0, 0, /* 0xC2 (11000010) */
	1, 2, 7, 8, 0, 0, 0, 0, /* 0xC3 (11000011) */
	3, 7, 8, 0, 0, 0, 0, 0, /* 0xC4 (11000100) */
	1, 3, 7, 8, 0, 0, 0, 0, /* 0xC5 (11000101) */
	2, 3, 7, 8, 0, 0, 0, 0, /* 0xC6 (11000110) */
	1, 2, 3, 7, 8, 0, 0, 0, /* 0xC7 (11000111) */
	4, 7, 8, 0, 0, 0, 0, 0, /* 0xC8 (11001000) */
	1, 4, 7, 8, 0, 0, 0, 0, /* 0xC9 (11001001) */
	2, 4, 7, 8, 0, 0, 0, 0, /* 0xCA (11001010) */
	1, 2, 4, 7, 8, 0, 0, 0, /* 0xCB (11001011) */
	3, 4, 7, 8, 0, 0, 0, 0, /* 0xCC (11001100) */
	1, 3, 4, 7, 8, 0, 0, 0, /* 0xCD (11001101) */
	2, 3, 4, 7, 8, 0, 0, 0, /* 0xCE (11001110) */
	1, 2, 3, 4, 7, 8, 0, 0, /* 0xCF (11001111) */
	5, 7, 8, 0, 0, 0, 0, 0, /* 0xD0 (11010000) */
	1, 5, 7, 8, 0, 0, 0, 0, /* 0xD1 (11010001) */
	2, 5, 7, 8, 0, 0, 0, 0, /* 0xD2 (11010010) */
	1, 2, 5, 7, 8, 0, 0, 0, /* 0xD3 (11010011) */
	3, 5, 7, 8, 0, 0, 0, 0, /* 0xD4 (11010100) */
	1, 3, 5, 7, 8, 0, 0, 0, /* 0xD5 (11010101) */
	2, 3, 5, 7, 8, 0, 0, 0, /* 0xD6 (11010110) */
	1, 2, 3, 5, 7, 8, 0, 0, /* 0xD7 (11010111) */
	4, 5, 7, 8, 0, 0, 0, 0, /* 0xD8 (11011000) */
	1, 4, 5, 7, 8, 0, 0, 0, /* 0xD9 (11011001) */
	2, 4, 5, 7, 8, 0, 0, 0, /* 0xDA (11011010) */
	1, 2, 4, 5, 7, 8, 0, 0, /* 0xDB (11011011) */
	3, 4, 5, 7, 8, 0, 0, 0, /* 0xDC (11011100) */
	1, 3, 4, 5, 7, 8, 0, 0, /* 0xDD (11011101) */
	2, 3, 4, 5, 7, 8, 0, 0, /* 0xDE (11011110) */
	1, 2, 3, 4, 5, 7, 8, 0, /* 0xDF (11011111) */
	6, 7, 8, 0, 0, 0, 0, 0, /* 0xE0 (11100000) */
	1, 6, 7, 8, 0, 0, 0, 0, /* 0xE1 (11100001) */
	2, 6, 7, 8, 0, 0, 0, 0, /* 0xE2 (11100010) */
	1, 2, 6, 7, 8, 0, 0, 0, /* 0xE3 (11100011) */
	3, 6, 7, 8, 0, 0, 0, 0, /* 0xE4 (11100100) */
	1, 3, 6, 7, 8, 0, 0, 0, /* 0xE5 (11100101) */
	2, 3, 6, 7, 8, 0, 0, 0, /* 0xE6 (11100110) */
	1, 2, 3, 6, 7, 8, 0, 0, /* 0xE7 (11100111) */
	4, 6, 7, 8, 0, 0, 0, 0, /* 0xE8 (11101000) */
	1, 4, 6, 7, 8, 0, 0, 0, /* 0xE9 (11101001) */
	2, 4, 6, 7, 8, 0, 0, 0, /* 0xEA (11101010) */
	1, 2, 4, 6, 7, 8, 0, 0, /* 0xEB (11101011) */
	3, 4, 6, 7, 8, 0, 0, 0, /* 0xEC (11101100) */
	1, 3, 4, 6, 7, 8, 0, 0, /* 0xED (11101101) */
	2, 3, 4, 6, 7, 8, 0, 0, /* 0xEE (11101110) */
	1, 2, 3, 4, 6, 7, 8, 0, /* 0xEF (11101111) */
	5, 6, 7, 8, 0, 0, 0, 0, /* 0xF0 (11110000) */
	1, 5, 6, 7, 8, 0, 0, 0, /* 0xF1 (11110001) */
	2, 5, 6, 7, 8, 0, 0, 0, /* 0xF2 (11110010) */
	1, 2, 5, 6, 7, 8, 0, 0, /* 0xF3 (11110011) */
	3, 5, 6, 7, 8, 0, 0, 0, /* 0xF4 (11110100) */
	1, 3, 5, 6, 7, 8, 0, 0, /* 0xF5 (11110101) */
	2, 3, 5, 6, 7, 8, 0, 0, /* 0xF6 (11110110) */
	1, 2, 3, 5, 6, 7, 8, 0, /* 0xF7 (11110111) */
	4, 5, 6, 7, 8, 0, 0, 0, /* 0xF8 (11111000) */
	1, 4, 5, 6, 7, 8, 0, 0, /* 0xF9 (11111001) */
	2, 4, 5, 6, 7, 8, 0, 0, /* 0xFA (11111010) */
	1, 2, 4, 5, 6, 7, 8, 0, /* 0xFB (11111011) */
	3, 4, 5, 6, 7, 8, 0, 0, /* 0xFC (11111100) */
	1, 3, 4, 5, 6, 7, 8, 0, /* 0xFD (11111101) */
	2, 3, 4, 5, 6, 7, 8, 0, /* 0xFE (11111110) */
	1, 2, 3, 4, 5, 6, 7, 8, /* 0xFF (11111111) */
}

//go:noescape
func bitsetAndAVX2(dst, src []byte)

//go:noescape
func bitsetAndAVX2Flag1(dst, src []byte) int

//go:noescape
func bitsetAndNotAVX2(dst, src []byte)

//go:noescape
func bitsetOrAVX2(dst, src []byte)

//go:noescape
func bitsetOrAVX2Flag1(dst, src []byte) int

//go:noescape
func bitsetXorAVX2(dst, src []byte)

//go:noescape
func bitsetNegAVX2(src []byte)

//go:noescape
func bitsetReverseAVX2(src []byte, bitsetReverseLut256 []uint8)

//go:noescape
func bitsetPopCountAVX2(src []byte) int64

//go:noescape
func bitsetIndexesAVX2FullCore(bitmap []byte, out []uint32, decodeTable []uint32, lengthTable []uint8) int

//go:noescape
func bitsetIndexesAVX2SkipCore(bitmap []byte, out []uint32, decodeTable []uint32, lengthTable []uint8) int

//go:noescape
func bitsetNextOneBitAVX2(src []byte, index uint64) uint64

//go:noescape
func bitsetNextZeroBitAVX2(src []byte, index uint64) uint64

func bitsetAnd(dst, src []byte, size int) int {
	switch {
	case useAVX2:
		ret := bitsetAndAVX2Flag1(dst, src)
		dst[len(dst)-1] &= bytemask(size)
		return ret
	default:
		return bitsetAndGenericFlag1(dst, src, size)
	}
}

func bitsetAndNot(dst, src []byte, size int) {
	switch {
	case useAVX2:
		bitsetAndNotAVX2(dst, src)
		dst[len(dst)-1] &= bytemask(size)
	default:
		bitsetAndNotGeneric(dst, src, size)
	}
}

func bitsetOr(dst, src []byte, size int) int {
	switch {
	case useAVX2:
		ret := bitsetOrAVX2Flag1(dst, src)
		dst[len(dst)-1] &= bytemask(size)
		return ret
	default:
		return bitsetOrGenericFlag1(dst, src, size)
	}
}

func bitsetXor(dst, src []byte, size int) {
	switch {
	case useAVX2:
		bitsetXorAVX2(dst, src)
		dst[len(dst)-1] &= bytemask(size)
	default:
		bitsetXorGeneric(dst, src, size)
	}
}

func bitsetNeg(src []byte, size int) {
	switch {
	case useAVX2:
		bitsetNegAVX2(src)
		src[len(src)-1] &= bytemask(size)
	default:
		bitsetNegGeneric(src, size)
	}
}

func bitsetReverse(src []byte) {
	switch {
	case useAVX2:
		bitsetReverseAVX2(src, bitsetReverseLut256)
	default:
		bitsetReverseGeneric(src)
	}
}

func bitsetIndexesAVX2Full(src []byte, size int, dst []uint32) int {
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}
	return bitsetIndexesAVX2FullCore(src, dst, decodeTable, lengthTable)
}

func bitsetIndexesAVX2Skip(src []byte, size int, dst []uint32) int {
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}
	return bitsetIndexesAVX2SkipCore(src, dst, decodeTable, lengthTable)
}

func bitsetIndexes(src []byte, size int, dst []uint32) int {
	switch {
	case useAVX2:
		return bitsetIndexesAVX2Skip(src, size, dst)
	default:
		return bitsetIndexesGenericSkip64(src, size, dst)
	}
}

func bitsetPopCount(src []byte, size int) int64 {
	switch {
	case useAVX2:
		switch true {
		case size == 0:
			return 0
		case size <= 8:
			return int64(bitsetLookup[src[0]&bytemask(size)])
		case size&0x7 == 0:
			return bitsetPopCountAVX2(src)
		default:
			cnt := bitsetPopCountAVX2(src[:len(src)-1])
			return cnt + int64(bitsetLookup[src[len(src)-1]&bytemask(size)])
		}
	default:
		return bitsetPopCountGeneric(src, size)
	}

}

func bitsetRun(src []byte, index, size int) (int, int) {
	switch {
	case useAVX2:
		return bitsetRunAVX2Wrapper(src, index, size)
	default:
		return bitsetRunGeneric(src, index, size)
	}
}

func bitsetRunAVX2Wrapper(src []byte, index, size int) (int, int) {
	if len(src) == 0 || index < 0 || index >= size {
		return -1, 0
	}
	var (
		start  int = -1
		length int
	)
	i := index >> 3

	// mask leading bits of the first byte
	offset := index & 0x7
	mask := byte(0xff) << uint(offset)
	first := src[i] & mask
	if first > 0 {
		// start is in same byte as index
		start = index - offset + bitsetLeadingZeros[first]
		length = -bitsetLeadingZeros[first]
	} else {
		// find next 1 bit
		i++

		// Note: function call overhead makes this perform only for large strides
		i = int(bitsetNextOneBitAVX2(src, uint64(i)))

		// no more one's
		if i == len(src) {
			return -1, 0
		}
		start = i<<3 + bitsetLeadingZeros[src[i]]
		length = -bitsetLeadingZeros[src[i]]
	}

	// find next 0 bit beginning at 'start' position in the current byte:
	// we first negate the byte to reuse the bitsetLeadingZeros lookup table,
	// then mask out leading bits before and including the start position, and
	// finally lookup the number of unmasked leading zeros; if there is any bit
	// set to one (remember, that's a negated zero bit) the run ends in the same
	// byte where it started.
	if pos := bitsetLeadingZeros[(^src[i])&(byte(0xff)<<uint((start&0x7)+1))]; pos < 8 {
		length += pos
		return start, length
	}

	// now that the start byte is processed, we continue scan in the
	// remainder of the bitset
	i++
	length += 8

	// Note: function call overhead makes this perform only for large strides
	j := int(bitsetNextZeroBitAVX2(src, uint64(i)))
	length += 8 * (j - i)
	i = j

	// rewind when we've moved past the slice end
	if i == len(src) {
		i--
	}

	// count trailing one bits
	if src[i] != 0xff {
		length += bitsetLeadingZeros[^src[i]]
		// corner-case overlow check
		if start+length > size {
			length = size - start
		}
	}

	return start, length
}
