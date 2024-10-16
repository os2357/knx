// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package generic

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

// go:nocheckptr
// use nocheckptr because the underlying struct layout doesn't change
func DecodeUint64(dst []uint64, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selector[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector[sel].n
		i += 8
	}
	return j, nil
}

// go:nocheckptr
// use nocheckptr because the underlying struct layout doesn't change
func DecodeUint32(dst []uint32, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selector32[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector[sel].n
		i += 8
	}
	return j, nil
}

// go:nocheckptr
// use nocheckptr because the underlying struct layout doesn't change
func DecodeUint16(dst []uint16, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selector16[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector[sel].n
		i += 8
	}
	return j, nil
}

// go:nocheckptr
// use nocheckptr because the underlying struct layout doesn't change
func DecodeUint8(dst []uint8, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selector8[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector[sel].n
		i += 8
	}
	return j, nil
}

// go:nocheckptr
// use nocheckptr because the underlying struct layout doesn't change
// func DecodeBytesBigEndian(dst []uint64, src []byte) (value int, err error) {
// 	if len(src)&7 != 0 {
// 		return 0, errors.New("src length is not multiple of 8")
// 	}

// 	i := 0
// 	j := 0
// 	for i < len(src) {
// 		v := binary.BigEndian.Uint64(src[i:])
// 		sel := (v >> 60) & 0xf
// 		selector[sel].unpack(v, unsafe.Pointer(&dst[j]))
// 		j += selector[sel].n
// 		i += 8
// 	}
// 	return j, nil
// }

func CountValues(b []byte) (int, error) {
	var count int
	for len(b) >= 8 {
		v := binary.LittleEndian.Uint64(b[:8])
		b = b[8:]

		sel := v >> 60
		if sel >= 16 {
			return 0, fmt.Errorf("invalid selector value: %v", sel)
		}
		count += selector[sel].n
	}

	if len(b) > 0 {
		return 0, fmt.Errorf("invalid slice len remaining: %v", len(b))
	}
	return count, nil
}

func CountValuesBigEndian(b []byte) (int, error) {
	var count int
	for len(b) >= 8 {
		v := binary.BigEndian.Uint64(b[:8])
		b = b[8:]

		sel := v >> 60
		if sel >= 16 {
			return 0, fmt.Errorf("invalid selector value: %v", sel)
		}
		count += selector[sel].n
	}

	if len(b) > 0 {
		return 0, fmt.Errorf("invalid slice len remaining: %v", len(b))
	}
	return count, nil
}
