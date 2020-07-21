// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

// const_0x55 is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x55<>+0x00(SB)/1, $(0x55)
GLOBL const_0x55<>(SB), (RODATA+NOPTR), $1

// const_0x33 is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x33<>+0x00(SB)/1, $(0x33)
GLOBL const_0x33<>(SB), (RODATA+NOPTR), $1

// const_0x0f is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x0f<>+0x00(SB)/1, $(0x0f)
GLOBL const_0x0f<>(SB), (RODATA+NOPTR), $1

// crosslane is the VPERMD input required to move data between AVX2 lanes
// used by all compare algorithms
DATA crosslane<>+0x00(SB)/1, $(6)
DATA crosslane<>+0x01(SB)/1, $(0)
DATA crosslane<>+0x02(SB)/1, $(0)
DATA crosslane<>+0x03(SB)/1, $(0)
DATA crosslane<>+0x04(SB)/1, $(7)
DATA crosslane<>+0x05(SB)/1, $(0)
DATA crosslane<>+0x06(SB)/1, $(0)
DATA crosslane<>+0x07(SB)/1, $(0)
DATA crosslane<>+0x08(SB)/1, $(2)
DATA crosslane<>+0x09(SB)/1, $(0)
DATA crosslane<>+0x0a(SB)/1, $(0)
DATA crosslane<>+0x0b(SB)/1, $(0)
DATA crosslane<>+0x0c(SB)/1, $(3)
DATA crosslane<>+0x0d(SB)/1, $(0)
DATA crosslane<>+0x0e(SB)/1, $(0)
DATA crosslane<>+0x0f(SB)/1, $(0)
DATA crosslane<>+0x10(SB)/1, $(4)
DATA crosslane<>+0x11(SB)/1, $(0)
DATA crosslane<>+0x12(SB)/1, $(0)
DATA crosslane<>+0x13(SB)/1, $(0)
DATA crosslane<>+0x14(SB)/1, $(5)
DATA crosslane<>+0x15(SB)/1, $(0)
DATA crosslane<>+0x16(SB)/1, $(0)
DATA crosslane<>+0x17(SB)/1, $(0)
DATA crosslane<>+0x18(SB)/1, $(0)
DATA crosslane<>+0x19(SB)/1, $(0)
DATA crosslane<>+0x1a(SB)/1, $(0)
DATA crosslane<>+0x1b(SB)/1, $(0)
DATA crosslane<>+0x1c(SB)/1, $(1)
DATA crosslane<>+0x1d(SB)/1, $(0)
DATA crosslane<>+0x1e(SB)/1, $(0)
DATA crosslane<>+0x1f(SB)/1, $(0)
GLOBL crosslane<>(SB), (RODATA+NOPTR), $32

// shuffle is the VPSHUFB input required to spread bytes in each word
// used by all compare algorithms
DATA shuffle<>+0x00(SB)/1, $(13)
DATA shuffle<>+0x01(SB)/1, $(12)
DATA shuffle<>+0x02(SB)/1, $(15)
DATA shuffle<>+0x03(SB)/1, $(14)
DATA shuffle<>+0x04(SB)/1, $(9)
DATA shuffle<>+0x05(SB)/1, $(8)
DATA shuffle<>+0x06(SB)/1, $(11)
DATA shuffle<>+0x07(SB)/1, $(10)
DATA shuffle<>+0x08(SB)/1, $(5)
DATA shuffle<>+0x09(SB)/1, $(4)
DATA shuffle<>+0x0a(SB)/1, $(7)
DATA shuffle<>+0x0b(SB)/1, $(6)
DATA shuffle<>+0x0c(SB)/1, $(1)
DATA shuffle<>+0x0d(SB)/1, $(0)
DATA shuffle<>+0x0e(SB)/1, $(3)
DATA shuffle<>+0x0f(SB)/1, $(2)
DATA shuffle<>+0x10(SB)/1, $(13)
DATA shuffle<>+0x11(SB)/1, $(12)
DATA shuffle<>+0x12(SB)/1, $(15)
DATA shuffle<>+0x13(SB)/1, $(14)
DATA shuffle<>+0x14(SB)/1, $(9)
DATA shuffle<>+0x15(SB)/1, $(8)
DATA shuffle<>+0x16(SB)/1, $(11)
DATA shuffle<>+0x17(SB)/1, $(10)
DATA shuffle<>+0x18(SB)/1, $(5)
DATA shuffle<>+0x19(SB)/1, $(4)
DATA shuffle<>+0x1a(SB)/1, $(7)
DATA shuffle<>+0x1b(SB)/1, $(6)
DATA shuffle<>+0x1c(SB)/1, $(1)
DATA shuffle<>+0x1d(SB)/1, $(0)
DATA shuffle<>+0x1e(SB)/1, $(3)
DATA shuffle<>+0x1f(SB)/1, $(2)
GLOBL shuffle<>(SB), (RODATA+NOPTR), $32
