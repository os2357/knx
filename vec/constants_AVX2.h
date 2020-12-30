// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// const_0x55 is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x55<>+0x00(SB)/1, $(0x55)
GLOBL const_0x55<>(SB), (RODATA+NOPTR), $1

// const_0x33 is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x33<>+0x00(SB)/1, $(0x33)
GLOBL const_0x33<>(SB), (RODATA+NOPTR), $1

// const_0x0f is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x0f<>+0x00(SB)/1, $(0x0f)
GLOBL const_0x0f<>(SB), (RODATA+NOPTR), $1

DATA const_0x80<>+0x00(SB)/1, $(0x80)
GLOBL const_0x80<>(SB), (RODATA+NOPTR), $1

DATA const_0x01<>+0x00(SB)/1, $(0x01)
GLOBL const_0x01<>(SB), (RODATA+NOPTR), $1

// crosslane is the VPERMD input required to move data between AVX2 lanes
// used by all compare algorithms
DATA crosslane<>+0x00(SB)/4, $(6)
DATA crosslane<>+0x04(SB)/4, $(7)
DATA crosslane<>+0x08(SB)/4, $(2)
DATA crosslane<>+0x0c(SB)/4, $(3)
DATA crosslane<>+0x10(SB)/4, $(4)
DATA crosslane<>+0x14(SB)/4, $(5)
DATA crosslane<>+0x18(SB)/4, $(0)
DATA crosslane<>+0x1c(SB)/4, $(1)
GLOBL crosslane<>(SB), (RODATA+NOPTR), $32

// VPSHUFB input required to spread bytes in each word
// used by all 16 bit compare algorithms
DATA shuffle16<>+0x00(SB)/1, $(15)
DATA shuffle16<>+0x01(SB)/1, $(14)
DATA shuffle16<>+0x02(SB)/1, $(13)
DATA shuffle16<>+0x03(SB)/1, $(12)
DATA shuffle16<>+0x04(SB)/1, $(11)
DATA shuffle16<>+0x05(SB)/1, $(10)
DATA shuffle16<>+0x06(SB)/1, $(9)
DATA shuffle16<>+0x07(SB)/1, $(8)
DATA shuffle16<>+0x08(SB)/1, $(7)
DATA shuffle16<>+0x09(SB)/1, $(6)
DATA shuffle16<>+0x0a(SB)/1, $(5)
DATA shuffle16<>+0x0b(SB)/1, $(4)
DATA shuffle16<>+0x0c(SB)/1, $(3)
DATA shuffle16<>+0x0d(SB)/1, $(2)
DATA shuffle16<>+0x0e(SB)/1, $(1)
DATA shuffle16<>+0x0f(SB)/1, $(0)
DATA shuffle16<>+0x10(SB)/1, $(15)
DATA shuffle16<>+0x11(SB)/1, $(14)
DATA shuffle16<>+0x12(SB)/1, $(13)
DATA shuffle16<>+0x13(SB)/1, $(12)
DATA shuffle16<>+0x14(SB)/1, $(11)
DATA shuffle16<>+0x15(SB)/1, $(10)
DATA shuffle16<>+0x16(SB)/1, $(9)
DATA shuffle16<>+0x17(SB)/1, $(8)
DATA shuffle16<>+0x18(SB)/1, $(7)
DATA shuffle16<>+0x19(SB)/1, $(6)
DATA shuffle16<>+0x1a(SB)/1, $(5)
DATA shuffle16<>+0x1b(SB)/1, $(4)
DATA shuffle16<>+0x1c(SB)/1, $(3)
DATA shuffle16<>+0x1d(SB)/1, $(2)
DATA shuffle16<>+0x1e(SB)/1, $(1)
DATA shuffle16<>+0x1f(SB)/1, $(0)
GLOBL shuffle16<>(SB), (RODATA+NOPTR), $32

// VPSHUFB input required to spread bytes in each word
// used by all 8 bit compare algorithms
DATA shuffle8<>+0x00(SB)/1, $(7)
DATA shuffle8<>+0x01(SB)/1, $(6)
DATA shuffle8<>+0x02(SB)/1, $(5)
DATA shuffle8<>+0x03(SB)/1, $(4)
DATA shuffle8<>+0x04(SB)/1, $(3)
DATA shuffle8<>+0x05(SB)/1, $(2)
DATA shuffle8<>+0x06(SB)/1, $(1)
DATA shuffle8<>+0x07(SB)/1, $(0)
DATA shuffle8<>+0x08(SB)/1, $(15)
DATA shuffle8<>+0x09(SB)/1, $(14)
DATA shuffle8<>+0x0a(SB)/1, $(13)
DATA shuffle8<>+0x0b(SB)/1, $(12)
DATA shuffle8<>+0x0c(SB)/1, $(11)
DATA shuffle8<>+0x0d(SB)/1, $(10)
DATA shuffle8<>+0x0e(SB)/1, $(9)
DATA shuffle8<>+0x0f(SB)/1, $(8)
DATA shuffle8<>+0x10(SB)/1, $(7)
DATA shuffle8<>+0x11(SB)/1, $(6)
DATA shuffle8<>+0x12(SB)/1, $(5)
DATA shuffle8<>+0x13(SB)/1, $(4)
DATA shuffle8<>+0x14(SB)/1, $(3)
DATA shuffle8<>+0x15(SB)/1, $(2)
DATA shuffle8<>+0x16(SB)/1, $(1)
DATA shuffle8<>+0x17(SB)/1, $(0)
DATA shuffle8<>+0x18(SB)/1, $(15)
DATA shuffle8<>+0x19(SB)/1, $(14)
DATA shuffle8<>+0x1a(SB)/1, $(13)
DATA shuffle8<>+0x1b(SB)/1, $(12)
DATA shuffle8<>+0x1c(SB)/1, $(11)
DATA shuffle8<>+0x1d(SB)/1, $(10)
DATA shuffle8<>+0x1e(SB)/1, $(9)
DATA shuffle8<>+0x1f(SB)/1, $(8)
GLOBL shuffle8<>(SB), (RODATA+NOPTR), $32

// VPSHUFB input required to spread bytes in each word
// used by all 64 bit compare algorithms
DATA shuffle64<>+0x00(SB)/1, $(5)
DATA shuffle64<>+0x01(SB)/1, $(4)
DATA shuffle64<>+0x02(SB)/1, $(13)
DATA shuffle64<>+0x03(SB)/1, $(12)
DATA shuffle64<>+0x04(SB)/1, $(7)
DATA shuffle64<>+0x05(SB)/1, $(6)
DATA shuffle64<>+0x06(SB)/1, $(15)
DATA shuffle64<>+0x07(SB)/1, $(14)
DATA shuffle64<>+0x08(SB)/1, $(1)
DATA shuffle64<>+0x09(SB)/1, $(0)
DATA shuffle64<>+0x0a(SB)/1, $(9)
DATA shuffle64<>+0x0b(SB)/1, $(8)
DATA shuffle64<>+0x0c(SB)/1, $(3)
DATA shuffle64<>+0x0d(SB)/1, $(2)
DATA shuffle64<>+0x0e(SB)/1, $(11)
DATA shuffle64<>+0x0f(SB)/1, $(10)
DATA shuffle64<>+0x10(SB)/1, $(5)
DATA shuffle64<>+0x11(SB)/1, $(4)
DATA shuffle64<>+0x12(SB)/1, $(13)
DATA shuffle64<>+0x13(SB)/1, $(12)
DATA shuffle64<>+0x14(SB)/1, $(7)
DATA shuffle64<>+0x15(SB)/1, $(6)
DATA shuffle64<>+0x16(SB)/1, $(15)
DATA shuffle64<>+0x17(SB)/1, $(14)
DATA shuffle64<>+0x18(SB)/1, $(1)
DATA shuffle64<>+0x19(SB)/1, $(0)
DATA shuffle64<>+0x1a(SB)/1, $(9)
DATA shuffle64<>+0x1b(SB)/1, $(8)
DATA shuffle64<>+0x1c(SB)/1, $(3)
DATA shuffle64<>+0x1d(SB)/1, $(2)
DATA shuffle64<>+0x1e(SB)/1, $(11)
DATA shuffle64<>+0x1f(SB)/1, $(10)
GLOBL shuffle64<>(SB), (RODATA+NOPTR), $32

// VPSHUFB input required to spread bytes in each word
// used by all 32 bit compare algorithms
DATA shuffle32<>+0x00(SB)/1, $(7)
DATA shuffle32<>+0x01(SB)/1, $(6)
DATA shuffle32<>+0x02(SB)/1, $(5)
DATA shuffle32<>+0x03(SB)/1, $(4)
DATA shuffle32<>+0x04(SB)/1, $(15)
DATA shuffle32<>+0x05(SB)/1, $(14)
DATA shuffle32<>+0x06(SB)/1, $(13)
DATA shuffle32<>+0x07(SB)/1, $(12)
DATA shuffle32<>+0x08(SB)/1, $(3)
DATA shuffle32<>+0x09(SB)/1, $(2)
DATA shuffle32<>+0x0a(SB)/1, $(1)
DATA shuffle32<>+0x0b(SB)/1, $(0)
DATA shuffle32<>+0x0c(SB)/1, $(11)
DATA shuffle32<>+0x0d(SB)/1, $(10)
DATA shuffle32<>+0x0e(SB)/1, $(9)
DATA shuffle32<>+0x0f(SB)/1, $(8)
DATA shuffle32<>+0x10(SB)/1, $(7)
DATA shuffle32<>+0x11(SB)/1, $(6)
DATA shuffle32<>+0x12(SB)/1, $(5)
DATA shuffle32<>+0x13(SB)/1, $(4)
DATA shuffle32<>+0x14(SB)/1, $(15)
DATA shuffle32<>+0x15(SB)/1, $(14)
DATA shuffle32<>+0x16(SB)/1, $(13)
DATA shuffle32<>+0x17(SB)/1, $(12)
DATA shuffle32<>+0x18(SB)/1, $(3)
DATA shuffle32<>+0x19(SB)/1, $(2)
DATA shuffle32<>+0x1a(SB)/1, $(1)
DATA shuffle32<>+0x1b(SB)/1, $(0)
DATA shuffle32<>+0x1c(SB)/1, $(11)
DATA shuffle32<>+0x1d(SB)/1, $(10)
DATA shuffle32<>+0x1e(SB)/1, $(9)
DATA shuffle32<>+0x1f(SB)/1, $(8)
GLOBL shuffle32<>(SB), (RODATA+NOPTR), $32

// look up table for reverting nibbles
// used by bitset revert algorithm
DATA LUT_reverse<>+0x00(SB)/1, $(0)
DATA LUT_reverse<>+0x01(SB)/1, $(8)
DATA LUT_reverse<>+0x02(SB)/1, $(4)
DATA LUT_reverse<>+0x03(SB)/1, $(12)
DATA LUT_reverse<>+0x04(SB)/1, $(2)
DATA LUT_reverse<>+0x05(SB)/1, $(10)
DATA LUT_reverse<>+0x06(SB)/1, $(6)
DATA LUT_reverse<>+0x07(SB)/1, $(14)
DATA LUT_reverse<>+0x08(SB)/1, $(1)
DATA LUT_reverse<>+0x09(SB)/1, $(9)
DATA LUT_reverse<>+0x0a(SB)/1, $(5)
DATA LUT_reverse<>+0x0b(SB)/1, $(13)
DATA LUT_reverse<>+0x0c(SB)/1, $(3)
DATA LUT_reverse<>+0x0d(SB)/1, $(11)
DATA LUT_reverse<>+0x0e(SB)/1, $(7)
DATA LUT_reverse<>+0x0f(SB)/1, $(15)
DATA LUT_reverse<>+0x10(SB)/1, $(0)
DATA LUT_reverse<>+0x11(SB)/1, $(8)
DATA LUT_reverse<>+0x12(SB)/1, $(4)
DATA LUT_reverse<>+0x13(SB)/1, $(12)
DATA LUT_reverse<>+0x14(SB)/1, $(2)
DATA LUT_reverse<>+0x15(SB)/1, $(10)
DATA LUT_reverse<>+0x16(SB)/1, $(6)
DATA LUT_reverse<>+0x17(SB)/1, $(14)
DATA LUT_reverse<>+0x18(SB)/1, $(1)
DATA LUT_reverse<>+0x19(SB)/1, $(9)
DATA LUT_reverse<>+0x1a(SB)/1, $(5)
DATA LUT_reverse<>+0x1b(SB)/1, $(13)
DATA LUT_reverse<>+0x1c(SB)/1, $(3)
DATA LUT_reverse<>+0x1d(SB)/1, $(11)
DATA LUT_reverse<>+0x1e(SB)/1, $(7)
DATA LUT_reverse<>+0x1f(SB)/1, $(15)
GLOBL LUT_reverse<>(SB), (RODATA+NOPTR), $32

// VPERMQ input required to revert qwords within YMM-register
DATA perm_reverse<>+0x00(SB)/4, $(6)
DATA perm_reverse<>+0x04(SB)/4, $(7)
DATA perm_reverse<>+0x08(SB)/4, $(4)
DATA perm_reverse<>+0x0c(SB)/4, $(5)
DATA perm_reverse<>+0x10(SB)/4, $(2)
DATA perm_reverse<>+0x14(SB)/4, $(3)
DATA perm_reverse<>+0x18(SB)/4, $(0)
DATA perm_reverse<>+0x1c(SB)/4, $(1)
GLOBL perm_reverse<>(SB), (RODATA+NOPTR), $32
