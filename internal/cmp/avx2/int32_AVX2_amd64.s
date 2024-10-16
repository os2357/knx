// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX2.h"

// func cmp_i32_eq_x2(src []int32, val int32, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i32_eq_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTD val+24(FP), Y0                 // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX


// works for >= 128 int32 (i.e. 512 bytes of data)
loop_big:
	VPCMPEQD	0(SI), Y0, Y1
	VPCMPEQD	32(SI), Y0, Y2
	VPCMPEQD	64(SI), Y0, Y3
	VPCMPEQD	96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPEQD	128(SI), Y0, Y5
	VPCMPEQD	160(SI), Y0, Y6
	VPCMPEQD	192(SI), Y0, Y7
	VPCMPEQD	224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VPCMPEQD	256(SI), Y0, Y1
	VPCMPEQD	288(SI), Y0, Y2
	VPCMPEQD	320(SI), Y0, Y3
	VPCMPEQD	352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPEQD	384(SI), Y0, Y5
	VPCMPEQD	416(SI), Y0, Y6
	VPCMPEQD	448(SI), Y0, Y7
	VPCMPEQD	480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQD	0(SI), Y0, Y1
	VPCMPEQD	32(SI), Y0, Y2
	VPCMPEQD	64(SI), Y0, Y3
	VPCMPEQD	96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVL	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int32
scalar:
	MOVL	(SI), R8
	CMPL	R8, DX
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	4(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func cmp_i32_ne_x2(src []int32, val int32, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i32_ne_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTD val+24(FP), Y0                 // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX


// works for >= 128 int32 (i.e. 512 bytes of data)
loop_big:
	VPCMPEQD	0(SI), Y0, Y1
	VPCMPEQD	32(SI), Y0, Y2
	VPCMPEQD	64(SI), Y0, Y3
	VPCMPEQD	96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPEQD	128(SI), Y0, Y5
	VPCMPEQD	160(SI), Y0, Y6
	VPCMPEQD	192(SI), Y0, Y7
	VPCMPEQD	224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VPCMPEQD	256(SI), Y0, Y1
	VPCMPEQD	288(SI), Y0, Y2
	VPCMPEQD	320(SI), Y0, Y3
	VPCMPEQD	352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPEQD	384(SI), Y0, Y5
	VPCMPEQD	416(SI), Y0, Y6
	VPCMPEQD	448(SI), Y0, Y7
	VPCMPEQD	480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQD	0(SI), Y0, Y1
	VPCMPEQD	32(SI), Y0, Y2
	VPCMPEQD	64(SI), Y0, Y3
	VPCMPEQD	96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
    NOTL        AX
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVL	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int32
scalar:
	MOVL	(SI), R8
	CMPL	R8, DX
	SETNE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	4(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func cmp_i32_lt_x2(src []int32, val int32, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i32_lt_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTD 	val+24(FP), Y0                   // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX


// works for >= 128 int32 (i.e. 512 bytes of data)
loop_big:
	VPCMPGTD		0(SI), Y0, Y1     
	VPCMPGTD		32(SI), Y0, Y2
	VPCMPGTD		64(SI), Y0, Y3
	VPCMPGTD		96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPGTD		128(SI), Y0, Y5      
	VPCMPGTD		160(SI), Y0, Y6
	VPCMPGTD		192(SI), Y0, Y7
	VPCMPGTD		224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VPCMPGTD		256(SI), Y0, Y1     
	VPCMPGTD		288(SI), Y0, Y2
	VPCMPGTD		320(SI), Y0, Y3
	VPCMPGTD		352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPGTD		384(SI), Y0, Y5      
	VPCMPGTD		416(SI), Y0, Y6
	VPCMPGTD		448(SI), Y0, Y7
	VPCMPGTD		480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPGTD		0(SI), Y0, Y1     
	VPCMPGTD		32(SI), Y0, Y2
	VPCMPGTD		64(SI), Y0, Y3
	VPCMPGTD		96(SI), Y0, Y4
    
	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVL	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int32
scalar:
	MOVL	(SI), R8
	CMPL	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	4(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func cmp_i32_le_x2(src []int32, val int32, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i32_le_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTD 	val+24(FP), Y0                   // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX


// works for >= 128 int32 (i.e. 512 bytes of data)
loop_big:
	VMOVDQU		0(SI), Y1      
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPCMPGTD	Y0, Y1, Y1     // signed compare
	VPCMPGTD	Y0, Y2, Y2
	VPCMPGTD	Y0, Y3, Y3
	VPCMPGTD	Y0, Y4, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		128(SI), Y5      
	VMOVDQU		160(SI), Y6
	VMOVDQU		192(SI), Y7
	VMOVDQU		224(SI), Y8
	VPCMPGTD	Y0, Y5, Y5     // signed compare
	VPCMPGTD	Y0, Y6, Y6
	VPCMPGTD	Y0, Y7, Y7
	VPCMPGTD	Y0, Y8, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		256(SI), Y1      
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VPCMPGTD	Y0, Y1, Y1     // signed compare
	VPCMPGTD	Y0, Y2, Y2
	VPCMPGTD	Y0, Y3, Y3
	VPCMPGTD	Y0, Y4, Y4
	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		384(SI), Y5      // load values (necessary to flip sign bit)
	VMOVDQU		416(SI), Y6
	VMOVDQU		448(SI), Y7
	VMOVDQU		480(SI), Y8
	VPCMPGTD	Y0, Y5, Y5     // signed compare
	VPCMPGTD	Y0, Y6, Y6
	VPCMPGTD	Y0, Y7, Y7
	VPCMPGTD	Y0, Y8, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPCMPGTD	Y0, Y1, Y1     // signed compare
	VPCMPGTD	Y0, Y2, Y2
	VPCMPGTD	Y0, Y3, Y3
	VPCMPGTD	Y0, Y4, Y4
    
	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
    NOTL        AX
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVL	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int32
scalar:
	MOVL	(SI), R8
	CMPL	R8, DX
	SETLE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	4(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func cmp_i32_gt_x2(src []int32, val int32, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i32_gt_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTD 	val+24(FP), Y0                   // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 128 int32 (i.e. 512 bytes of data)
loop_big:
	VMOVDQU		0(SI), Y1      
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPCMPGTD	Y0, Y1, Y1     // signed compare
	VPCMPGTD	Y0, Y2, Y2
	VPCMPGTD	Y0, Y3, Y3
	VPCMPGTD	Y0, Y4, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		128(SI), Y5      
	VMOVDQU		160(SI), Y6
	VMOVDQU		192(SI), Y7
	VMOVDQU		224(SI), Y8
	VPCMPGTD	Y0, Y5, Y5     // signed compare
	VPCMPGTD	Y0, Y6, Y6
	VPCMPGTD	Y0, Y7, Y7
	VPCMPGTD	Y0, Y8, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		256(SI), Y1      
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VPCMPGTD	Y0, Y1, Y1     // signed compare
	VPCMPGTD	Y0, Y2, Y2
	VPCMPGTD	Y0, Y3, Y3
	VPCMPGTD	Y0, Y4, Y4
	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		384(SI), Y5      
	VMOVDQU		416(SI), Y6
	VMOVDQU		448(SI), Y7
	VMOVDQU		480(SI), Y8
	VPCMPGTD	Y0, Y5, Y5     // signed compare
	VPCMPGTD	Y0, Y6, Y6
	VPCMPGTD	Y0, Y7, Y7
	VPCMPGTD	Y0, Y8, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1     
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPCMPGTD	Y0, Y1, Y1     // signed compare
	VPCMPGTD	Y0, Y2, Y2
	VPCMPGTD	Y0, Y3, Y3
	VPCMPGTD	Y0, Y4, Y4
    
	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVL	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int32
scalar:
	MOVL	(SI), R8
	CMPL	R8, DX
	SETGT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	4(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET
    
// func cmp_i32_ge_x2(src []int32, val int32, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i32_ge_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTD 	val+24(FP), Y0                   // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX


// works for >= 128 int32 (i.e. 512 bytes of data)
loop_big:
	VPCMPGTD		0(SI), Y0, Y1      
	VPCMPGTD		32(SI), Y0, Y2
	VPCMPGTD		64(SI), Y0, Y3
	VPCMPGTD		96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPGTD		128(SI), Y0, Y5    
	VPCMPGTD		160(SI), Y0, Y6
	VPCMPGTD		192(SI), Y0, Y7
	VPCMPGTD		224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VPCMPGTD		256(SI), Y0, Y1      
	VPCMPGTD		288(SI), Y0, Y2
	VPCMPGTD		320(SI), Y0, Y3
	VPCMPGTD		352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPGTD		384(SI), Y0, Y5    
	VPCMPGTD		416(SI), Y0, Y6
	VPCMPGTD		448(SI), Y0, Y7
	VPCMPGTD		480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPGTD		0(SI), Y0, Y1      
	VPCMPGTD		32(SI), Y0, Y2
	VPCMPGTD		64(SI), Y0, Y3
	VPCMPGTD		96(SI), Y0, Y4
    
	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
    NOTL        AX
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVL	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int32
scalar:
	MOVL	(SI), R8
	CMPL	R8, DX
	SETGE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	4(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func cmp_i32_bw_x2(src []int32, a, b int32, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   R10 = lower comparison value for scalar
//   R11 = upper comparison value for scalar
//   Y0 = lower comparison value for AVX2
//   Y11 = upper comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_i32_bw_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

// check is using GT with switched operands and add
// the diff method to avoid jumps:
// 	diff := b - a + 1
//  v-a < diff
prep_avx:
	VPCMPEQQ		Y11, Y11, Y11                   // create 0x8000.. mask
	VPSLLD			$31, Y11, Y11                   // create 0x8000.. mask
	VPCMPEQD		Y13, Y13, Y13                   // create 1 for adding
	VPSRLD			$31, Y13, Y13
	VPBROADCASTD 	a+24(FP), Y12                   // load val a into AVX2 reg
	VPBROADCASTD 	b+28(FP), Y0                    // load val b into AVX2 reg
	VPSUBD			Y12, Y0, Y0                     // compute diff
	VPADDD			Y13, Y0, Y0
	VPXOR			Y11, Y0, Y0                     // flip sign bit
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX


// works for >= 128 int32 (i.e. 512 bytes of data)
loop_big:
	VMOVDQU		0(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPSUBD		Y12, Y1, Y1
	VPSUBD		Y12, Y2, Y2
	VPSUBD		Y12, Y3, Y3
	VPSUBD		Y12, Y4, Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTD	Y1, Y0, Y1     // signed compare
	VPCMPGTD	Y2, Y0, Y2
	VPCMPGTD	Y3, Y0, Y3
	VPCMPGTD	Y4, Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		128(SI), Y5      // load values (necessary to flip sign bit)
	VMOVDQU		160(SI), Y6
	VMOVDQU		192(SI), Y7
	VMOVDQU		224(SI), Y8
	VPSUBD		Y12, Y5, Y5
	VPSUBD		Y12, Y6, Y6
	VPSUBD		Y12, Y7, Y7
	VPSUBD		Y12, Y8, Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTD	Y5, Y0, Y5     // signed compare
	VPCMPGTD	Y6, Y0, Y6
	VPCMPGTD	Y7, Y0, Y7
	VPCMPGTD	Y8, Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		256(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VPSUBD		Y12, Y1, Y1
	VPSUBD		Y12, Y2, Y2
	VPSUBD		Y12, Y3, Y3
	VPSUBD		Y12, Y4, Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTD	Y1, Y0, Y1     // signed compare
	VPCMPGTD	Y2, Y0, Y2
	VPCMPGTD	Y3, Y0, Y3
	VPCMPGTD	Y4, Y0, Y4
	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		384(SI), Y5      // load values (necessary to flip sign bit)
	VMOVDQU		416(SI), Y6
	VMOVDQU		448(SI), Y7
	VMOVDQU		480(SI), Y8
	VPSUBD		Y12, Y5, Y5
	VPSUBD		Y12, Y6, Y6
	VPSUBD		Y12, Y7, Y7
	VPSUBD		Y12, Y8, Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTD	Y5, Y0, Y5     // signed compare
	VPCMPGTD	Y6, Y0, Y6
	VPCMPGTD	Y7, Y0, Y7
	VPCMPGTD	Y8, Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPSUBD		Y12, Y1, Y1
	VPSUBD		Y12, Y2, Y2
	VPSUBD		Y12, Y3, Y3
	VPSUBD		Y12, Y4, Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTD	Y1, Y0, Y1     // signed compare
	VPCMPGTD	Y2, Y0, Y2
	VPCMPGTD	Y3, Y0, Y3
	VPCMPGTD	Y4, Y0, Y4
    
	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVL	a+24(FP), R13   // load val a
	MOVL	b+28(FP), DX    // load val b
	SUBL	R13, DX
	INCL	DX
	MOVQ    $1, R12          // create 0x80... mask
	SHLL    $31, R12
	XORL    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int32
scalar:
	MOVL	(SI), R8
	SUBL	R13, R8          // v - a < diff
	XORL    R12, R8          // flip sign bit
	CMPL	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	4(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET
