// Code generated by command: go run asm.go -out match.s -stubs match_amd64.go. DO NOT EDIT.

//go:build amd64

#include "textflag.h"

// func MatchMetadata(metadata *[16]int8, hash int8) uint16
// Requires: SSE2, SSSE3
TEXT ·MatchMetadata(SB), NOSPLIT, $0-18
	MOVQ     metadata+0(FP), AX  // ptr
	MOVBLSX  hash+8(FP), CX      // hash value  // 移动 1 个字节，赋值到 8 个字节,, 8bit->64bit
	MOVD     CX, X0  // 把 64bit 赋值到  128bit 寄存器
	PXOR     X1, X1  // x1 = 0
	PSHUFB   X1, X0  // （Packed Shuffle Bytes）  // 应该是要匹配的字符， 1 字节扩展到 16 个位置
	MOVOU    (AX), X1  // 不对齐的加载到 x1
	PCMPEQB  X1, X0  // Packed Compare Equal Bytes  // 一次比较 16  字节
	PMOVMSKB X0, AX  // 16 字节变成  16bit
	MOVW     AX, ret+16(FP)  // 返回 bitmap
	RET
