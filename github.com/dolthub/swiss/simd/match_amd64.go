// Code generated by command: go run asm.go -out match.s -stubs match_amd64.go. DO NOT EDIT.

//go:build amd64

package simd

// MatchMetadata performs a 16-way probe of |metadata| using SSE instructions
// nb: |metadata| must be an aligned pointer
func MatchMetadata(metadata *[16]int8, hash int8) uint16  // 匹配 16  字节，返回 16 bit 的  mask
