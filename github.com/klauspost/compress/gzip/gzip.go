// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gzip

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/flate"
)

// These constants are copied from the flate package, so that code that imports
// "compress/gzip" does not also have to import "compress/flate".
const (
	NoCompression       = flate.NoCompression
	BestSpeed           = flate.BestSpeed
	BestCompression     = flate.BestCompression
	DefaultCompression  = flate.DefaultCompression
	ConstantCompression = flate.ConstantCompression
	HuffmanOnly         = flate.HuffmanOnly

	// StatelessCompression will do compression but without maintaining any state
	// between Write calls.
	// There will be no memory kept between Write calls,
	// but compression and speed will be suboptimal.
	// Because of this, the size of actual Write calls will affect output size.
	StatelessCompression = -3
)

// A Writer is an io.WriteCloser.
// Writes to a Writer are compressed and written to w.
type Writer struct {
	Header      // written at first call to Write, Flush, or Close
	w           io.Writer  // 压缩后的内容，写入的目的地址
	level       int  // 压缩级别
	err         error
	compressor  *flate.Writer
	digest      uint32 // CRC-32, IEEE polynomial (section 8)  // 所有数据的校验和
	size        uint32 // Uncompressed size (section 2.3.1)  // 压缩前的总长度
	wroteHeader bool
	closed      bool
	buf         [10]byte
}

// NewWriter returns a new Writer.
// Writes to the returned writer are compressed and written to w.
//
// It is the caller's responsibility to call Close on the WriteCloser when done.
// Writes may be buffered and not flushed until Close.
//
// Callers that wish to set the fields in Writer.Header must do so before
// the first call to Write, Flush, or Close.
func NewWriter(w io.Writer) *Writer {
	z, _ := NewWriterLevel(w, DefaultCompression)
	return z
}

// NewWriterLevel is like NewWriter but specifies the compression level instead
// of assuming DefaultCompression.
//
// The compression level can be DefaultCompression, NoCompression, or any
// integer value between BestSpeed and BestCompression inclusive. The error
// returned will be nil if the level is valid.
func NewWriterLevel(w io.Writer, level int) (*Writer, error) {
	if level < StatelessCompression || level > BestCompression {
		return nil, fmt.Errorf("gzip: invalid compression level: %d", level)
	}
	z := new(Writer)
	z.init(w, level)
	return z, nil
}

// MinCustomWindowSize is the minimum window size that can be sent to NewWriterWindow.
const MinCustomWindowSize = flate.MinCustomWindowSize

// MaxCustomWindowSize is the maximum custom window that can be sent to NewWriterWindow.
const MaxCustomWindowSize = flate.MaxCustomWindowSize

// NewWriterWindow returns a new Writer compressing data with a custom window size.
// windowSize must be from MinCustomWindowSize to MaxCustomWindowSize.
func NewWriterWindow(w io.Writer, windowSize int) (*Writer, error) {
	if windowSize < MinCustomWindowSize {
		return nil, errors.New("gzip: requested window size less than MinWindowSize")
	}
	if windowSize > MaxCustomWindowSize {
		return nil, errors.New("gzip: requested window size bigger than MaxCustomWindowSize")
	}

	z := new(Writer)
	z.init(w, -windowSize)
	return z, nil
}

func (z *Writer) init(w io.Writer, level int) {  // 初始化  Writer 对象
	compressor := z.compressor
	if level != StatelessCompression {
		if compressor != nil {
			compressor.Reset(w)
		}
	}

	*z = Writer{  // 这不白白浪费一个对象嘛  => 是为了配合 reset 方法
		Header: Header{
			OS: 255, // unknown
		},
		w:          w,
		level:      level,
		compressor: compressor,
	}
}

// Reset discards the Writer z's state and makes it equivalent to the
// result of its original state from NewWriter or NewWriterLevel, but
// writing to w instead. This permits reusing a Writer rather than
// allocating a new one.
func (z *Writer) Reset(w io.Writer) {  // 对象可以重用
	z.init(w, z.level)
}

// writeBytes writes a length-prefixed byte slice to z.w.
func (z *Writer) writeBytes(b []byte) error {  // 写元数据的块
	if len(b) > 0xffff {
		return errors.New("gzip.Write: Extra data is too large")
	}
	le.PutUint16(z.buf[:2], uint16(len(b)))
	_, err := z.w.Write(z.buf[:2])  // 每个块的前面两个字节，是数据的压缩后的长度
	if err != nil {
		return err
	}
	_, err = z.w.Write(b)
	return err
}

// writeString writes a UTF-8 string s in GZIP's format to z.w.
// GZIP (RFC 1952) specifies that strings are NUL-terminated ISO 8859-1 (Latin-1).
func (z *Writer) writeString(s string) (err error) {
	// GZIP stores Latin-1 strings; error if non-Latin-1; convert if non-ASCII.
	needconv := false
	for _, v := range s {
		if v == 0 || v > 0xff {
			return errors.New("gzip.Write: non-Latin-1 header string")
		}
		if v > 0x7f {
			needconv = true
		}
	}
	if needconv {
		b := make([]byte, 0, len(s))
		for _, v := range s {
			b = append(b, byte(v))
		}
		_, err = z.w.Write(b)
	} else {
		_, err = io.WriteString(z.w, s)
	}
	if err != nil {
		return err
	}
	// GZIP strings are NUL-terminated.
	z.buf[0] = 0
	_, err = z.w.Write(z.buf[:1])
	return err
}

// Write writes a compressed form of p to the underlying io.Writer. The
// compressed bytes are not necessarily flushed until the Writer is closed.
func (z *Writer) Write(p []byte) (int, error) {
	if z.err != nil {
		return 0, z.err
	}
	var n int
	// Write the GZIP header lazily.
	if !z.wroteHeader {  // 有个标记，去记录是不是写过数据了
		z.wroteHeader = true
		z.buf[0] = gzipID1  // 0~3 是头信息
		z.buf[1] = gzipID2
		z.buf[2] = gzipDeflate
		z.buf[3] = 0
		if z.Extra != nil {
			z.buf[3] |= 0x04
		}
		if z.Name != "" {
			z.buf[3] |= 0x08
		}
		if z.Comment != "" {
			z.buf[3] |= 0x10
		}
		le.PutUint32(z.buf[4:8], uint32(z.ModTime.Unix()))  // 4~ 7 是时间
		if z.level == BestCompression {
			z.buf[8] = 2
		} else if z.level == BestSpeed {  // 8 是压缩级别
			z.buf[8] = 4
		} else {
			z.buf[8] = 0
		}
		z.buf[9] = z.OS  // 9 是  os
		n, z.err = z.w.Write(z.buf[:10])  // 把  10  字节的头信息写进去
		if z.err != nil {
			return n, z.err
		}
		if z.Extra != nil {
			z.err = z.writeBytes(z.Extra)  // 写一个块   2 字节 length + 内容
			if z.err != nil {
				return n, z.err
			}
		}
		if z.Name != "" {
			z.err = z.writeString(z.Name)
			if z.err != nil {
				return n, z.err
			}
		}
		if z.Comment != "" {
			z.err = z.writeString(z.Comment)
			if z.err != nil {
				return n, z.err
			}
		}

		if z.compressor == nil && z.level != StatelessCompression {
			z.compressor, _ = flate.NewWriter(z.w, z.level)  // 分配一个压缩器
		}
	}
	z.size += uint32(len(p))
	z.digest = crc32.Update(z.digest, crc32.IEEETable, p)  // 计算校验和
	if z.level == StatelessCompression {
		return len(p), flate.StatelessDeflate(z.w, p, false, nil)
	}
	n, z.err = z.compressor.Write(p)  // 使用压缩器压缩内容
	return n, z.err
}

// Flush flushes any pending compressed data to the underlying writer.
//
// It is useful mainly in compressed network protocols, to ensure that
// a remote reader has enough data to reconstruct a packet. Flush does
// not return until the data has been written. If the underlying
// writer returns an error, Flush returns that error.
//
// In the terminology of the zlib library, Flush is equivalent to Z_SYNC_FLUSH.
func (z *Writer) Flush() error {
	if z.err != nil {
		return z.err
	}
	if z.closed || z.level == StatelessCompression {
		return nil
	}
	if !z.wroteHeader {
		z.Write(nil)
		if z.err != nil {
			return z.err
		}
	}
	z.err = z.compressor.Flush()
	return z.err
}

// Close closes the Writer, flushing any unwritten data to the underlying
// io.Writer, but does not close the underlying io.Writer.
func (z *Writer) Close() error {
	if z.err != nil {
		return z.err
	}
	if z.closed {
		return nil
	}
	z.closed = true
	if !z.wroteHeader {
		z.Write(nil)
		if z.err != nil {
			return z.err
		}
	}
	if z.level == StatelessCompression {
		z.err = flate.StatelessDeflate(z.w, nil, true, nil)
	} else {
		z.err = z.compressor.Close()
	}
	if z.err != nil {
		return z.err
	}
	le.PutUint32(z.buf[:4], z.digest)
	le.PutUint32(z.buf[4:8], z.size)
	_, z.err = z.w.Write(z.buf[:8])  // close 的时候，会写入最终的 crc 值和总长度
	return z.err
}
