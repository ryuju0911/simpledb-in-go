package file

import (
	"encoding/binary"
	"io"
)

// How data is stored in a Page at a given `offset`:
//
//	 [ Record starts at `offset` ]
//	<------------------ 4 + N bytes ------------------>
//	+-----------------+-------------------------------+
//	|   Length (size) |            Content            |
//	|    (4 bytes)    |           (N bytes)           |
//	+-----------------+-------------------------------+
//	^                 ^                               ^
//	|                 |                               |
//
// offset           offset + 4                      offset + 4 + N

// Page provides methods for reading and writing data to a fixed-size byte slice,
// which corresponds to a disk block. It is essentially a wrapper around a []byte
// that provides convenient, offset-based I/O operations.
type Page struct {
	buf []byte
}

// NewPage creates a new page backed by a byte slice of the specified size.
func NewPage(blockSize int32) *Page {
	return &Page{buf: make([]byte, blockSize)}
}

// Buf returns the underlying byte slice of the page.
func (p *Page) Buf() []byte {
	return p.buf
}

// WriteInt32At writes an int32 value to the page at a specific offset.
// It returns an io.EOF error if the write would exceed the page's bounds.
func (p *Page) WriteInt32At(offset int32, n int32) error {
	if offset+4 > int32(len(p.buf)) {
		return io.EOF
	}
	binary.BigEndian.PutUint32(p.buf[offset:], uint32(n))
	return nil
}

// ReadInt32At reads an int32 value from the page at a specific offset.
// It returns an io.EOF error if the read would exceed the page's bounds.
func (p *Page) ReadInt32At(offset int32) (int32, error) {
	if offset+4 > int32(len(p.buf)) {
		return 0, io.EOF
	}
	return int32(binary.BigEndian.Uint32(p.buf[offset : offset+4])), nil
}

// WriteBytesAt writes a byte slice to the page at a specific offset.
// It first writes the length of the slice as a 4-byte integer, followed by the
// bytes of the slice itself.
// It returns an io.EOF error if the write would exceed the page's bounds.
func (p *Page) WriteBytesAt(offset int32, b []byte) error {
	if offset+4+int32(len(b)) > int32(len(p.buf)) {
		return io.EOF
	}
	p.WriteInt32At(offset, int32(len(b)))
	copy(p.buf[offset+4:], b)
	return nil
}

// ReadBytesAt reads a byte slice from the page at a specific offset.
// It first reads a 4-byte integer representing the length of the slice,
// and then returns the subsequent bytes.
// It returns an io.EOF error if the read would exceed the page's bounds.
func (p *Page) ReadBytesAt(offset int32) ([]byte, error) {
	length, err := p.ReadInt32At(offset)
	if err != nil {
		return nil, err
	}
	if offset+4+length > int32(len(p.buf)) {
		return nil, io.EOF
	}

	b := make([]byte, length)
	copy(b, p.buf[offset+4:offset+4+length])
	return b, nil
}

// WriteStringAt writes a string to the page at a specific offset.
// It is a convenience wrapper around WriteBytesAt. The string is stored as a
// 4-byte length prefix followed by its byte representation.
// It returns an io.EOF error if the write would exceed the page's bounds.
func (p *Page) WriteStringAt(offset int32, s string) error {
	return p.WriteBytesAt(offset, []byte(s))
}

// ReadStringAt reads a string from the page at a specific offset.
// It is a convenience wrapper around ReadBytesAt, reading a length-prefixed
// byte slice and converting it to a string.
// It returns an io.EOF error if the read would exceed the page's bounds.
func (p *Page) ReadStringAt(offset int32) (string, error) {
	b, err := p.ReadBytesAt(offset)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
