package file

import (
	"bytes"
	"io"
	"testing"
)

func TestPage_WriteInt32At(t *testing.T) {
	const blockSize = 100

	testCases := []struct {
		name    string
		offset  int32
		val     int32
		wantErr error
	}{
		{"Write positive value to middle", 20, 12345, nil},
		{"Write negative value to start", 0, -1, nil},
		{"Write to last possible offset", 96, 98765, nil},
		{
			name:    "Write out of bounds",
			offset:  97, // 97 + 4 > 100
			val:     999,
			wantErr: io.EOF,
		},
		{
			name:    "Write at exact boundary",
			offset:  100,
			val:     999,
			wantErr: io.EOF,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPage(blockSize)
			err := p.WriteInt32At(tc.offset, tc.val)
			if err != tc.wantErr {
				t.Errorf("WriteInt32At() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr == nil {
				gotVal, _ := p.ReadInt32At(tc.offset)
				if gotVal != tc.val {
					t.Errorf("WriteInt32At() wrote %v, want %v", gotVal, tc.val)
				}
			}
		})
	}
}

func TestPage_ReadInt32At(t *testing.T) {
	const blockSize = 100
	p := NewPage(blockSize)

	// Setup: write some known values into the page buffer to test against.
	testValues := map[int32]int32{
		0:  -1,
		20: 12345,
		96: 98765, // Last possible offset
	}

	for offset, val := range testValues {
		err := p.WriteInt32At(offset, val)
		if err != nil {
			t.Fatalf("setup: WriteInt32At(%d, %d) failed: %v", offset, val, err)
		}
	}

	testCases := []struct {
		name    string
		offset  int32
		wantVal int32
		wantErr error
	}{
		{"Read positive value from middle", 20, 12345, nil},
		{"Read negative value from start", 0, -1, nil},
		{"Read from last possible offset", 96, 98765, nil},
		{
			name:    "Read out of bounds",
			offset:  97, // 97 + 4 > 100
			wantVal: 0,
			wantErr: io.EOF,
		},
		{
			name:    "Read at exact boundary",
			offset:  100,
			wantVal: 0,
			wantErr: io.EOF,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotVal, gotErr := p.ReadInt32At(tc.offset)

			if gotErr != tc.wantErr {
				t.Errorf("ReadInt32At() error = %v, wantErr %v", gotErr, tc.wantErr)
			}

			if gotVal != tc.wantVal {
				t.Errorf("ReadInt32At() gotVal = %v, want %v", gotVal, tc.wantVal)
			}
		})
	}
}

func TestPage_WriteBytesAt(t *testing.T) {
	const blockSize = 100

	testCases := []struct {
		name    string
		offset  int32
		val     []byte
		wantErr error
	}{
		{"Write normal bytes", 10, []byte("hello world"), nil},
		{"Write empty bytes", 30, []byte{}, nil},
		{"Write bytes that fill the page exactly", 91, []byte("final"), nil}, // 91+4+5 = 100
		{"Write bytes that are too long", 50, make([]byte, 60), io.EOF},      // 50+4+60 > 100
		{"Write bytes with offset out of bounds", 97, []byte("end"), io.EOF}, // 97+4+3 > 100
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPage(blockSize)

			err := p.WriteBytesAt(tc.offset, tc.val)

			if err != tc.wantErr {
				t.Errorf("WriteBytesAt() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr == nil {
				// Read back and verify.
				gotVal, err := p.ReadBytesAt(tc.offset)
				if err != nil {
					t.Fatalf("ReadBytesAt() failed during verification: %v", err)
				}
				if !bytes.Equal(gotVal, tc.val) {
					t.Errorf("WriteBytesAt() wrote %q, but ReadBytesAt() read %q", tc.val, gotVal)
				}
			}
		})
	}
}

func TestPage_ReadBytesAt(t *testing.T) {
	const blockSize = 100
	p := NewPage(blockSize)

	// Setup: write some known values into the page buffer to test against.
	testValues := map[int32][]byte{
		10: []byte("hello world"),
		30: {},
		91: []byte("final"), // Last possible offset
	}

	for offset, val := range testValues {
		err := p.WriteBytesAt(offset, val)
		if err != nil {
			t.Fatalf("setup: WriteBytesAt(%d, %q) failed: %v", offset, val, err)
		}
	}

	testCases := []struct {
		name    string
		offset  int32
		wantVal []byte
		wantErr error
	}{
		{"Read normal bytes", 10, []byte("hello world"), nil},
		{"Read empty bytes", 30, []byte{}, nil},
		{"Read bytes at end of page", 91, []byte("final"), nil},
		{"Read with length prefix out of bounds", 98, nil, io.EOF},
		{"Read at exact boundary", 100, nil, io.EOF},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotVal, gotErr := p.ReadBytesAt(tc.offset)

			if gotErr != tc.wantErr {
				t.Errorf("ReadBytesAt() error = %v, wantErr %v", gotErr, tc.wantErr)
			}

			if !bytes.Equal(gotVal, tc.wantVal) {
				t.Errorf("ReadBytesAt() gotVal = %q, want %q", gotVal, tc.wantVal)
			}
		})
	}
}
