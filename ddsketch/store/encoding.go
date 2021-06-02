package store

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

const (
	// >= 128 reserved for bin encoding
	EncodingCountOneIndexesVarInt = byte(128)
	// EncodingIndexesCountOneInt8  = byte(128)
	// EncodingIndexesCountOneInt16  = byte(129)
	// EncodingIndexesCountOneInt32  = byte(130)
	EncodingCountOneIndexesInt64    = byte(131)
	EncodingContiguousCountsFloat64 = byte(136)
	// EncodingBinInt8Float64  = byte(144)
	// EncodingBinInt16Float64  = byte(145)
	// EncodingBinInt32Float64  = byte(146)
	EncodingBinInt64Float64 = byte(147)
)

var (
	byteOrder = binary.LittleEndian

	ErrDecoding = errors.New("decoding error")
)

func FallbackEncode(s Store, bytes []byte) []byte {

	offset := 0
	for bin := range s.Bins() {
		bytes = append(bytes, make([]byte, 17)...)
		bytes[offset] = EncodingBinInt64Float64
		byteOrder.PutUint64(bytes[offset+1:], uint64(bin.index))
		byteOrder.PutUint64(bytes[offset+9:], math.Float64bits(bin.count))
		offset += 17
	}
	return bytes
}

func FallbackEncodeIOWriter(s Store, w io.Writer) error {
	var buf [8]byte
	var err error
	for bin := range s.Bins() {
		_, err = w.Write([]byte{EncodingBinInt64Float64})
		if err != nil {
			return err
		}
		byteOrder.PutUint64(buf[:], uint64(bin.index))
		_, err = w.Write(buf[:])
		if err != nil {
			return err
		}
		byteOrder.PutUint64(buf[:], math.Float64bits(bin.count))
		_, err = w.Write(buf[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func FallbackDecodeAndMergeBytes(s Store, b []byte) ([]byte, error) {
	var err error

	for len(b) > 0 {
		switch b[0] {
		case EncodingCountOneIndexesVarInt:
			var numIndexes uint64
			numIndexes, b, err = DecodeVarInt(b[1:])
			if err != nil {
				return b, err
			}

			for i := uint64(0); i < numIndexes; i++ {
				var index uint64
				index, b, err = DecodeVarInt(b)
				if err != nil {
					return b, err
				}
				s.Add(int(index))
			}

		case EncodingCountOneIndexesInt64:
			var numIndexes uint64
			numIndexes, b, err = DecodeVarInt(b[1:])
			if err != nil {
				return b, err
			}

			for i := uint64(0); i < numIndexes; i++ {
				var index uint64
				index, b, err = DecodeUint64(b)
				if err != nil {
					return b, err
				}
				s.Add(int(index))
			}

		case EncodingBinInt64Float64:
			var index uint64
			index, b, err = DecodeUint64(b[1:])
			if err != nil {
				return b, err
			}

			var countBits uint64
			countBits, b, err = DecodeUint64(b)
			if err != nil {
				return b, err
			}

			s.AddWithCount(int(index), math.Float64frombits(countBits))

		case EncodingContiguousCountsFloat64:
			var indexOffset uint64
			indexOffset, b, err = DecodeVarInt(b[1:])
			if err != nil {
				return b, err
			}

			var numCounts uint64
			numCounts, b, err = DecodeVarInt(b)
			if err != nil {
				return b, err
			}

			for i := int(indexOffset); i < int(indexOffset+numCounts); i++ {
				var countBits uint64
				countBits, b, err = DecodeUint64(b)
				if err != nil {
					return b, err
				}
				s.AddWithCount(i, math.Float64frombits(countBits))
			}

		default:
			return b, errors.New("unknown block type")
		}
	}
	return b, nil
}

type IOReader interface {
	io.ByteScanner
	io.Reader
}

func FallbackDecodeAndMergeIOReader(s Store, r IOReader) error {
	for {
		b, err := r.ReadByte()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		switch b {
		case EncodingCountOneIndexesVarInt:
			var numIndexes uint64
			numIndexes, err = binary.ReadUvarint(r)
			if err != nil {
				return err
			}

			for i := uint64(0); i < numIndexes; i++ {
				var index uint64
				index, err = binary.ReadUvarint(r)
				if err != nil {
					return err
				}
				s.Add(int(index))
			}

		case EncodingCountOneIndexesInt64:
			var numIndexes uint64
			numIndexes, err = binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			// var buf [64]byte
			// i := 0
			// for ; i+64 < 8*int(numIndexes); i += 64 {
			// 	_, err := io.ReadFull(r, buf[:])
			// 	if err != nil {
			// 		return err
			// 	}
			// 	for j := 0; j < len(buf); j += 8 {
			// 		s.Add(int(Encoder.Uint64(buf[j:])))
			// 	}
			// }
			// _, err := io.ReadFull(r, buf[:8*int(numIndexes)-i])
			// if err != nil {
			// 	return err
			// }
			// for j := 0; j < 8*int(numIndexes)-i; j += 8 {
			// 	s.Add(int(Encoder.Uint64(buf[j:])))
			// }

			bytes := make([]byte, 8*numIndexes)
			_, err := io.ReadFull(r, bytes)
			if err != nil {
				return err
			}
			for i := 0; i < len(bytes); i += 8 {
				s.Add(int(byteOrder.Uint64(bytes[i:])))
			}

		case EncodingBinInt64Float64:
			var index uint64
			index, err = ReadUint64(r)
			if err != nil {
				return err
			}

			var countBits uint64
			countBits, err = ReadUint64(r)
			if err != nil {
				return err
			}

			s.AddWithCount(int(index), math.Float64frombits(countBits))

		case EncodingContiguousCountsFloat64:
			var indexOffset uint64
			indexOffset, err = binary.ReadUvarint(r)
			if err != nil {
				return err
			}

			var numCounts uint64
			numCounts, err = binary.ReadUvarint(r)
			if err != nil {
				return err
			}

			for i := int(indexOffset); i < int(indexOffset+numCounts); i++ {
				var countBits uint64
				countBits, err = ReadUint64(r)
				if err != nil {
					return err
				}
				s.AddWithCount(i, math.Float64frombits(countBits))
			}

		default:
			return errors.New("unknown block type")
		}
	}
}

type CustomReader interface {
	io.ByteScanner
	Next(n int) []byte
}

type BytesCustomReader struct {
	b   []byte
	off int
}

func NewBytesCustomReader(b []byte) *BytesCustomReader {
	return &BytesCustomReader{b: b, off: 0}
}

func (r *BytesCustomReader) Next(n int) []byte {
	newOff := r.off + n // FIXME: overflow, negative
	if newOff > len(r.b) {
		newOff = len(r.b)
	}
	data := r.b[r.off:newOff]
	r.off = newOff
	return data
}

func (r *BytesCustomReader) ReadByte() (byte, error) {
	if r.off >= len(r.b) {
		return byte(0), io.EOF
	}
	val := r.b[r.off]
	r.off++
	return byte(val), nil
}

func (r *BytesCustomReader) UnreadByte() error {
	if r.off <= 0 {
		return errors.New("err")
	}
	r.off--
	return nil
}

func (r *BytesCustomReader) Reset(b []byte) {
	r.b = b
	r.off = 0
}

var _ CustomReader = (*BytesCustomReader)(nil)

func FallbackDecodeAndMergeCustomReader(s Store, r CustomReader) error {
	for {
		b, err := r.ReadByte()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		switch b {
		case EncodingCountOneIndexesVarInt:
			var numIndexes uint64
			numIndexes, err = binary.ReadUvarint(r)
			if err != nil {
				return err
			}

			for i := uint64(0); i < numIndexes; i++ {
				var index uint64
				index, err = binary.ReadUvarint(r)
				if err != nil {
					return err
				}
				s.Add(int(index))
			}

		case EncodingCountOneIndexesInt64:
			var numIndexes uint64
			numIndexes, err = binary.ReadUvarint(r)
			if err != nil {
				return err
			}

			view := r.Next(8 * int(numIndexes))
			if len(view) < 8*int(numIndexes) {
				return io.ErrUnexpectedEOF
			}
			for i := 0; i < len(view); i += 8 {
				s.Add(int(byteOrder.Uint64(view[i:])))
			}

			// for i := uint64(0); i < numIndexes; i++ {
			// 	var index uint64
			// 	index, err = ReadUint64(r)
			// 	if err != nil {
			// 		return err
			// 	}
			// 	s.Add(int(index))
			// }

		case EncodingBinInt64Float64:
			view := r.Next(8)
			if len(view) < 8 {
				return io.ErrUnexpectedEOF
			}
			index := byteOrder.Uint64(view)

			view = r.Next(8)
			if len(view) < 8 {
				return io.ErrUnexpectedEOF
			}
			countBits := byteOrder.Uint64(view)

			s.AddWithCount(int(index), math.Float64frombits(countBits))

		case EncodingContiguousCountsFloat64:
			var indexOffset uint64
			indexOffset, err = binary.ReadUvarint(r)
			if err != nil {
				return err
			}

			var numCounts uint64
			numCounts, err = binary.ReadUvarint(r)
			if err != nil {
				return err
			}

			for i := int(indexOffset); i < int(indexOffset+numCounts); i++ {
				view := r.Next(8)
				if len(view) < 8 {
					return io.ErrUnexpectedEOF
				}
				countBits := byteOrder.Uint64(view)
				s.AddWithCount(i, math.Float64frombits(countBits))
			}

		default:
			r.UnreadByte()
			return errors.New("unknown block type")
		}
	}
}

func EncodeUint64(b []byte, v uint64) []byte {
	b = append(b, make([]byte, 8)...)
	byteOrder.PutUint64(b[len(b)-8:], v)
	return b
}

func DecodeUint64(b []byte) (uint64, []byte, error) {
	if len(b) < 8 {
		return 0, b, io.ErrUnexpectedEOF
	}
	return byteOrder.Uint64(b), b[8:], nil
}

func EncodeVarInt(bytes []byte, i uint64) []byte {
	for i >= uint64(0x80) {
		bytes = append(bytes, byte(i)|byte(0x80))
		i >>= 7
	}
	return append(bytes, byte(i))
}

func DecodeVarInt(b []byte) (uint64, []byte, error) {
	// TODO: implement as in Molecule
	val := uint64(0)
	for i := 0; i < len(b); i++ {
		v := b[i]
		val += uint64(v&byte(0x7F)) << (7 * i)
		if v&byte(0x80) == 0 {
			return val, b[i+1:], nil
		}
	}
	return 0, b, io.ErrUnexpectedEOF
}

func ReadUint64(r io.Reader) (uint64, error) {
	var b [8]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, err // FIXME
	}
	return byteOrder.Uint64(b[:]), nil
}
