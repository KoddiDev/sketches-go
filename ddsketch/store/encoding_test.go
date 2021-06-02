package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeVarInt(t *testing.T) {
	assert.Equal(t, []byte{0}, EncodeVarInt([]byte{}, 0))
	assert.Equal(t, []byte{1}, EncodeVarInt([]byte{}, 1))
	assert.Equal(t, []byte{127}, EncodeVarInt([]byte{}, 127))
	assert.Equal(t, []byte{128, 1}, EncodeVarInt([]byte{}, 128))
	assert.Equal(t, []byte{129, 1}, EncodeVarInt([]byte{}, 129))
	assert.Equal(t, []byte{255, 1}, EncodeVarInt([]byte{}, 255))
	assert.Equal(t, []byte{128, 2}, EncodeVarInt([]byte{}, 256))
	assert.Equal(t, []byte{255, 127}, EncodeVarInt([]byte{}, 16383))
	assert.Equal(t, []byte{128, 128, 1}, EncodeVarInt([]byte{}, 16384))
	assert.Equal(t, []byte{129, 128, 1}, EncodeVarInt([]byte{}, 16385))
}

func TestDecodeVarInt(t *testing.T) {
	var i uint64
	var b []byte
	var err error

	i, b, err = DecodeVarInt([]byte{0})
	assert.Equal(t, uint64(0), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)

	i, b, err = DecodeVarInt([]byte{1})
	assert.Equal(t, uint64(1), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)

	i, b, err = DecodeVarInt([]byte{127})
	assert.Equal(t, uint64(127), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)

	i, b, err = DecodeVarInt([]byte{128, 1})
	assert.Equal(t, uint64(128), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)

	i, b, err = DecodeVarInt([]byte{129, 1})
	assert.Equal(t, uint64(129), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)

	i, b, err = DecodeVarInt([]byte{255, 1})
	assert.Equal(t, uint64(255), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)

	i, b, err = DecodeVarInt([]byte{128, 2})
	assert.Equal(t, uint64(256), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)

	i, b, err = DecodeVarInt([]byte{255, 127})
	assert.Equal(t, uint64(16383), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)

	i, b, err = DecodeVarInt([]byte{128, 128, 1})
	assert.Equal(t, uint64(16384), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)

	i, b, err = DecodeVarInt([]byte{129, 128, 1})
	assert.Equal(t, uint64(16385), i)
	assert.Equal(t, []byte{}, b)
	assert.Nil(t, err)
}
