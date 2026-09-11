package soe_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/awaken/avro/v2/soe"
	"github.com/awaken/avro/v2/soe/internal/testdata"
	"github.com/stretchr/testify/require"
)

func TestCodec_RejectsTrailingObject(t *testing.T) {
	for _, test := range []struct {
		schema string
		value  any
		target func() any
	}{
		{schema: `"int"`, value: 23, target: func() any { return new(int) }},
		{schema: `"null"`, value: nil, target: func() any { return new(any) }},
		{schema: `"bytes"`, value: bytes.Repeat([]byte{7}, 1025), target: func() any { return new([]byte) }},
	} {
		codec, err := soe.NewCodec(avro.MustParse(test.schema))
		require.NoError(t, err)
		frame, err := codec.Encode(test.value)
		require.NoError(t, err)
		for name, decode := range decoderFuncs(codec) {
			t.Run(test.schema+"/"+name, func(t *testing.T) {
				require.NoError(t, decode(frame, test.target()))
				for _, tail := range [][]byte{{0}, {0x80}, {0, 0}} {
					data := append(append([]byte(nil), frame...), tail...)
					require.Error(t, decode(data, test.target()), "accepted trailing bytes %x", tail)
				}
				require.NoError(t, decode(frame, test.target()), "failed decode contaminated the next frame")
			})
		}
	}
}

type nilAPI struct {
	avro.API
}

type exactSOEWrapper struct {
	avro.API
	calls int
}

func (a *exactSOEWrapper) UnmarshalExact(schema avro.Schema, data []byte, v any) error {
	a.calls++
	return a.API.(avro.ExactUnmarshaler).UnmarshalExact(schema, data, v)
}

func TestCodec_CustomExactAPI(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	legacy, err := soe.NewCodecWithAPI(schema, &nilAPI{API: avro.DefaultConfig})
	require.NoError(t, err)
	frame, err := legacy.Encode(12)
	require.NoError(t, err, "custom encoding remains supported")
	for _, decode := range decoderFuncs(legacy) {
		require.ErrorIs(t, decode(frame, new(int)), soe.ErrExactAPI)
	}

	api := &exactSOEWrapper{API: avro.DefaultConfig}
	codec, err := soe.NewCodecWithAPI(schema, api)
	require.NoError(t, err)
	for _, decode := range decoderFuncs(codec) {
		var got int
		require.NoError(t, decode(frame, &got))
		require.Equal(t, 12, got)
		require.Error(t, decode(append(append([]byte(nil), frame...), 0), &got))
	}
	require.Equal(t, 4, api.calls, "SOE must use the custom exact decoder")
}

func TestNewCodecWithAPIRejectsNil(t *testing.T) {
	schema := avro.MustParse("null")
	tests := []struct {
		name string
		api  avro.API
	}{
		{name: "nil", api: nil},
		{name: "typed nil", api: (*nilAPI)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			codec, err := soe.NewCodecWithAPI(schema, test.api)

			require.ErrorContains(t, err, "API cannot be nil")
			require.Nil(t, codec)
		})
	}
}

func newCodec(t *testing.T) *soe.Codec {
	t.Helper()

	codec, err := soe.NewCodec(testdata.StringIntSchema)
	require.NoError(t, err)

	return codec
}

func TestCodec_EncodeReturnsIndependentData(t *testing.T) {
	codec, err := soe.NewCodec(avro.MustParse("null"))
	require.NoError(t, err)

	first, err := codec.Encode(nil)
	require.NoError(t, err)
	want := append([]byte(nil), first...)
	first[0] = 0

	second, err := codec.Encode(nil)
	require.NoError(t, err)
	require.Equal(t, want, second)
}

// Used to test over all decoder functions.
func decoderFuncs(codec *soe.Codec) map[string]func([]byte, any) error {
	return map[string]func([]byte, any) error{
		"Decode":           codec.Decode,
		"DecodeUnverified": codec.DecodeUnverified,
	}
}

func TestCodec_Roundtrip(t *testing.T) {
	codec := newCodec(t)

	v0 := testdata.StringInt{
		StringVal: "abc",
		IntVal:    123,
	}

	// Encode
	data, err := codec.Encode(v0)
	require.NoError(t, err)

	// Test all decoders behave the same.
	for name, decoderFunc := range decoderFuncs(codec) {
		t.Run(name, func(t *testing.T) {
			var v1 testdata.StringInt
			err := decoderFunc(data, &v1)

			// All decoders should successfully decode good data.
			require.NoError(t, err)
			require.Equal(t, v0, v1)
		})
	}
}

func TestCodec_DecodeShortHeader(t *testing.T) {
	codec := newCodec(t)

	// At least 10 bytes header required
	data := []byte{
		0xc3, 0x01,
	}

	// Test all decoders behave the same.
	for name, decoderFunc := range decoderFuncs(codec) {
		t.Run(name, func(t *testing.T) {
			var v1 testdata.StringInt
			err := decoderFunc(data, &v1)

			// All decoders should validate length.
			require.ErrorContains(t, err, "too short")
		})
	}
}

func TestCodec_DecodeBadMagic(t *testing.T) {
	codec := newCodec(t)

	data := []byte{
		// Invalid magic
		0x00, 0x00,
		// Faux schema ID
		0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
		// No data payload
	}

	// Test all decoders behave the same.
	for name, decoderFunc := range decoderFuncs(codec) {
		t.Run(name, func(t *testing.T) {
			var v1 testdata.StringInt
			err := decoderFunc(data, &v1)

			// All decoders should validate the magic
			require.ErrorContains(t, err, "invalid magic")

		})
	}
}

func TestCodec_DecodeBadFingerprint(t *testing.T) {
	codec := newCodec(t)

	data := []byte{
		// Good magic
		0xc3, 0x01,
		// Faux schema ID
		0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
		// No data payload
	}

	t.Run("Decode", func(t *testing.T) {
		// Decode fails due to fingerprint mismatch
		var v1 testdata.StringInt
		err := codec.Decode(data, &v1)

		require.ErrorContains(t, err, "bad fingerprint")
	})
	t.Run("DecodeUnverified", func(t *testing.T) {
		// DecodeUnverified does not validate the fingerprint, but still
		// rejects a truncated record payload.
		var v1 testdata.StringInt
		err := codec.DecodeUnverified(data, &v1)

		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, testdata.StringInt{}, v1)
	})
}

func TestCodec_HeaderFormat(t *testing.T) {
	codec := newCodec(t)

	// Build an expected header from magic + schema fingerprint
	expectedHeader, err := soe.BuildHeader(testdata.StringIntSchema)
	require.NoError(t, err)

	// Encode an arbitrary value
	v0 := testdata.StringInt{}
	data, err := codec.Encode(v0)
	require.NoError(t, err)

	// Extract as much of SOE header as is available from payload.
	var header []byte
	if len(data) < 10 {
		header = data
	} else {
		header = data[:10]
	}

	// Compare to the actual header
	require.Equal(t, expectedHeader, header)
}
