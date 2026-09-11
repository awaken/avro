package avro_test

import (
	"bytes"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEncoder_SchemaError(t *testing.T) {
	defer ConfigTeardown()

	schema := "{}"
	_, err := avro.NewEncoder(schema, nil)

	assert.Error(t, err)
}

func TestEncoder_EncodeUnsupportedType(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.NewPrimitiveSchema(avro.Type("test"), nil)
	buf := bytes.NewBuffer([]byte{})
	enc := avro.NewEncoderForSchema(schema, buf)

	err := enc.Encode(true)

	assert.Error(t, err)
}

func TestEncoder_ResetAfterError(t *testing.T) {
	defer ConfigTeardown()

	enc, _ := avro.NewEncoder("boolean", errorWriter{})
	require.Error(t, enc.Encode(true))

	var buf bytes.Buffer
	enc.Reset(&buf)

	require.NoError(t, enc.Encode(false))
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestMarshal(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("boolean")

	b, err := avro.Marshal(schema, true)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x01}, b)
}

func TestMarshal_Error(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("int")

	_, err := avro.Marshal(schema, true)

	assert.Error(t, err)
}

func TestEncoder_NilSchemaReturnsError(t *testing.T) {
	var typedNil *avro.PrimitiveSchema
	tests := []struct {
		name   string
		schema avro.Schema
	}{
		{name: "nil interface"},
		{name: "typed nil", schema: typedNil},
	}

	for _, test := range tests {
		t.Run(test.name+"/stream", func(t *testing.T) {
			enc := avro.NewEncoderForSchema(test.schema, bytes.NewBuffer(nil))
			var err error

			assert.NotPanics(t, func() {
				err = enc.Encode(true)
			})
			assert.Error(t, err)
		})

		t.Run(test.name+"/marshal", func(t *testing.T) {
			var err error

			assert.NotPanics(t, func() {
				_, err = avro.Marshal(test.schema, true)
			})
			assert.Error(t, err)
		})
	}
}
