package avro

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Cloning must preserve shared schema identity without sharing mutable metadata.
func TestAuditCloneSharedSchemas(t *testing.T) {
	for _, source := range []string{
		`"int"`, `"null"`,
		`{"type":"enum","name":"E","symbols":["A"]}`,
		`{"type":"fixed","name":"F","size":1}`,
		`{"type":"array","items":"int"}`,
		`{"type":"map","values":"int"}`,
		`["int","null"]`,
		`{"type":"record","name":"R","fields":[]}`,
	} {
		t.Run(source, func(t *testing.T) {
			child := MustParse(source)
			first, err := NewField("first", child)
			require.NoError(t, err)
			second, err := NewField("second", child)
			require.NoError(t, err)
			parent, err := NewRecordSchema("Parent", "", []*Field{first, second})
			require.NoError(t, err)
			copy := cloneSchemaGraph(parent).(*RecordSchema)
			require.NotSame(t, parent, copy)
			require.NotSame(t, child, copy.fields[0].typ)
			require.Same(t, copy.fields[0].typ, copy.fields[1].typ)
			require.Equal(t, parent.String(), copy.String())
			require.NoError(t, NewSchemaCompatibility().Compatible(parent, copy))
		})
	}
	field, err := NewField("value", NewPrimitiveSchema(Int, nil), WithDefault(7))
	require.NoError(t, err)
	field.encodedDef.Store([]byte{14})
	copy := cloneField(field, map[Schema]Schema{})
	copy.encodedDef.Load().([]byte)[0] = 2
	require.Equal(t, []byte{14}, field.encodedDef.Load())
	value := [32]byte{1}
	fingerprint := cloneCacheFingerprinter(cacheFingerprinter{writerFingerprint: &value})
	(*fingerprint.writerFingerprint)[0] = 2
	require.Equal(t, byte(1), value[0], "clone must not mutate the writer fingerprint")
}
