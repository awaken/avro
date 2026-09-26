package avro_test

import (
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/require"
)

type auditTextKey struct{ text string }

func (k *auditTextKey) UnmarshalText(b []byte) error {
	k.text = string(b)
	return nil
}

// Custom text keys must retain normal map limits and error propagation.
func TestAuditTextMapFailures(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":"int"}`)
	for _, data := range [][]byte{
		{4},          // Two entries exceed the configured limit.
		{2, 2},       // Truncated key.
		{2, 0, 0x80}, // Truncated value.
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2}, // Invalid count.
	} {
		var got map[*auditTextKey]int
		err := avro.Config{MaxMapAllocSize: 1}.Freeze().Unmarshal(schema, data, &got)
		require.Error(t, err)
		require.Empty(t, got, "failed entries must not be inserted")
	}
	var got map[*auditTextKey]int
	require.NoError(t, avro.Config{MaxMapAllocSize: -1}.Freeze().Unmarshal(schema, []byte{2, 0, 2, 0}, &got))
	require.Len(t, got, 1)
}

func TestAuditMapKeyUTF8(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":"int"}`)
	for _, key := range []string{"", "valid é", string([]byte{0xff})} {
		writer := avro.NewWriter(nil, 0)
		writer.WriteLong(1)
		writer.WriteBytes([]byte(key))
		writer.WriteInt(42)
		writer.WriteLong(0)
		var got map[*auditTextKey]int32
		err := avro.Unmarshal(schema, writer.Buffer(), &got)
		if key == string([]byte{0xff}) {
			if err == nil || len(got) != 0 {
				t.Errorf("invalid UTF-8 map key accepted: entries=%d, error=%v", len(got), err)
			}
			continue
		}
		if err != nil || len(got) != 1 {
			t.Fatalf("valid map key: %v, %v", got, err)
		}
		for k, value := range got {
			if k.text != key || value != 42 {
				t.Errorf("map entry = %q:%d, want %q:42", k.text, value, key)
			}
		}
	}
}
