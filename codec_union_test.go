package avro_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/require"
)

type auditUnionFlag struct {
	Flag bool `json:"Flag" yaml:"Flag" xml:"Flag"`
}

type auditUnionConversion struct {
	value any
	err   error
}

func (c *auditUnionConversion) ToAny() (any, error) { return c.value, c.err }
func (*auditUnionConversion) FromAny(any) error     { return errors.New("conversion refused") }

func TestAuditUnionErrorPaths(t *testing.T) {
	schema := avro.MustParse(`["int","string"]`)
	var slice []int
	require.Error(t, avro.Unmarshal(schema, []byte{0, 14}, &slice))
	var object map[string]any
	require.Error(t, avro.Unmarshal(schema, []byte{2, 4, 'x'}, &object))
	require.Empty(t, object, "a truncated branch must not be published")

	for _, value := range []*auditUnionConversion{
		nil, {err: errors.New("conversion refused")}, {value: 7},
	} {
		_, err := avro.Marshal(schema, value)
		require.Error(t, err)
	}
	var custom auditUnionConversion
	require.ErrorContains(t, avro.Unmarshal(schema, []byte{0, 14}, &custom), "conversion refused")
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType:              avro.Int,
		DecoderTypeConversion: func(any, avro.Schema) (any, error) { return nil, nil },
	})
	require.Error(t, api.Unmarshal(schema, []byte{0, 14}, &custom), "non-nullable conversion cannot produce nil")

	array := avro.MustParse(`["null",{"type":"array","items":{"type":"record","name":"EmptyArrayItem","fields":[]}}]`)
	wire, err := avro.Marshal(array, map[string]any{"array": nil})
	require.NoError(t, err)
	require.Equal(t, []byte{2, 0}, wire)
	values := []struct{}{{}}
	require.NoError(t, avro.Unmarshal(array, wire, &values))
	require.Empty(t, values, "reuse must replace the prior array")
	api.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType:              avro.Array,
		EncoderTypeConversion: func(any, avro.Schema) (any, error) { return nil, nil },
	})
	wire, err = api.Marshal(array, map[string]any{"array": []struct{}{{}}})
	require.NoError(t, err)
	require.Equal(t, []byte{2, 0}, wire, "an explicitly empty conversion must encode an empty array")
}

func TestAuditResolvedUnionNull(t *testing.T) {
	reader := avro.MustParse(`["null","long","string"]`)
	writer := avro.MustParse(`["null","int","string"]`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)
	var ptr *int64
	require.NoError(t, avro.Unmarshal(resolved, []byte{2, 14}, &ptr))
	require.NotNil(t, ptr)
	require.Equal(t, int64(7), *ptr)
	require.NoError(t, avro.Unmarshal(resolved, []byte{0}, &ptr))
	require.Nil(t, ptr, "null must clear an existing pointer")
	value := int64(7)
	require.Error(t, avro.Unmarshal(resolved, []byte{0}, &value))
	require.Equal(t, int64(7), value, "unrepresentable null must preserve the value")
}

// Keep the suspected undersized pointer allocation in a disposable child.
func TestAuditUnionPointerStorage(t *testing.T) {
	if os.Getenv("FLOWER_AUDIT_AVRO_POINTER") == "1" {
		schema := avro.MustParse(`[{"type":"record","name":"AuditFlag","fields":[{"name":"Flag","type":"boolean"}]},"null"]`)
		api := avro.Config{}.Freeze()
		api.Register("AuditFlag", auditUnionFlag{})
		var got any
		if err := api.Unmarshal(schema, []byte{0, 0}, &got); err != nil || got != (auditUnionFlag{}) {
			t.Fatalf("value registration control: %v, %v", got, err)
		}
		api.Register("AuditFlag", (*auditUnionFlag)(nil))
		wire := make([]byte, 64) // Keep input outside the tiny allocation class.
		for i := range 64 {
			if err := api.Unmarshal(schema, wire[:2], &got); err != nil {
				t.Fatal(err)
			}
			value, ok := got.(*auditUnionFlag)
			if !ok || value == nil || value.Flag {
				t.Fatalf("pointer registration corrupted false flag at datum %d: %#v", i, got)
			}
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAuditUnionPointerStorage$")
	cmd.Env = append(os.Environ(), "FLOWER_AUDIT_AVRO_POINTER=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated pointer decode: %v; %s", err, out[:min(len(out), 1000)])
	}
}

func TestNullUnionRejectsPayload(t *testing.T) {
	schema := avro.MustParse(`["null","string"]`)
	for _, value := range []any{"lost", 42, []byte{1}} {
		writer := avro.NewWriter(nil, 0)
		writer.WriteVal(schema, map[string]any{"null": value})
		require.Error(t, writer.Error)
		require.Empty(t, writer.Buffer(), "invalid branch must not write its index")
	}
	data, err := avro.Marshal(schema, map[string]any{"null": nil})
	require.NoError(t, err)
	require.Equal(t, []byte{0}, data)
}

func TestNullUnionAllowsExplicitConversion(t *testing.T) {
	for _, kind := range []avro.Type{avro.Union, avro.Null} {
		api := avro.Config{}.Freeze()
		api.RegisterTypeConverters(avro.TypeConversionFuncs{
			AvroType:              kind,
			EncoderTypeConversion: func(in any, schema avro.Schema) (any, error) { return nil, nil },
		})
		data, err := api.Marshal(avro.MustParse(`["null","string"]`), map[string]any{"null": "converted"})
		require.NoError(t, err)
		require.Equal(t, []byte{0}, data)
	}
}
