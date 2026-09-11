package avro_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaResolutionGraph(t *testing.T) {
	for _, container := range []string{"array", "map"} {
		t.Run(container, func(t *testing.T) {
			childType := `{"type":"array","items":"GraphNode"}`
			var children any = []any{map[string]any{"value": 2, "children": []any{}}}
			var wantChildren any = []any{map[string]any{"value": int64(2), "children": []any{}, "added": "default"}}
			if container == "map" {
				childType = `{"type":"map","values":"GraphNode"}`
				children = map[string]any{"child": map[string]any{"value": 2, "children": map[string]any{}}}
				wantChildren = map[string]any{"child": map[string]any{"value": int64(2), "children": map[string]any{}, "added": "default"}}
			}
			writer := avro.MustParse(`{"type":"record","name":"GraphNode","fields":[{"name":"value","type":"int"},{"name":"children","type":` + childType + `}]}`)
			reader := avro.MustParse(`{"type":"record","name":"GraphNode","fields":[{"name":"value","type":"long"},{"name":"children","type":` + childType + `},{"name":"added","type":"string","default":"default"}]}`)
			data, err := avro.Marshal(writer, map[string]any{"value": 1, "children": children})
			require.NoError(t, err)
			resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
			require.NoError(t, err)
			var got any
			require.NoError(t, avro.Unmarshal(resolved, data, &got))
			require.Equal(t, map[string]any{"value": int64(1), "children": wantChildren, "added": "default"}, got)
		})
	}

	t.Run("shared record", func(t *testing.T) {
		writer := avro.MustParse(`{"type":"record","name":"SharedRoot","fields":[{"name":"first","type":{"type":"record","name":"SharedLeaf","fields":[{"name":"value","type":"int"}]}},{"name":"second","type":"SharedLeaf"}]}`)
		reader := avro.MustParse(`{"type":"record","name":"SharedRoot","fields":[{"name":"first","type":{"type":"record","name":"SharedLeaf","fields":[{"name":"value","type":"long"},{"name":"added","type":"int","default":3}]}},{"name":"second","type":"SharedLeaf"}]}`)
		data, err := avro.Marshal(writer, map[string]any{"first": map[string]any{"value": 1}, "second": map[string]any{"value": 2}})
		require.NoError(t, err)
		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		require.NoError(t, err)
		var got any
		require.NoError(t, avro.Unmarshal(resolved, data, &got))
		require.Equal(t, map[string]any{"first": map[string]any{"value": int64(1), "added": 3}, "second": map[string]any{"value": int64(2), "added": 3}}, got)
	})

	t.Run("direct reference", func(t *testing.T) {
		schema := avro.MustParse(`{"type":"record","name":"DirectNode","fields":[{"name":"next","type":"DirectNode"}]}`)
		resolved, err := avro.NewSchemaCompatibility().Resolve(schema, schema)
		require.NoError(t, err)
		require.Equal(t, schema.String(), resolved.String())
	})
}

func TestSchemaResolutionGraphCache(t *testing.T) {
	writer := avro.MustParse(`{"type":"record","name":"GraphA","fields":[{"name":"value","type":"int"},{"name":"b","type":{"type":"record","name":"GraphB","fields":[{"name":"back","type":["null","GraphA"]}]}}]}`)
	data, err := avro.Marshal(writer, map[string]any{"value": 1, "b": map[string]any{"back": map[string]any{"GraphA": map[string]any{"value": 2, "b": map[string]any{"back": nil}}}}})
	require.NoError(t, err)
	compat := avro.NewSchemaCompatibility()
	var fingerprints [][32]byte
	for _, added := range []int{3, 9, 3} {
		reader := avro.MustParse(fmt.Sprintf(`{"type":"record","name":"GraphA","fields":[{"name":"value","type":"long"},{"name":"b","type":{"type":"record","name":"GraphB","fields":[{"name":"back","type":["null","GraphA"]}]}},{"name":"added","type":"int","default":%d}]}`, added))
		before, err := json.Marshal(reader)
		require.NoError(t, err)
		resolved, err := compat.Resolve(reader, writer)
		require.NoError(t, err)
		fingerprints = append(fingerprints, resolved.CacheFingerprint())
		var got any
		require.NoError(t, avro.Unmarshal(resolved, data, &got))
		require.Equal(t, map[string]any{"value": int64(1), "added": added, "b": map[string]any{"back": map[string]any{"GraphA": map[string]any{"value": int64(2), "added": added, "b": map[string]any{"back": nil}}}}}, got)
		after, err := json.Marshal(reader)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
	require.NotEqual(t, fingerprints[0], fingerprints[1])
	require.Equal(t, fingerprints[0], fingerprints[2])

	// A rejected graph must not leave incomplete records in later calls.
	bad := avro.MustParse(`{"type":"record","name":"GraphA","fields":[{"name":"value","type":"boolean"}]}`)
	resolved, err := compat.Resolve(bad, writer)
	require.Error(t, err)
	require.Nil(t, resolved)
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 8 {
				resolved, err := compat.Resolve(writer, writer)
				if err != nil {
					t.Error(err)
					return
				}
				var got any
				if err := avro.Unmarshal(resolved, data, &got); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	group.Wait()
}

type resolutionUnionValue struct{ Value any }

func (v *resolutionUnionValue) FromAny(value any) error { v.Value = value; return nil }
func (v *resolutionUnionValue) ToAny() (any, error)     { return nil, fmt.Errorf("decode fixture") }

func TestSchemaResolutionUnionBranches(t *testing.T) {
	for _, tt := range []struct {
		reader  string
		writers []string
		values  []any
		want    []any
	}{
		{`"double"`, []string{"int", "long", "float", "double"}, []any{7, int64(1 << 40), float32(1.25), 2.5}, []any{float64(7), float64(1 << 40), 1.25, 2.5}},
		{`"float"`, []string{"int", "long", "float"}, []any{7, int64(8), float32(1.25)}, []any{float32(7), float32(8), float32(1.25)}},
		{`"string"`, []string{"bytes", "string"}, []any{[]byte("first"), "second"}, []any{"first", "second"}},
	} {
		t.Run(tt.reader, func(t *testing.T) {
			encoded, err := json.Marshal(tt.writers)
			require.NoError(t, err)
			writer := avro.MustParse(string(encoded))
			reader := avro.MustParse(tt.reader)
			resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
			require.NoError(t, err)
			for i, value := range tt.values {
				data, err := avro.Marshal(writer, map[string]any{tt.writers[i]: value})
				require.NoError(t, err)
				native := reflect.New(reflect.TypeOf(tt.want[i]))
				require.NoError(t, avro.Unmarshal(resolved, data, native.Interface()))
				require.Equal(t, tt.want[i], native.Elem().Interface())
				want := map[string]any{string(reader.Type()): tt.want[i]}
				var generic any
				require.NoError(t, avro.Unmarshal(resolved, data, &generic))
				require.Equal(t, tt.want[i], generic)
				var mapped map[string]any
				require.NoError(t, avro.Unmarshal(resolved, data, &mapped))
				require.Equal(t, want, mapped)
				r := avro.NewReader(bytes.NewReader(data), 32)
				require.Equal(t, want, r.ReadNext(resolved))
				require.NoError(t, r.Error)
				var converted resolutionUnionValue
				require.NoError(t, avro.Unmarshal(resolved, data, &converted))
				require.Equal(t, tt.want[i], converted.Value)
			}
		})
	}
}

func TestSchemaResolutionUnionNullable(t *testing.T) {
	reader := avro.MustParse(`["null","long"]`)
	for _, source := range []string{`["int","null","long"]`, `["long","int","null"]`} {
		writer := avro.MustParse(source)
		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		require.NoError(t, err)
		for _, value := range []map[string]any{{"int": 7}, {"long": int64(1 << 40)}, nil} {
			data, err := avro.Marshal(writer, value)
			require.NoError(t, err)
			got := new(int64)
			*got = 99
			require.NoError(t, avro.Unmarshal(resolved, data, &got))
			if value == nil {
				require.Nil(t, got)
				continue
			}
			want := int64(7)
			if v, ok := value["long"].(int64); ok {
				want = v
			}
			require.Equal(t, want, *got)
		}
	}
}

func TestSchemaResolutionUnionRecords(t *testing.T) {
	reader := avro.MustParse(`{"type":"record","name":"UnionReader","aliases":["WriterA","WriterB"],"fields":[{"name":"value","type":"double"},{"name":"added","type":"int","default":5}]}`)
	writer := avro.MustParse(`[{"type":"record","name":"WriterA","fields":[{"name":"value","type":"int"}]},{"type":"record","name":"WriterB","fields":[{"name":"value","type":"float"}]}]`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)
	for _, name := range []string{"WriterA", "WriterB"} {
		var value any = 7
		if name == "WriterB" {
			value = float32(7)
		}
		data, err := avro.Marshal(writer, map[string]any{name: map[string]any{"value": value}})
		require.NoError(t, err)
		var got struct {
			Value float64 `avro:"value"`
			Added int     `avro:"added"`
		}
		require.NoError(t, avro.Unmarshal(resolved, data, &got))
		require.Equal(t, float64(7), got.Value)
		require.Equal(t, 5, got.Added)
		var generic any
		require.NoError(t, avro.Unmarshal(resolved, data, &generic))
		require.Equal(t, map[string]any{"UnionReader": map[string]any{"value": float64(7), "added": 5}}, generic)
	}
}

func TestSchemaResolutionNestedUnions(t *testing.T) {
	reader := avro.MustParse(`{"type":"record","name":"NestedUnions","fields":[{"name":"list","type":{"type":"array","items":["null","double"]}},{"name":"lookup","type":{"type":"map","values":"long"}}]}`)
	writer := avro.MustParse(`{"type":"record","name":"NestedUnions","fields":[{"name":"list","type":{"type":"array","items":["int","null","float"]}},{"name":"lookup","type":{"type":"map","values":["int","long"]}}]}`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)
	data, err := avro.Marshal(writer, map[string]any{
		"list":   []any{map[string]any{"int": 7}, nil, map[string]any{"float": float32(1.25)}},
		"lookup": map[string]any{"a": map[string]any{"int": 9}, "b": map[string]any{"long": int64(1 << 40)}},
	})
	require.NoError(t, err)
	var got struct {
		List   []*float64       `avro:"list"`
		Lookup map[string]int64 `avro:"lookup"`
	}
	require.NoError(t, avro.Unmarshal(resolved, data, &got))
	require.Len(t, got.List, 3)
	require.Equal(t, float64(7), *got.List[0])
	require.Nil(t, got.List[1])
	require.Equal(t, 1.25, *got.List[2])
	require.Equal(t, map[string]int64{"a": 9, "b": 1 << 40}, got.Lookup)
	// Omitting the collection still consumes every original branch correctly.
	var onlyLookup struct {
		Lookup map[string]int64 `avro:"lookup"`
	}
	require.NoError(t, avro.Unmarshal(resolved, data, &onlyLookup))
	require.Equal(t, got.Lookup, onlyLookup.Lookup)
}

func TestSchemaResolutionUnionPublicSchema(t *testing.T) {
	reader := avro.MustParse(`["long","null"]`)
	writer := avro.MustParse(`["null","int","long"]`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)
	union := resolved.(*avro.UnionSchema)
	require.Len(t, union.Types(), 2)
	require.Equal(t, reader.String(), resolved.String())
	require.Equal(t, reader.Fingerprint(), resolved.Fingerprint())
	require.NotEqual(t, reader.CacheFingerprint(), resolved.CacheFingerprint())
	encoded, err := json.Marshal(resolved)
	require.NoError(t, err)
	reparsed, err := avro.Parse(string(encoded))
	require.NoError(t, err)
	require.Equal(t, reader.Fingerprint(), reparsed.Fingerprint())
	for _, value := range []any{nil, int64(7), map[string]any{"long": int64(7)}, &resolutionUnionValue{}} {
		_, err := avro.Marshal(resolved, value)
		require.ErrorIs(t, err, avro.ErrResolvedSchemaEncoding)
	}
	_, err = avro.NewUnionSchema([]avro.Schema{reader.(*avro.UnionSchema).Types()[0], reader.(*avro.UnionSchema).Types()[0]})
	require.Error(t, err)
}

func TestSchemaResolutionSkipPlans(t *testing.T) {
	for _, tt := range []struct {
		reader, writer string
		value          any
	}{
		{`{"type":"record","name":"SkipValue","fields":[{"name":"v","type":"long"},{"name":"added","type":"int","default":3}]}`, `{"type":"record","name":"SkipValue","fields":[{"name":"v","type":"int"}]}`, map[string]any{"v": 7}},
		{`{"type":"enum","name":"SkipEnum","symbols":["A"],"default":"A"}`, `{"type":"enum","name":"SkipEnum","symbols":["A","B","C"]}`, "C"},
	} {
		writer := avro.MustParse(`{"type":"record","name":"SkipRoot","fields":[{"name":"skip","type":` + tt.writer + `},{"name":"tail","type":"int"}]}`)
		reader := avro.MustParse(`{"type":"record","name":"SkipRoot","fields":[{"name":"skip","type":` + tt.reader + `},{"name":"tail","type":"int"}]}`)
		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		require.NoError(t, err)
		data, err := avro.Marshal(writer, map[string]any{"skip": tt.value, "tail": 41})
		require.NoError(t, err)
		var got struct {
			Tail int `avro:"tail"`
		}
		err = avro.Unmarshal(resolved, data, &got)
		if !assert.NoError(t, err) {
			continue
		}
		require.Equal(t, 41, got.Tail)
	}
}
