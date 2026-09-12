package avro_test

import (
	"encoding/json"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/require"
)

func TestParse_RecordAliasNamespace(t *testing.T) {
	fields := []string{
		`{"name":"a","type":"int","aliases":["old","old"]}`,
		`{"name":"a","type":"int","aliases":["a"]}`,
		`{"name":"a","type":"int","aliases":["shared"]},{"name":"b","type":"int","aliases":["shared"]}`,
		`{"name":"a","type":"int","aliases":["b"]},{"name":"b","type":"int"}`,
		`{"name":"b","type":"int"},{"name":"a","type":"int","aliases":["b"]}`,
	}
	for _, typ := range []string{"record", "error"} {
		for _, fields := range fields {
			cache := &avro.SchemaCache{}
			_, err := avro.ParseWithCache(`{"type":"`+typ+`","name":"R","fields":[`+fields+`]}`, "", cache)
			require.Error(t, err, "%s: %s", typ, fields)
			_, err = avro.ParseWithCache(`"R"`, "", cache)
			require.Error(t, err, "invalid record leaked into the schema cache")
		}
	}

	_, err := avro.ParseWithCache(`{"type":"record","name":"R","fields":[{"name":"value","type":"int","aliases":["old-name",""]},{"name":"next","type":["null","R"]}]}`, "", nil)
	require.NoError(t, err, "arbitrary alias text and recursive records remain valid")
}

func TestExactNumericProperties(t *testing.T) {
	for _, number := range []string{"9007199254740993", "1e400", "1e-400", "0.10000000000000001"} {
		schema, err := avro.Parse(`{"type":"long","n":` + number + `,"nested":{"values":[` + number + `]}}`)
		require.NoError(t, err, number)
		require.Equal(t, json.Number(number), schema.(*avro.PrimitiveSchema).Prop("n"))
		require.Equal(t, json.Number(number), schema.(*avro.PrimitiveSchema).Prop("nested").(map[string]any)["values"].([]any)[0])
		data, err := json.Marshal(schema)
		require.NoError(t, err)
		var raw map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data, &raw))
		require.Equal(t, number, string(raw["n"]))
	}
	for _, number := range []string{"2", "2.0", "2e0", "0.1", "-0", "9007199254740992"} {
		schema, err := avro.Parse(`{"type":"long","n":` + number + `}`)
		require.NoError(t, err)
		value, err := json.Number(number).Float64()
		require.NoError(t, err)
		require.Equal(t, value, schema.(*avro.PrimitiveSchema).Prop("n"))
	}
}
