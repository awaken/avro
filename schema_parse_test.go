package avro_test

import (
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
