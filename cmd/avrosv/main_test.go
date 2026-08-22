package main

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
)

func TestAvroSv_RequiredFlags(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantExitCode  int
		wantErrorPart string
	}{
		{
			name:          "validates no schema is set",
			args:          []string{"avrosv"},
			wantExitCode:  1,
			wantErrorPart: "Error: at least one schema is required",
		},
		{
			name:          "validates single schema is set",
			args:          []string{"avrosv", "some/file"},
			wantExitCode:  2,
			wantErrorPart: "Error: open some/file",
		},
		{
			name:          "validates multiple schemas are set",
			args:          []string{"avrosv", "some/file", "some/other"},
			wantExitCode:  2,
			wantErrorPart: "Error: open some/file",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var stderr bytes.Buffer
			got := realMain(test.args, io.Discard, &stderr)

			assert.Equal(t, test.wantExitCode, got)
			assert.Contains(t, stderr.String(), test.wantErrorPart)
		})
	}
}

func TestAvroSv_ValidatesSchema(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantExitCode  int
		wantErrorPart string
	}{
		{
			name:         "validates a simple schema",
			args:         []string{"avrosv", "testdata/schema.avsc"},
			wantExitCode: 0,
		},
		{
			name:          "does not validate a bad schema",
			args:          []string{"avrosv", "testdata/bad-schema.avsc"},
			wantExitCode:  2,
			wantErrorPart: "Error: avro: unknown type: map[]",
		},
		{
			name:          "does not validate a schema with a bad default",
			args:          []string{"avrosv", "testdata/bad-default-schema.avsc"},
			wantExitCode:  2,
			wantErrorPart: "Error: avro: invalid default for field someString. <nil> not a string",
		},
		{
			name:          "does not validate a schema with a reference to a missing schema",
			args:          []string{"avrosv", "testdata/withref-schema.avsc"},
			wantExitCode:  2,
			wantErrorPart: "Error: avro: unknown type: test",
		},
		{
			name:         "validates a schema with a reference to an existing schema",
			args:         []string{"avrosv", "testdata/schema.avsc", "testdata/withref-schema.avsc"},
			wantExitCode: 0,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			avroSvResetSchemaCache(t)
			var stderr bytes.Buffer
			got := realMain(test.args, io.Discard, &stderr)

			assert.Equal(t, test.wantExitCode, got)
			if test.wantErrorPart == "" {
				assert.Empty(t, stderr.String())
			} else {
				assert.Contains(t, stderr.String(), test.wantErrorPart)
			}
		})
	}
}

func TestAvroSv_Verbose(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantStdout    string
		wantExitCode  int
		wantErrorPart string
	}{
		{
			name:         "dumps a simple schema",
			args:         []string{"avrosv", "-v", "testdata/schema.avsc"},
			wantStdout:   "{\"name\":\"test\",\"type\":\"record\",\"fields\":[{\"name\":\"someString\",\"type\":\"string\"}]}\n",
			wantExitCode: 0,
		},
		{
			name:         "dumps a schema with a reference to an existing schema",
			args:         []string{"avrosv", "-v", "testdata/schema.avsc", "testdata/withref-schema.avsc"},
			wantStdout:   "{\"name\":\"testref\",\"type\":\"record\",\"fields\":[{\"name\":\"someref\",\"type\":{\"name\":\"test\",\"type\":\"record\",\"fields\":[{\"name\":\"someString\",\"type\":\"string\"}]}}]}\n",
			wantExitCode: 0,
		},
		{
			name:          "does not dump any schema when the schema file is invalid",
			args:          []string{"avrosv", "-v", "testdata/bad-schema.avsc"},
			wantStdout:    "",
			wantExitCode:  2,
			wantErrorPart: "Error: avro: unknown type: map[]",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			avroSvResetSchemaCache(t)
			var stdout bytes.Buffer
			var stderr bytes.Buffer

			got := realMain(test.args, &stdout, &stderr)

			assert.Equal(t, test.wantStdout, stdout.String())
			assert.Equal(t, test.wantExitCode, got)
			if test.wantErrorPart == "" {
				assert.Empty(t, stderr.String())
			} else {
				assert.Contains(t, stderr.String(), test.wantErrorPart)
			}
		})
	}
}

type avroSvErrorWriter struct{}

func (avroSvErrorWriter) Write([]byte) (int, error) {
	return 0, errors.New("unit write failure")
}

func TestAvroSv_VerboseWriteError(t *testing.T) {
	avroSvResetSchemaCache(t)
	var stderr bytes.Buffer

	got := realMain([]string{"avrosv", "-v", "testdata/schema.avsc"}, avroSvErrorWriter{}, &stderr)

	assert.Equal(t, 3, got)
	assert.Contains(t, stderr.String(), "Error: could not write schema: unit write failure")
}

func TestAvroSv_SchemaCacheIsolation(t *testing.T) {
	originalCache := avro.DefaultSchemaCache
	t.Run("isolated invocation", func(t *testing.T) {
		avroSvResetSchemaCache(t)
		assert.NotSame(t, originalCache, avro.DefaultSchemaCache)
		assert.Equal(t, 0, realMain([]string{"avrosv", "testdata/schema.avsc"}, io.Discard, io.Discard))
	})
	assert.Same(t, originalCache, avro.DefaultSchemaCache)
}

func avroSvResetSchemaCache(t *testing.T) {
	t.Helper()
	originalCache := avro.DefaultSchemaCache
	avro.DefaultSchemaCache = &avro.SchemaCache{}
	t.Cleanup(func() { avro.DefaultSchemaCache = originalCache })
}
