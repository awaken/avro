package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/awaken/avro/v2/gen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "Update golden files")

func TestAvroGen_RequiredFlags(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantExitCode  int
		wantErrorPart string
	}{
		{
			name:          "validates schema is set",
			args:          []string{"avrogen", "-pkg", "test", "-o", "some/file"},
			wantExitCode:  1,
			wantErrorPart: "Error: at least one schema is required",
		},
		{
			name:          "validates schema exists",
			args:          []string{"avrogen", "-pkg", "test", "-o", "some/file", "some/schema"},
			wantExitCode:  2,
			wantErrorPart: "open some/schema",
		},
		{
			name:          "validates package is set",
			args:          []string{"avrogen", "-o", "some/file", "schema.avsc"},
			wantExitCode:  1,
			wantErrorPart: "Error: a package is required",
		},
		{
			name:          "validates tag format are valid",
			args:          []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", "snake", "schema.avsc"},
			wantExitCode:  1,
			wantErrorPart: `"snake" is not a valid tag, should be in the format "tag:style"`,
		},
		{
			name:          "validates tag key are valid",
			args:          []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", ":snake", "schema.avsc"},
			wantExitCode:  1,
			wantErrorPart: `tag name is required in ":snake"`,
		},
		{
			name:          "validates tag style are valid",
			args:          []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", "json:something", "schema.avsc"},
			wantExitCode:  1,
			wantErrorPart: `style "something" is invalid in "json:something"`,
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

func TestAvroGen_GeneratesSchemaStdout(t *testing.T) {
	var buf bytes.Buffer

	args := []string{"avrogen", "-pkg", "testpkg", "-pkgdoc", "package testpkg is generated from schema.avsc", "testdata/schema.avsc"}
	gotCode := realMain(args, &buf, io.Discard)
	require.Equal(t, 0, gotCode)

	want, err := os.ReadFile("testdata/golden.go")
	require.NoError(t, err)
	assert.Equal(t, want, buf.Bytes())
}

func TestAvroGen_GeneratesSchema(t *testing.T) {
	file := avroGenTestOutputFile(t)
	args := []string{"avrogen", "-o", file, "-pkg", "testpkg", "-pkgdoc", "package testpkg is generated from schema.avsc", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithFullname(t *testing.T) {
	file := avroGenTestOutputFile(t)
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-fullname", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_fullname.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_fullname.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithEncoders(t *testing.T) {
	file := avroGenTestOutputFile(t)
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-encoders", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_encoders.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_encoders.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithFullSchema(t *testing.T) {
	file := avroGenTestOutputFile(t)
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-encoders", "-fullschema", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_encoders_fullschema.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_encoders_fullschema.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithStrictTypes(t *testing.T) {
	file := avroGenTestOutputFile(t)
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-strict-types", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_stricttypes.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_stricttypes.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithCustomLogicalTypes(t *testing.T) {
	file := avroGenTestOutputFile(t)
	args := []string{
		"avrogen", "-pkg", "testpkg", "-o", file,
		// test mapping to an external package type
		"-logicaltype", "uuid,uuid.UUID,github.com/google/uuid",
		// test mapping to a stdlib package type
		"-logicaltype", "decimal,json.RawMessage,encoding/json",
		// test mapping to a built-in type
		"-logicaltype", "date,int32",
		"testdata/schema_logicaltypes.avsc",
	}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_logicaltypes.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_logicaltypes.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithHyphenatedLogicalTypeFlag(t *testing.T) {
	var buf bytes.Buffer
	args := []string{
		"avrogen", "-pkg", "testpkg",
		"-logical-type", "uuid,uuid.UUID,github.com/google/uuid",
		"-logical-type", "decimal,json.RawMessage,encoding/json",
		"-logical-type", "date,int32",
		"testdata/schema_logicaltypes.avsc",
	}
	gotCode := realMain(args, &buf, io.Discard)
	require.Equal(t, 0, gotCode)

	want, err := os.ReadFile("testdata/golden_logicaltypes.go")
	require.NoError(t, err)
	assert.Equal(t, want, buf.Bytes())
}

func TestAvroGen_GeneratesDependentSchemasWithExplicitCache(t *testing.T) {
	var buf bytes.Buffer
	args := []string{
		"avrogen", "-pkg", "testpkg",
		"../../gen/testdata/uniontype.avsc",
		"../../gen/testdata/main.avsc",
	}

	gotCode := realMain(args, &buf, io.Discard)

	require.Equal(t, 0, gotCode)
	assert.Contains(t, buf.String(), "type TestUnionType struct")
	assert.Contains(t, buf.String(), "type TestMain struct")
}

func TestParseTags(t *testing.T) {
	tests := []struct {
		name string
		tags string
		want map[string]gen.TagStyle
	}{
		{
			name: "snake case",
			tags: "json:snake",
			want: map[string]gen.TagStyle{"json": gen.Snake},
		},
		{
			name: "camel case",
			tags: "json:camel",
			want: map[string]gen.TagStyle{"json": gen.Camel},
		},
		{
			name: "upper camel case",
			tags: "json:upper-camel",
			want: map[string]gen.TagStyle{"json": gen.UpperCamel},
		},
		{
			name: "kebab case",
			tags: "json:kebab",
			want: map[string]gen.TagStyle{"json": gen.Kebab},
		},
		{
			name: "original case",
			tags: "json:original",
			want: map[string]gen.TagStyle{"json": gen.Original},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := parseTags(test.tags)

			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestParseInitialisms(t *testing.T) {
	tests := []struct {
		name        string
		initialisms string
		want        []string
		wantErr     string
	}{
		{
			name:        "single initialism",
			initialisms: "ABC",
			want:        []string{"ABC"},
		},
		{
			name:        "multiple initialisms",
			initialisms: "ABC,DEF",
			want:        []string{"ABC", "DEF"},
		},
		{
			name:        "surrounding whitespace",
			initialisms: " ABC, DEF ",
			want:        []string{"ABC", "DEF"},
		},
		{
			name:        "wrong initialism",
			initialisms: "ABC,def,GHI",
			wantErr:     `initialism "def" must be fully in upper case`,
		},
		{
			name:        "empty initialism",
			initialisms: "ABC,",
			wantErr:     "initialism cannot be empty",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := parseInitialisms(test.initialisms)

			if test.wantErr != "" {
				require.EqualError(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func avroGenTestOutputFile(t *testing.T) string {
	t.Helper()
	tempRoot, err := filepath.Abs("../../../../../tmp")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(tempRoot, 0o700))
	tempDir, err := os.MkdirTemp(tempRoot, "avrogen-test-")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Errorf("remove avrogen test directory: %v", err)
		}
	})
	return filepath.Join(tempDir, "test.go")
}
