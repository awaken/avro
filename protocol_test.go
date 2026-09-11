package avro_test

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMustParseProtocol(t *testing.T) {
	proto := avro.MustParseProtocol(`{"protocol":"test", "namespace": "org.hamba.avro", "doc": "docs"}`)

	assert.IsType(t, &avro.Protocol{}, proto)
}

func TestMustParseProtocol_PanicsOnError(t *testing.T) {
	assert.Panics(t, func() {
		avro.MustParseProtocol("123")
	})
}

func TestNewProtocol_ValidatesName(t *testing.T) {
	_, err := avro.NewProtocol("0test", "", nil, nil)

	assert.Error(t, err)
}

func TestNewMessage(t *testing.T) {
	field, _ := avro.NewField("test", avro.NewPrimitiveSchema(avro.String, nil))
	fields := []*avro.Field{field}
	req, _ := avro.NewRecordSchema("test", "", fields)
	resp := avro.NewPrimitiveSchema(avro.String, nil)
	types := []avro.Schema{avro.NewPrimitiveSchema(avro.String, nil)}
	errs, _ := avro.NewUnionSchema(types)

	msg := avro.NewMessage(req, resp, errs, false)

	assert.Equal(t, req, msg.Request())
	assert.Equal(t, resp, msg.Response())
	assert.Equal(t, errs, msg.Errors())
	assert.False(t, msg.OneWay())
}

func TestParseProtocol(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "Valid",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "doc": "docs"}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"protocol":"test", "namespace": ""}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Invalid Json",
			schema:  `{`,
			wantErr: assert.Error,
		},
		{
			name:    "Invalid Name First Char",
			schema:  `{"protocol":"0test", "namespace": "org.hamba.avro"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Invalid Name Other Char",
			schema:  `{"protocol":"test+", "namespace": "org.hamba.avro"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"protocol":"", "namespace": "org.hamba.avro"}`,
			wantErr: assert.Error,
		},
		{
			name:    "No Name",
			schema:  `{"namespace": "org.hamba.avro"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro+"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Invalid Type Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "types":["test"]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Type Not Named Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "types":["string"]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Not Object",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":["test"]}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Request Invalid Request Json",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": "test"}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Request Invalid Field",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar"}]}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Response Invalid Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "response": "test"}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Errors Invalid Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": ["test"]}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Errors Record Not Error Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": [{"type":"record", "name":"test", "fields":[{"name": "field", "type": "int"}]}]}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Errors Duplicate Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": ["string"]}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message One Way Invalid",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": ["int"], "one-way": true}}}`,
			wantErr: assert.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := avro.ParseProtocol(test.schema)

			test.wantErr(t, err)
		})
	}
}

func TestParseProtocol_ExplicitOneWayMessage(t *testing.T) {
	schema := `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "response":"null", "one-way":true}}}`

	proto, err := avro.ParseProtocol(schema)

	require.NoError(t, err)

	msg := proto.Message("test")
	require.NotNil(t, msg)
	assert.True(t, msg.OneWay())
}

func TestParseProtocol_PreservesLargeLongDefault(t *testing.T) {
	protocol, err := avro.ParseProtocol(`{"protocol":"test","messages":{"m":{"request":[{"name":"v","type":"long","default":9007199254740993}],"response":"null"}}}`)
	require.NoError(t, err)

	assert.Equal(t, int64(9007199254740993), protocol.Message("m").Request().Fields()[0].Default())
}

func TestParseProtocol_Docs(t *testing.T) {
	schema := `{"protocol":"test", "doc": "foo", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "response":"null", "doc": "bar"}}}`

	proto, err := avro.ParseProtocol(schema)
	require.NoError(t, err)

	assert.Equal(t, "foo", proto.Doc())

	msg := proto.Message("test")
	require.NotNil(t, msg)
	assert.Equal(t, "bar", msg.Doc())
}

func TestParseProtocolFile(t *testing.T) {
	protocol, err := avro.ParseProtocolFile("testdata/echo.avpr")

	want := `{"protocol":"Echo","namespace":"org.hamba.avro","types":[{"name":"org.hamba.avro.Ping","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"text","type":"string"}]},{"name":"org.hamba.avro.Pong","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"ping","type":"org.hamba.avro.Ping"}]},{"name":"org.hamba.avro.PongError","type":"error","fields":[{"name":"timestamp","type":"long"},{"name":"reason","type":"string"}]}],"messages":{"ping":{"request":[{"name":"ping","type":"org.hamba.avro.Ping"}],"response":"org.hamba.avro.Pong","errors":["org.hamba.avro.PongError"]}}}`
	wantMD5 := "5bc594ae86fc8c209f553ce3bc4291a5"
	require.NoError(t, err)
	assert.Equal(t, want, protocol.String())
	assert.Equal(t, wantMD5, protocol.Hash())
}

func TestParseProtocolFile_InvalidPath(t *testing.T) {
	_, err := avro.ParseProtocolFile("test.avpr")

	assert.Error(t, err)
}

func TestParseProtocol_Types(t *testing.T) {
	protocol, err := avro.ParseProtocolFile("testdata/echo.avpr")

	wantPing := `{"name":"org.hamba.avro.Ping","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"text","type":"string"}]}`
	wantPong := `{"name":"org.hamba.avro.Pong","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"ping","type":"org.hamba.avro.Ping"}]}`
	wantPongError := `{"name":"org.hamba.avro.PongError","type":"error","fields":[{"name":"timestamp","type":"long"},{"name":"reason","type":"string"}]}`
	wantLen := 3
	require.NoError(t, err)
	assert.Equal(t, wantLen, len(protocol.Types()))
	assert.Equal(t, wantPing, protocol.Types()[0].String())
	assert.Equal(t, wantPong, protocol.Types()[1].String())
	assert.Equal(t, wantPongError, protocol.Types()[2].String())
}

func TestProtocolStableMessageOrder(t *testing.T) {
	req, err := avro.NewRecordSchema("Request", "", nil)
	require.NoError(t, err)
	msg := avro.NewMessage(req, avro.NewPrimitiveSchema(avro.String, nil), nil, false)
	want := `{"protocol":"Stable","namespace":"","types":[],"messages":{`
	for i, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		if i > 0 {
			want += ","
		}
		want += `"` + name + `":{"request":[],"response":"string"}`
	}
	want += "}}"
	hash := md5.Sum([]byte(want))

	for i := range 16 {
		messages := make(map[string]*avro.Message)
		for j := range 8 {
			messages[string(rune('a'+(i+j)%8))] = msg
		}
		proto, err := avro.NewProtocol("Stable", "", nil, messages)
		require.NoError(t, err)
		assert.Equal(t, hex.EncodeToString(hash[:]), proto.Hash())
		for range 8 {
			require.Equal(t, want, proto.String())
		}
	}
}

func TestProtocolCollectionSnapshots(t *testing.T) {
	a, err := avro.NewRecordSchema("A", "", nil)
	require.NoError(t, err)
	b, err := avro.NewRecordSchema("B", "", nil)
	require.NoError(t, err)
	msg := avro.NewMessage(a, avro.NewPrimitiveSchema(avro.String, nil), nil, false)
	types := []avro.NamedSchema{a, b}
	messages := map[string]*avro.Message{"m": msg}
	proto, err := avro.NewProtocol("Snapshot", "", types, messages)
	require.NoError(t, err)
	want, hash := proto.String(), proto.Hash()

	types[0] = b
	delete(messages, "m")
	messages["other"] = msg
	assert.Same(t, msg, proto.Message("m"))
	assert.Nil(t, proto.Message("other"))
	assert.Equal(t, []avro.NamedSchema{a, b}, proto.Types())
	returned := proto.Types()
	returned[1] = a
	assert.Equal(t, []avro.NamedSchema{a, b}, proto.Types())
	assert.Equal(t, want, proto.String())
	assert.Equal(t, hash, proto.Hash())
}

func TestProtocolOneWayRoundTrip(t *testing.T) {
	for _, oneWay := range []bool{false, true} {
		suffix := ""
		if oneWay {
			suffix = `,"one-way":true`
		}
		want := `{"protocol":"Calls","namespace":"","types":[],"messages":{"m":{"request":[],"response":"null"` + suffix + `}}}`
		proto, err := avro.ParseProtocol(want)
		require.NoError(t, err)
		assert.Equal(t, oneWay, proto.Message("m").OneWay())
		if assert.NotNil(t, proto.Message("m").Response()) {
			assert.Equal(t, avro.Null, proto.Message("m").Response().Type())
		}
		assert.Equal(t, want, proto.String())
		hash := md5.Sum([]byte(want))
		assert.Equal(t, hex.EncodeToString(hash[:]), proto.Hash())
		again, err := avro.ParseProtocol(proto.String())
		require.NoError(t, err)
		assert.Equal(t, oneWay, again.Message("m").OneWay())
		assert.Equal(t, proto.Hash(), again.Hash())
	}
}

func TestNewMessageNilResponse(t *testing.T) {
	req, err := avro.NewRecordSchema("Request", "", nil)
	require.NoError(t, err)
	msg := avro.NewMessage(req, nil, nil, true)
	assert.Equal(t, `{"request":[],"response":"null","one-way":true}`, msg.String())
	require.NotNil(t, msg.Response())
	assert.Equal(t, avro.Null, msg.Response().Type())
}

func TestProtocolDeclaredErrorTypes(t *testing.T) {
	for _, typ := range []string{
		`"int"`, `"string"`, `"null"`,
		`{"type":"record","name":"Ordinary","fields":[]}`,
		`{"type":"enum","name":"Enum","symbols":["A"]}`,
		`{"type":"fixed","name":"Fixed","size":1}`,
		`{"type":"array","items":"int"}`,
		`{"type":"map","values":"int"}`,
	} {
		t.Run(typ, func(t *testing.T) {
			_, err := avro.ParseProtocol(`{"protocol":"Errors","messages":{"m":{"request":[],"response":"null","errors":[` + typ + `]}}}`)
			assert.Error(t, err)
		})
	}
	for _, kind := range []string{"record", "error"} {
		t.Run("reference_"+kind, func(t *testing.T) {
			proto, err := avro.ParseProtocol(`{"protocol":"Errors","types":[{"type":"` + kind + `","name":"Declared","fields":[]}],"messages":{"m":{"request":[],"response":"null","errors":["Declared"]}}}`)
			if kind == "record" {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, proto.Message("m").Errors().Types(), 2)
			assert.Equal(t, avro.String, proto.Message("m").Errors().Types()[0].Type())
			_, err = avro.ParseProtocol(proto.String())
			assert.NoError(t, err)
		})
	}
	proto, err := avro.ParseProtocol(`{"protocol":"Errors","messages":{"m":{"request":[],"response":"null","errors":[{"type":"error","name":"Declared","fields":[]}]}}}`)
	require.NoError(t, err)
	assert.Len(t, proto.Message("m").Errors().Types(), 2)
}

func TestProtocolRequiredMessageFields(t *testing.T) {
	for _, body := range []string{
		`{}`, `{"request":[]}`, `{"response":"null"}`,
		`{"request":null,"response":"null"}`,
		`{"request":{},"response":"null"}`,
		`{"request":[null],"response":"null"}`,
		`{"request":[],"response":null}`,
		`{"request":[],"response":false}`,
		`{"request":[],"response":"null","one-way":null}`,
		`{"request":[],"response":"null","one-way":"true"}`,
		`{"request":[],"response":"string","one-way":true}`,
		`{"request":[],"response":"null","errors":null}`,
		`{"request":[],"response":"null","one-way":true,"errors":[{"type":"error","name":"Oops","fields":[]}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			_, err := avro.ParseProtocol(`{"protocol":"Required","messages":{"m":` + body + `}}`)
			assert.Error(t, err)
		})
	}
	for _, body := range []string{
		`{"request":[],"response":"null"}`,
		`{"request":[],"response":{"type":"null"},"one-way":false}`,
	} {
		proto, err := avro.ParseProtocol(`{"protocol":"Required","messages":{"m":` + body + `}}`)
		require.NoError(t, err)
		assert.False(t, proto.Message("m").OneWay())
	}
}

func TestProtocolMessageNames(t *testing.T) {
	req, err := avro.NewRecordSchema("Request", "", nil)
	require.NoError(t, err)
	msg := avro.NewMessage(req, avro.NewPrimitiveSchema(avro.String, nil), nil, false)
	for _, name := range []string{"", "bad-name", "0name", "name.space", "_valid2"} {
		t.Run(name, func(t *testing.T) {
			key, err := json.Marshal(name)
			require.NoError(t, err)
			_, parseErr := avro.ParseProtocol(`{"protocol":"Names","messages":{` + string(key) + `:{"request":[],"response":"string"}}}`)
			_, makeErr := avro.NewProtocol("Names", "", nil, map[string]*avro.Message{name: msg})
			if name == "_valid2" {
				assert.NoError(t, parseErr)
				assert.NoError(t, makeErr)
			} else {
				assert.Error(t, parseErr)
				assert.Error(t, makeErr)
			}
		})
	}
}

func TestProtocolPermissiveNamesStayJSON(t *testing.T) {
	old := avro.SkipNameValidation
	avro.SkipNameValidation = true
	t.Cleanup(func() { avro.SkipNameValidation = old })
	req, err := avro.NewRecordSchema("Request", "", nil)
	require.NoError(t, err)
	msg := avro.NewMessage(req, avro.NewPrimitiveSchema(avro.String, nil), nil, false)
	name, namespace, message := "P\"\n", "N\\s", "m\x01\""
	proto, err := avro.NewProtocol(name, namespace, nil, map[string]*avro.Message{message: msg})
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(proto.String()), &decoded))
	assert.Equal(t, name, decoded["protocol"])
	assert.Equal(t, namespace, decoded["namespace"])
	assert.Contains(t, decoded["messages"], message)
	assert.False(t, strings.ContainsRune(proto.String(), '\x01'))
}
