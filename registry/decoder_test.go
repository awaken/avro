package registry_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/awaken/avro/v2/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nilAPI struct {
	avro.API
}

func TestNewDecoder_NilOption(t *testing.T) {
	client, err := registry.NewClient("http://example.com")
	require.NoError(t, err)

	var decoder *registry.Decoder
	require.NotPanics(t, func() {
		decoder = registry.NewDecoder(client, nil)
	})
	require.NotNil(t, decoder)
}

func TestDecoder_DecodeNilClient(t *testing.T) {
	decoder := registry.NewDecoder(nil)

	var err error
	require.NotPanics(t, func() {
		err = decoder.Decode(context.Background(), []byte{0, 0, 0, 0, 1}, new(any))
	})
	require.ErrorContains(t, err, "registry client cannot be nil")
}

func TestDecoder_DecodeNilAPI(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"null"}`))
	}))
	t.Cleanup(srv.Close)
	client, err := registry.NewClient(srv.URL)
	require.NoError(t, err)

	tests := []struct {
		name string
		api  avro.API
	}{
		{name: "nil", api: nil},
		{name: "typed nil", api: (*nilAPI)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := registry.NewDecoder(client, registry.WithAPI(test.api))

			var decodeErr error
			require.NotPanics(t, func() {
				decodeErr = decoder.Decode(context.Background(), []byte{0, 0, 0, 0, 1}, new(any))
			})
			require.ErrorContains(t, decodeErr, "avro API cannot be nil")
		})
	}
}

func TestDecoder_Decode(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		schema  string
		want    int
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "decodes data",
			data:    []byte{0x0, 0x0, 0x0, 0x0, 0x2a, 0x80, 0x2},
			schema:  `{"schema":"int"}`,
			want:    128,
			wantErr: require.NoError,
		},
		{
			name:    "handles short data",
			data:    []byte{0x0, 0x0, 0x0, 0x0, 0x2a},
			schema:  `{"schema":"int"}`,
			wantErr: require.Error,
		},
		{
			name:    "handles bad magic",
			data:    []byte{0x1, 0x0, 0x0, 0x0, 0x2a, 0x80, 0x2},
			schema:  `{"schema":"int"}`,
			wantErr: require.Error,
		},
		{
			name:    "handles bad schema",
			data:    []byte{0x0, 0x0, 0x0, 0x0, 0x2a, 0x80, 0x2},
			schema:  `{"schema":"nope"}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			h := http.NewServeMux()
			h.Handle("/schemas/ids/42", http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				assert.Equal(t, "GET", req.Method)

				_, _ = rw.Write([]byte(test.schema))
			}))
			srv := httptest.NewServer(h)
			t.Cleanup(srv.Close)

			client, _ := registry.NewClient(srv.URL)
			decoder := registry.NewDecoder(client)

			var got int
			err := decoder.Decode(context.Background(), test.data, &got)

			test.wantErr(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestDecoder_DecodeZeroWidthPayloads(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
	}{
		{name: "null", schema: "null", value: new(any)},
		{name: "empty record", schema: `{"type":"record","name":"Empty","fields":[]}`, value: new(struct{})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "resolved", r.URL.Query().Get("format"))
				_ = json.NewEncoder(w).Encode(map[string]string{"schema": test.schema})
			}))
			t.Cleanup(srv.Close)
			client, err := registry.NewClient(srv.URL)
			require.NoError(t, err)

			decoder := registry.NewDecoder(client)
			err = decoder.Decode(context.Background(), []byte{0, 0, 0, 0, 42}, test.value)

			require.NoError(t, err)
		})
	}
}

func TestDecoder_DecodeResolvedReference(t *testing.T) {
	const resolved = `{"type":"record","name":"Parent","namespace":"com.acme","fields":[{"name":"child","type":{"type":"record","name":"Child","fields":[{"name":"value","type":"string"}]}}]}`
	schema := avro.MustParse(resolved)
	payload, err := avro.Marshal(schema, map[string]any{"child": map[string]any{"value": "ok"}})
	require.NoError(t, err)
	data := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(data[1:5], 42)
	copy(data[5:], payload)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/schemas/ids/42", r.URL.Path)
		require.Equal(t, "resolved", r.URL.Query().Get("format"))
		_, _ = w.Write([]byte(`{"schema":` + strconv.Quote(resolved) + `}`))
	}))
	t.Cleanup(srv.Close)
	client, err := registry.NewClient(srv.URL)
	require.NoError(t, err)
	decoder := registry.NewDecoder(client)

	var got map[string]any
	err = decoder.Decode(context.Background(), data, &got)

	require.NoError(t, err)
	require.Equal(t, map[string]any{"child": map[string]any{"value": "ok"}}, got)
}

type headerDecoder interface {
	DecodeHeaders(context.Context, []byte, []registry.Header, bool, any) error
}

func TestDecodeGUIDHeaders(t *testing.T) {
	const guid = "00010203-0405-0607-0809-0a0b0c0d0e0f"
	header := []byte{1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	var paths []string
	client, err := registry.NewClient("https://registry.invalid/base/", registry.WithHTTPClient(&http.Client{
		Transport: registryResponseTransport(func(r *http.Request) (*http.Response, error) {
			paths = append(paths, r.URL.RequestURI())
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"schema":"long"}`))}, nil
		}),
	}))
	require.NoError(t, err)
	dec, ok := any(registry.NewDecoder(client)).(headerDecoder)
	require.True(t, ok, "missing header-aware decoding capability")
	var got int64
	for _, key := range []bool{false, true} {
		name := "__value_schema_id"
		if key {
			name = "__key_schema_id"
		}
		headers := []registry.Header{{Key: name, Value: []byte{0}}, {Key: name, Value: header}}
		require.NoError(t, dec.DecodeHeaders(context.Background(), []byte{84}, headers, key, &got))
		require.Equal(t, int64(42), got)
	}
	require.Equal(t, []string{"/base/schemas/guids/" + guid}, paths, "GUID lookup is cached")
	require.NoError(t, dec.DecodeHeaders(context.Background(), []byte{0, 0, 0, 0, 7, 12}, nil, false, &got))
	require.Equal(t, int64(6), got)
	require.Equal(t, "/base/schemas/ids/7?format=resolved", paths[1])
}

func TestDecodeGUIDRejectsMalformedHeader(t *testing.T) {
	dec, ok := any(registry.NewDecoder(nil)).(headerDecoder)
	require.True(t, ok, "missing header-aware decoding capability")
	for _, header := range [][]byte{nil, {}, {1}, make([]byte, 17), append([]byte{1}, make([]byte, 17)...)} {
		var got int64 = 99
		// A present invalid header cannot fall back to otherwise valid legacy bytes.
		err := dec.DecodeHeaders(context.Background(), []byte{0, 0, 0, 0, 7, 12}, []registry.Header{{Key: "__value_schema_id", Value: header}}, false, &got)
		require.ErrorContains(t, err, "header")
		require.Equal(t, int64(99), got)
	}
}

func TestGUIDHeaderPrecedence(t *testing.T) {
	header := append([]byte{1}, make([]byte, 16)...)
	calls := 0
	client, err := registry.NewClient("https://registry.invalid", registry.WithHTTPClient(&http.Client{
		Transport: registryResponseTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			if strings.Contains(r.URL.Path, "/guids/") {
				return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader(`{"error_code":40403,"message":"missing"}`))}, nil
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"schema":"long"}`))}, nil
		}),
	}))
	require.NoError(t, err)
	dec := registry.NewDecoder(client)
	var got int64
	legacy := []byte{0, 0, 0, 0, 1, 14}
	headers := []registry.Header{{Key: registry.KeySchemaIDHeader, Value: header}}
	require.NoError(t, dec.DecodeHeaders(context.Background(), legacy, headers, false, &got))
	require.Equal(t, int64(7), got)
	require.Error(t, dec.DecodeHeaders(context.Background(), legacy, headers, true, &got))
	require.Equal(t, 2, calls, "failed GUID lookup must not try a numeric schema")
	require.Error(t, registry.NewDecoder(nil).DecodeHeaders(context.Background(), nil, headers, true, &got))
	require.Error(t, registry.NewDecoder(client, registry.WithAPI(nil)).DecodeHeaders(context.Background(), nil, headers, true, &got))
	require.Equal(t, 2, calls, "invalid API must not request a schema")
}

func TestConfluentRawBytes(t *testing.T) {
	client, err := registry.NewClient("https://registry.invalid", registry.WithHTTPClient(&http.Client{
		Transport: registryResponseTransport(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"schema":"bytes"}`))}, nil
		}),
	}))
	require.NoError(t, err)
	headers := []registry.Header{{Key: registry.ValueSchemaIDHeader, Value: append([]byte{1}, make([]byte, 16)...)}}
	for i, data := range [][]byte{{}, {0xff, 0x03}} {
		for _, header := range []bool{false, true} {
			t.Run(strconv.Itoa(i)+"/header="+strconv.FormatBool(header), func(t *testing.T) {
				var got []byte
				dec := registry.NewDecoder(client)
				if header {
					require.NoError(t, dec.DecodeHeaders(context.Background(), data, headers, false, &got))
				} else {
					require.NoError(t, dec.Decode(context.Background(), append([]byte{0, 0, 0, 0, 1}, data...), &got))
				}
				require.Equal(t, data, got)
			})
		}
	}
	limited := registry.NewDecoder(client, registry.WithAPI(avro.Config{MaxByteSliceSize: 1}.Freeze()))
	var got []byte
	require.Error(t, limited.DecodeHeaders(context.Background(), []byte{1, 2}, headers, false, &got))
}
