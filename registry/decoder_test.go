package registry_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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
