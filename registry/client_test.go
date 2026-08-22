package registry_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/awaken/avro/v2/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const referencedParentSchema = `{"type":"record","name":"Parent","namespace":"com.acme","fields":[{"name":"child","type":"com.acme.Child"}]}`

const resolvedParentSchema = `{"type":"record","name":"Parent","namespace":"com.acme","fields":[{"name":"child","type":{"type":"record","name":"Child","fields":[{"name":"value","type":"string"}]}}]}`

func TestNewClient(t *testing.T) {
	client, err := registry.NewClient("http://example.com")

	require.NoError(t, err)
	assert.Implements(t, (*registry.Registry)(nil), client)
}

func TestNewClient_UrlError(t *testing.T) {
	_, err := registry.NewClient("://")

	assert.Error(t, err)
}

func TestSchemaInfo_IDBytes(t *testing.T) {
	schemaInfo := registry.SchemaInfo{
		ID: 123456789,
	}

	idBytes := schemaInfo.IDBytes()

	assert.Equal(t, []byte{7, 91, 205, 21}, idBytes)
}

func TestClient_PopulatesError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/vnd.schemaregistry.v1+json")
		w.WriteHeader(422)
		_, _ = w.Write([]byte(`{"error_code": 42202, "message": "schema may not be empty"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema(context.Background(), "test", "")

	assert.Error(t, err)
	assert.IsType(t, registry.Error{}, err)
	assert.Equal(t, "schema may not be empty", err.Error())
	regError := err.(registry.Error)
	assert.Equal(t, 422, regError.StatusCode)
	assert.Equal(t, 42202, regError.Code)
	assert.Equal(t, "schema may not be empty", regError.Message)
}

func TestNewClient_BasicAuth(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "username", username)
		assert.Equal(t, "password", password)

		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL, registry.WithBasicAuth("username", "password"))

	_, err := client.GetSubjects(context.Background())

	assert.NoError(t, err)
}

func TestClient_GetSchema(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/schemas/ids/5", r.URL.Path)
		assert.Equal(t, "resolved", r.URL.Query().Get("format"))

		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	schema, err := client.GetSchema(context.Background(), 5)

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_GetSchemaCachesSchema(t *testing.T) {
	count := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, _ = client.GetSchema(context.Background(), 5)

	_, _ = client.GetSchema(context.Background(), 5)

	assert.Equal(t, 1, count)
}

func TestClient_GetSchemaRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(context.Background(), 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(context.Background(), 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(context.Background(), 5)

	assert.Error(t, err)
}

func TestClient_GetSubjects(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects", r.URL.Path)

		_, _ = w.Write([]byte(`["foobar"]`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	subs, err := client.GetSubjects(context.Background())

	require.NoError(t, err)
	require.Len(t, subs, 1)
	assert.Equal(t, "foobar", subs[0])
}

func TestClient_DeleteSubject(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/subjects/foobar", r.URL.Path)

		_, _ = w.Write([]byte(`[1]`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	versions, err := client.DeleteSubject(context.Background(), "foobar")

	require.NoError(t, err)
	require.Len(t, versions, 1)
	assert.Equal(t, 1, versions[0])
}

func TestClient_DeleteSubjectRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.DeleteSubject(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetSubjectsRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSubjects(context.Background())

	assert.Error(t, err)
}

func TestClient_GetSubjectsJSONError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`["foobar"`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSubjects(context.Background())

	assert.Error(t, err)
}

func TestClient_GetVersions(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions", r.URL.Path)

		_, _ = w.Write([]byte(`[10]`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	vers, err := client.GetVersions(context.Background(), "foobar")

	require.NoError(t, err)
	require.Len(t, vers, 1)
	assert.Equal(t, 10, vers[0])
}

func TestClient_GetVersionsEscapesSubject(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/subjects/path%2Fto%20my.proto%3Fx/versions", r.URL.EscapedPath())
		assert.Empty(t, r.URL.RawQuery)

		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetVersions(context.Background(), "path/to my.proto?x")

	require.NoError(t, err)
}

func TestClient_GetVersionsRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetVersions(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetVersionsJSONError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[10`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetVersions(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetSchemaByVersion(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions/5", r.URL.Path)
		assert.Equal(t, "resolved", r.URL.Query().Get("format"))

		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	schema, err := client.GetSchemaByVersion(context.Background(), "foobar", 5)

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_GetSchemaByVersionRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaByVersion(context.Background(), "foobar", 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaByVersionJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaByVersion(context.Background(), "foobar", 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaByVersionSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaByVersion(context.Background(), "foobar", 5)

	assert.Error(t, err)
}

func TestClient_GetLatestSchema(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions/latest", r.URL.Path)
		assert.Equal(t, "resolved", r.URL.Query().Get("format"))

		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	schema, err := client.GetLatestSchema(context.Background(), "foobar")

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_GetLatestSchemaRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchema(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchema(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchema(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetSchemaInfo(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions/1", r.URL.Path)
		assert.Equal(t, "resolved", r.URL.Query().Get("format"))

		_, _ = w.Write([]byte(`{"subject": "foobar", "version": 1, "id": 2, "schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	schemaInfo, err := client.GetSchemaInfo(context.Background(), "foobar", 1)

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schemaInfo.Schema.String())
	assert.Equal(t, 2, schemaInfo.ID)
	assert.Equal(t, 1, schemaInfo.Version)
}

func TestClient_GetSchemaInfoWithMetadata(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions/1", r.URL.Path)
		assert.Equal(t, "resolved", r.URL.Query().Get("format"))

		_, _ = w.Write([]byte(`{"subject": "foobar", "version": 1, "id": 2, "metadata": {"properties": {"foo": "bar", "baz": "quux"}}, "schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	schemaInfo, err := client.GetSchemaInfo(context.Background(), "foobar", 1)

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schemaInfo.Schema.String())
	assert.Equal(t, 2, schemaInfo.ID)
	assert.Equal(t, 1, schemaInfo.Version)
	assert.Equal(t, "bar", schemaInfo.Metadata.Properties["foo"])
	assert.Equal(t, "quux", schemaInfo.Metadata.Properties["baz"])
}

func TestClient_GetSchemaInfoRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaInfo(context.Background(), "foobar", 1)

	assert.Error(t, err)
}

func TestClient_GetSchemaInfoJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"subject": "foobar", "version": 1, "id": 2, "schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaInfo(context.Background(), "foobar", 1)

	assert.Error(t, err)
}

func TestClient_GetSchemaInfoSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaInfo(context.Background(), "foobar", 1)

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaInfo(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions/latest", r.URL.Path)
		assert.Equal(t, "resolved", r.URL.Query().Get("format"))

		_, _ = w.Write([]byte(`{"subject": "foobar", "version": 1, "id": 2, "schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	schemaInfo, err := client.GetLatestSchemaInfo(context.Background(), "foobar")

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schemaInfo.Schema.String())
	assert.Equal(t, 2, schemaInfo.ID)
	assert.Equal(t, 1, schemaInfo.Version)
}

func TestClient_GetLatestSchemaInfoRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchemaInfo(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaInfoJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"subject": "foobar", "version": 1, "id": 2, "schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchemaInfo(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaInfoSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchemaInfo(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_CreateSchema(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/subjects/foobar/versions", r.URL.Path)

		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	id, schema, err := client.CreateSchema(context.Background(), "foobar", "[\"null\",\"string\",\"int\"]")

	require.NoError(t, err)
	assert.Equal(t, 10, id)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_CreateSchemaWithRefsReturnsResolvedSchema(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/subjects/parent/versions":
			var payload map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
			require.Len(t, payload["references"], 1)
			_, _ = w.Write([]byte(`{"id":10}`))
		case r.Method == http.MethodGet && r.URL.Path == "/schemas/ids/10":
			require.Equal(t, "resolved", r.URL.Query().Get("format"))
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": resolvedParentSchema})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(s.Close)
	client, err := registry.NewClient(s.URL)
	require.NoError(t, err)

	id, schema, err := client.CreateSchema(
		context.Background(),
		"parent",
		referencedParentSchema,
		registry.SchemaReference{Name: "com.acme.Child", Subject: "child", Version: 1},
	)

	require.NoError(t, err)
	require.Equal(t, 10, id)
	require.Equal(t, avro.MustParse(resolvedParentSchema).String(), schema.String())
}

func TestClient_CreateSchemaRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema(context.Background(), "foobar", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_CreateSchemaJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema(context.Background(), "foobar", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_CreateSchemaSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema(context.Background(), "foobar", "[\"null\",\"string\",\"int\"")

	assert.Error(t, err)
}

func TestClient_IsRegistered(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/subjects/test", r.URL.Path)

		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	id, schema, err := client.IsRegistered(context.Background(), "test", "[\"null\",\"string\",\"int\"]")

	require.NoError(t, err)
	assert.Equal(t, 10, id)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_IsRegisteredWithRefs(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			require.Equal(t, "/schemas/ids/10", r.URL.Path)
			require.Equal(t, "resolved", r.URL.Query().Get("format"))
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": resolvedParentSchema})
			return
		}

		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/subjects/test", r.URL.Path)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var decoded map[string]any
		err = json.Unmarshal(body, &decoded)
		require.NoError(t, err)

		assert.Contains(t, decoded, "references")
		refs, ok := decoded["references"].([]any)
		require.True(t, ok)
		require.Len(t, refs, 1)
		ref, ok := refs[0].(map[string]any)
		require.True(t, ok)

		assert.Equal(t, "com.acme.Child", ref["name"].(string))
		assert.Equal(t, "some_subject", ref["subject"].(string))
		assert.Equal(t, float64(3), ref["version"].(float64))

		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	id, schema, err := client.IsRegisteredWithRefs(
		context.Background(),
		"test",
		referencedParentSchema,
		registry.SchemaReference{
			Name:    "com.acme.Child",
			Subject: "some_subject",
			Version: 3,
		},
	)

	require.NoError(t, err)
	assert.Equal(t, 10, id)
	assert.Equal(t, avro.MustParse(resolvedParentSchema).String(), schema.String())
}

func TestClient_ReferenceRetrievalsRequestResolvedFormat(t *testing.T) {
	requests := map[string]int{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "resolved", r.URL.Query().Get("format"))
		requests[r.URL.Path]++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      10,
			"version": 2,
			"schema":  resolvedParentSchema,
		})
	}))
	t.Cleanup(s.Close)
	client, err := registry.NewClient(s.URL)
	require.NoError(t, err)
	want := avro.MustParse(resolvedParentSchema).String()

	byID, err := client.GetSchema(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, want, byID.String())

	byVersion, err := client.GetSchemaByVersion(context.Background(), "parent", 2)
	require.NoError(t, err)
	require.Equal(t, want, byVersion.String())

	latest, err := client.GetLatestSchema(context.Background(), "parent")
	require.NoError(t, err)
	require.Equal(t, want, latest.String())

	info, err := client.GetSchemaInfo(context.Background(), "parent", 2)
	require.NoError(t, err)
	require.Equal(t, want, info.Schema.String())

	latestInfo, err := client.GetLatestSchemaInfo(context.Background(), "parent")
	require.NoError(t, err)
	require.Equal(t, want, latestInfo.Schema.String())

	require.Equal(t, map[string]int{
		"/schemas/ids/10":                  1,
		"/subjects/parent/versions/2":      2,
		"/subjects/parent/versions/latest": 2,
	}, requests)
}

func TestClient_ResolvedFormatUnsupported(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "resolved", r.URL.Query().Get("format"))
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error_code":42205,"message":"unsupported format resolved"}`))
	}))
	t.Cleanup(s.Close)
	client, err := registry.NewClient(s.URL)
	require.NoError(t, err)

	_, err = client.GetSchema(context.Background(), 10)
	require.ErrorContains(t, err, "getting resolved schema 10")
	require.ErrorContains(t, err, "unsupported format resolved")
}

func TestClient_IsRegisteredRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.IsRegistered(context.Background(), "test", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_IsRegisteredJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.IsRegistered(context.Background(), "test", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_IsRegisteredSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.IsRegistered(context.Background(), "test", "[\"null\",\"string\",\"int\"")

	assert.Error(t, err)
}

func TestClient_GetGlobalCompatibilityLevel(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/config", r.URL.Path)

		_, _ = w.Write([]byte(`{
			"compatibilityLevel": "FULL"
		  }`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	compatibilityLevel, err := client.GetGlobalCompatibilityLevel(context.Background())

	require.NoError(t, err)
	assert.Equal(t, registry.FullCL, compatibilityLevel)
}

func TestClient_GetGlobalCompatibilityLevelError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetGlobalCompatibilityLevel(context.Background())

	assert.Error(t, err)
}

func TestClient_GetCompatibilityLevel(t *testing.T) {
	subject := "boh_subj"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/config/"+subject, r.URL.Path)

		_, _ = w.Write([]byte(`{"compatibilityLevel":"FULL"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	compatibilityLevel, err := client.GetCompatibilityLevel(context.Background(), subject)

	require.NoError(t, err)
	assert.Equal(t, registry.FullCL, compatibilityLevel)
}

func TestClient_GetCompatibilityLevelError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetCompatibilityLevel(context.Background(), "boh")

	assert.Error(t, err)
}

func TestClient_SetGlobalCompatibilityLevel(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/config", r.URL.Path)

		_, _ = w.Write([]byte("{\"compatibility\":\"BACKWARD\"}"))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	err := client.SetGlobalCompatibilityLevel(context.Background(), registry.BackwardCL)

	require.NoError(t, err)
}

func TestClient_SetGlobalCompatibilityLevelError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	err := client.SetGlobalCompatibilityLevel(context.Background(), registry.BackwardCL)

	assert.Error(t, err)
}

func TestClient_SetGlobalCompatibilityLevelHandlesInvalidLevel(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.FailNow(t, "unexpected call")
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	err := client.SetGlobalCompatibilityLevel(context.Background(), "INVALID_STUFF")

	assert.Error(t, err)
}

func TestClient_SetCompatibilityLevel(t *testing.T) {
	subject := "boh_subj"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/config/"+subject, r.URL.Path)

		_, _ = w.Write([]byte("{\"compatibility\":\"BACKWARD\"}"))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	err := client.SetCompatibilityLevel(context.Background(), subject, registry.BackwardCL)

	require.NoError(t, err)
}

func TestClient_SetCompatibilityLevelError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	err := client.SetCompatibilityLevel(context.Background(), "boh", registry.BackwardCL)

	assert.Error(t, err)
}

func TestClient_SetCompatibilityLevelHandlesInvalidLevel(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.FailNow(t, "unexpected call")
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	err := client.SetCompatibilityLevel(context.Background(), "boh", "INVALID_STUFF")

	assert.Error(t, err)
}

func TestClient_IsCompatible(t *testing.T) {
	subject := "test_subject"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/compatibility/subjects/"+subject+"/versions", r.URL.Path)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var decoded map[string]any
		err = json.Unmarshal(body, &decoded)
		require.NoError(t, err)

		assert.Contains(t, decoded, "schema")
		schema, ok := decoded["schema"].(string)
		require.True(t, ok)
		require.Equal(t, schema, "[\"null\",\"string\",\"int\"]")

		_, _ = w.Write([]byte(`{"is_compatible":true}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	isCompatible, err := client.IsCompatible(
		context.Background(),
		"test_subject",
		"[\"null\",\"string\",\"int\"]",
	)

	require.NoError(t, err)
	assert.Equal(t, true, isCompatible)
}

func TestClient_IsCompatibleWithRefs(t *testing.T) {
	subject := "test_subject"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/compatibility/subjects/"+subject+"/versions", r.URL.Path)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var decoded map[string]any
		err = json.Unmarshal(body, &decoded)
		require.NoError(t, err)

		assert.Contains(t, decoded, "schema")
		schema, ok := decoded["schema"].(string)
		require.True(t, ok)
		require.Equal(t, schema, "[\"null\",\"string\",\"int\"]")

		assert.Contains(t, decoded, "references")
		refs, ok := decoded["references"].([]any)
		require.True(t, ok)
		require.Len(t, refs, 1)
		ref, ok := refs[0].(map[string]any)
		require.True(t, ok)

		assert.Equal(t, "some_schema", ref["name"].(string))
		assert.Equal(t, "some_subject", ref["subject"].(string))
		assert.Equal(t, float64(3), ref["version"].(float64))

		_, _ = w.Write([]byte(`{"is_compatible":true}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	isCompatible, err := client.IsCompatibleWithRefs(
		context.Background(),
		"test_subject",
		"[\"null\",\"string\",\"int\"]",
		registry.SchemaReference{
			Name:    "some_schema",
			Subject: "some_subject",
			Version: 3,
		},
	)

	require.NoError(t, err)
	assert.Equal(t, true, isCompatible)
}

func TestClient_HandlesServerError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(context.Background(), 5)

	assert.Error(t, err)
}

func TestError_Error(t *testing.T) {
	err := registry.Error{
		StatusCode: 404,
		Code:       40403,
		Message:    "Schema not found",
	}

	str := err.Error()

	assert.Equal(t, "Schema not found", str)
}

func TestError_ErrorEmptyMessage(t *testing.T) {
	err := registry.Error{
		StatusCode: 404,
	}

	str := err.Error()

	assert.Equal(t, "registry error: 404", str)
}
