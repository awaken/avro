package registry

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient_WithHTTPClient(t *testing.T) {
	httpClient := &http.Client{}

	client, _ := NewClient("http://example.com", WithHTTPClient(httpClient))

	assert.Equal(t, client.client, httpClient)
}

func TestClient_NilHTTPClientAfterConstruction(t *testing.T) {
	client, err := NewClient("http://example.com")
	require.NoError(t, err)
	WithHTTPClient(nil)(client)

	var requestErr error
	require.NotPanics(t, func() {
		_, requestErr = client.GetSubjects(context.Background())
	})
	require.ErrorContains(t, requestErr, "http client cannot be nil")
}

func TestNewClient_WithBasicAuth(t *testing.T) {
	creds := credentials{username: "username", password: "password"}

	client, _ := NewClient("http://example.com", WithBasicAuth("username", "password"))

	assert.Equal(t, client.creds, creds)
}

func TestEscapeSubject(t *testing.T) {
	tests := []struct {
		name    string
		subject string
		want    string
	}{
		{name: "plain", subject: "orders-value", want: "orders-value"},
		{name: "slash", subject: "path/to.proto", want: "path%2Fto.proto"},
		{name: "dot", subject: ".", want: "%2E"},
		{name: "dot dot", subject: "..", want: "%2E%2E"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, escapeSubject(test.subject))
		})
	}
}
