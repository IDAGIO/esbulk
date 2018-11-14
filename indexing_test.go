package esbulk_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/idagio/esbulk"
)

func TestDeleteIndexBackoff(t *testing.T) {
	expectedMaxRetries := 3
	currentRetries := 0

	// Start a local HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		currentRetries++
		rw.WriteHeader(http.StatusGatewayTimeout)
		rw.Write([]byte("504 Gateway Timeout"))
	}))
	defer server.Close()

	options := esbulk.Options{
		Servers:    []string{server.URL},
		Index:      "exampleIndex",
		DocType:    "default",
		BatchSize:  1,
		Verbose:    true,
		Scheme:     "http",
		MaxRetries: expectedMaxRetries,
		IDField:    "",
		Username:   "",
		Password:   "",
	}

	esbulk.DeleteIndex(options)

	if currentRetries != expectedMaxRetries {
		t.Errorf("Expected %d, found %d", expectedMaxRetries, currentRetries)
	}
}
