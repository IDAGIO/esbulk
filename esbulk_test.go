package esbulk

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	idField          = "id"
	recordingDocType = "recording"
	indexName        = "search-2018-11-13-10-23"
)

func TestIndexOptionsFromFilepathWithoutId(t *testing.T) {
	defaultOptions := getDefaultOptions([]string{"http://localhost:9200"})
	filename := fmt.Sprintf("%s.%s.ldj", recordingDocType, indexName)

	path, _ := createLDJFile(filename)
	defer cleanupLDJFile(path)

	updatedOptions, err := IndexOptionsFromFilepath(path, defaultOptions)
	if err != nil {
		t.Error(err)
	}

	if updatedOptions.DocType != recordingDocType {
		t.Errorf("Expected DocType should have value %q but has %q", recordingDocType, updatedOptions.DocType)
	}
	if updatedOptions.Index != "search-2018-11-13-10-23" {
		t.Errorf("Expected Index should have value %q but has %q", indexName, updatedOptions.Index)
	}
}
func TestIndexOptionsFromFilepathWithId(t *testing.T) {
	defaultOptions := getDefaultOptions([]string{"http://localhost:9200"})

	filename := fmt.Sprintf("%s.%s.%s.ldj", recordingDocType, idField, indexName)

	path, _ := createLDJFile(filename)
	defer cleanupLDJFile(path)

	updatedOptions, err := IndexOptionsFromFilepath(path, defaultOptions)
	if err != nil {
		t.Error(err)
	}

	if updatedOptions.IDField != idField {
		t.Errorf("Expected IDField should have value %q but has %q", idField, updatedOptions.IDField)
	}
	if updatedOptions.DocType != recordingDocType {
		t.Errorf("Expected DocType should have value %q but has %q", recordingDocType, updatedOptions.DocType)
	}
	if updatedOptions.Index != indexName {
		t.Errorf("Expected Index should have value %q but has %q", indexName, updatedOptions.Index)
	}
}

func TestIndexOptionsFromFilepathNotExisting(t *testing.T) {
	defaultOptions := getDefaultOptions([]string{"http://localhost:9200"})
	path := "/tmp/foo/bar"

	_, err := IndexOptionsFromFilepath(path, defaultOptions)
	if err.Error() != fmt.Sprintf("failed to load %q as it does not exists", path) {
		t.Errorf("Expected error %q but got %q", path, err)
	}
}

func TestUpdateIndexSettings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" {
			t.Errorf("Expect PUT request but got %q", req.Method)
		}

		if !strings.HasSuffix(req.URL.Path, "/_settings") {
			t.Error("Expect request URI to end with \"_settings\"")
		}
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("OK"))
	}))
	defer server.Close()

	defaultOptions := getDefaultOptions([]string{server.URL})

	_, err := updateIndexSettings(`{"index": {"refresh_interval": "1s"}}`, defaultOptions)
	if err != nil {
		t.Error(err)
	}
}

func TestWaitForIndexDeletion(t *testing.T) {
	currentRetries := 0
	succeedOnRetry := 2
	indexName := "exampleIndex"

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		currentRetries++
		if req.Method != "HEAD" {
			t.Errorf("Expect HEAD request but got %q", req.Method)
		}

		if !strings.HasSuffix(req.URL.Path, indexName) {
			t.Errorf("Expect request URI to end with %q", indexName)
		}

		if currentRetries == succeedOnRetry {
			rw.WriteHeader(http.StatusNotFound)
			rw.Write([]byte("Not Found"))
		} else {
			rw.WriteHeader(http.StatusOK)
			rw.Write([]byte("OK"))
		}
	}))
	defer server.Close()

	options := getDefaultOptions([]string{server.URL})

	for i := 1; i <= maxRetriesUntilIndexIsDeleted; i++ {
		err := waitForIndexDeletion(options, 0)
		if err != nil {
			t.Error(err)
		} else {
			break
		}
	}

	if currentRetries != succeedOnRetry {
		t.Errorf("Expected %d retries, found %d", succeedOnRetry, currentRetries)
	}
}

func getDefaultOptions(servers []string) Options {
	return Options{
		Servers:   servers,
		Index:     "exampleIndex",
		DocType:   "default",
		BatchSize: 1,
		Verbose:   true,
		Scheme:    "http",
		IDField:   "",
		Username:  "",
		Password:  "",
	}
}

func createLDJFile(filename string) (string, error) {
	dir, err := ioutil.TempDir("", "out")
	if err != nil {
		return "", err
	}

	path := filepath.Join(dir, filename)
	content := []byte("{\"test\": 1}")
	if err := ioutil.WriteFile(path, content, 0666); err != nil {
		return "", err
	}

	return path, nil
}

func cleanupLDJFile(path string) error {
	return os.Remove(path)
}
