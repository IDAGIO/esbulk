package esbulk

import (
	// "fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

//TODO: do profiling on single and multiple files -cpuprofile cp -memprofile mp

// ./esbulk -id uuid -0 -index search-2018-11-14-10-23 -type album -w 4 -verbose -server http://localhost:9200 ~/esbulk_out/album.search-2018-11-14-10-23.ldj
// ./esbulk -id uuid -0 -index metadata-2018-11-14-11-26 -type album -w 4 -verbose -server http://localhost:9200 ~/esbulk_out/album.metadata-2018-11-14-11-26.ldj

// ./esbulk -id uuid -0 -w 4 -verbose -server http://localhost:9200 -dir ~/esbulk_out

func TestParseLDJFilenameWithoutId(t *testing.T) {
	defaults := getDefaultOptions([]string{"http://localhost:9200"})

	filename := "recording.search-2018-11-13-10-23.ldj"
	path, _ := createLDJFile(filename)
	defer cleanupLDJFile(path)

	options, err := ParseLDJFilename(path, defaults)
	if err != nil {
		t.Error(err)
	}

	if options.DocType != "recording" {
		t.Errorf("Expected DocType should have value \"recording\" but has %q", options.DocType)
	}
	if options.Index != "search-2018-11-13-10-23" {
		t.Errorf("Expected Index should have value \"search-2018-11-13-10-23\" but has %q", options.Index)
	}
}
func TestParseLDJFilenameWithId(t *testing.T) {
	defaults := getDefaultOptions([]string{"http://localhost:9200"})

	filename := "id.recording.search-2018-11-13-10-23.ldj"
	path, _ := createLDJFile(filename)
	defer cleanupLDJFile(path)

	options, err := ParseLDJFilename(path, defaults)
	if err != nil {
		t.Error(err)
	}

	if options.IDField != "id" {
		t.Errorf("Expected IDField should have value \"id\" but has %q", options.IDField)
	}
	if options.DocType != "recording" {
		t.Errorf("Expected DocType should have value \"recording\" but has %q", options.DocType)
	}
	if options.Index != "search-2018-11-13-10-23" {
		t.Errorf("Expected Index should have value \"search-2018-11-13-10-23\" but has %q", options.Index)
	}
}

func TestIndexSettingsRequest(t *testing.T) {
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

	options := getDefaultOptions([]string{server.URL})

	_, err := indexSettingsRequest(`{"index": {"refresh_interval": "1s"}}`, options)
	if err != nil {
		t.Error(err)
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

func cleanupLDJFile(path string) {
	os.RemoveAll(filepath.Dir(path))
}
