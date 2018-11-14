package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/idagio/esbulk"
)

//TODO: do profiling on single and multiple files -cpuprofile cp -memprofile mp

// ./esbulk -id uuid -0 -index search-2018-11-14-10-23 -type album -w 4 -verbose -server http://localhost:9200 ~/esbulk_out/album.search-2018-11-14-10-23.ldj
// ./esbulk -id uuid -0 -index metadata-2018-11-14-11-26 -type album -w 4 -verbose -server http://localhost:9200 ~/esbulk_out/album.metadata-2018-11-14-11-26.ldj

// ./esbulk -id uuid -0 -w 4 -verbose -server http://localhost:9200 -dir ~/esbulk_out

func TestParseLDJFileWithoutId(t *testing.T) {
	defaults := getDefaultOptions()

	filename := "recording.search-2018-11-13-10-23.ldj"
	path, _ := createLDJFile(filename)
	// defer cleanupLDJFile(path)

	options, err := parseLDJFile(path, defaults)
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
func TestParseLDJFileWithId(t *testing.T) {
	defaults := getDefaultOptions()

	filename := "id.recording.search-2018-11-13-10-23.ldj"
	path, _ := createLDJFile(filename)
	// defer cleanupLDJFile(path)

	options, err := parseLDJFile(path, defaults)
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

func getDefaultOptions() esbulk.Options {
	return esbulk.Options{
		Servers:   []string{"http://localhost:9200"},
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
