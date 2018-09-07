package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/idagio/esbulk"
)

// esbulk -id uuid -0 -index search-2018-11-13-10-23 -type album -w 4 -verbose -server $ELASTICSEARCH_URL out/album.search-2018-11-13-10-23.ldj
// esbulk -id id -0 -index metadata-de-de-2018-11-13-12-29 -type artist -w 4 -verbose -server $ELASTICSEARCH_URL out/artist.metadata-de-de-2018-11-13-12-29.ldj
// esbulk -id id -0 -index metadata-de-de-2018-11-13-12-29 -type work -w 4 -verbose -server $ELASTICSEARCH_URL out/work.metadata-de-de-2018-11-13-12-29.ldj
// esbulk -id id -0 -index search-2018-11-13-10-23 -type piece -w 4 -verbose -server $ELASTICSEARCH_URL out/piece.search-2018-11-13-10-23.ldj
// esbulk -id id -0 -index metadata-2018-11-13-11-23 -type recording -w 4 -verbose -server $ELASTICSEARCH_URL out/recording.metadata-2018-11-13-11-23.ldj
// esbulk -id uuid -0 -index search-2018-11-13-10-23 -type playlist -w 4 -verbose -server $ELASTICSEARCH_URL out/playlist.search-2018-11-13-10-23.ldj
// esbulk -id id -0 -index metadata-2018-11-13-11-23 -type artist -w 4 -verbose -server $ELASTICSEARCH_URL out/artist.metadata-2018-11-13-11-23.ldj
// esbulk -id uuid -0 -index metadata-2018-11-13-11-23 -type album -w 4 -verbose -server $ELASTICSEARCH_URL out/album.metadata-2018-11-13-11-23.ldj
// esbulk -id id -0 -index search-2018-11-13-10-23 -type work -w 4 -verbose -server $ELASTICSEARCH_URL out/work.search-2018-11-13-10-23.ldj
// esbulk -id id -0 -index metadata-de-de-2018-11-13-12-29 -type recording -w 4 -verbose -server $ELASTICSEARCH_URL out/recording.metadata-de-de-2018-11-13-12-29.ldj
// esbulk -id id -0 -index metadata-2018-11-13-11-23 -type track -w 4 -verbose -server $ELASTICSEARCH_URL out/track.metadata-2018-11-13-11-23.ldj
// esbulk -id id -0 -index metadata-2018-11-13-11-23 -type work -w 4 -verbose -server $ELASTICSEARCH_URL out/work.metadata-2018-11-13-11-23.ldj
// esbulk -id uuid -0 -index metadata-de-de-2018-11-13-12-29 -type album -w 4 -verbose -server $ELASTICSEARCH_URL out/album.metadata-de-de-2018-11-13-12-29.ldj
// esbulk -id id -0 -index metadata-de-de-2018-11-13-12-29 -type track -w 4 -verbose -server $ELASTICSEARCH_URL out/track.metadata-de-de-2018-11-13-12-29.ldj
// esbulk -id id -0 -index search-2018-11-13-10-23 -type artist -w 4 -verbose -server $ELASTICSEARCH_URL out/artist.search-2018-11-13-10-23.ldj
// esbulk -id id -0 -index search-2018-11-13-10-23 -type recording -w 4 -verbose -server $ELASTICSEARCH_URL out/recording.search-2018-11-13-10-23.ldj

func TestParseLDJFileWithInvalidExtension(t *testing.T) {
	defaults := getDefaultOptions()

	sourceFilename := "id.recording.search-2018-11-13-10-23.foo"
	sourceFilePath, _ := createLDJFile(sourceFilename)
	defer cleanupLDJFile(sourceFilePath)
	_, err := parseSourceFile(defaults, sourceFilename)

	expectedError := fmt.Sprintf("failed to load %q as it is not IDL file", sourceFilename)

	if err.Error() != expectedError {
		t.Errorf("Expect to get an error %q but got %q\n", expectedError, err.Error())
	}
}

func TestParseLDJFileWithId(t *testing.T) {
	defaults := getDefaultOptions()

	sourceFilename := "id.recording.search-2018-11-13-10-23.ldj"
	sourceFilePath, _ := createLDJFile(sourceFilename)
	defer cleanupLDJFile(sourceFilePath)

	options, _ := parseSourceFile(defaults, sourceFilePath)

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
