package esbulk

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Options represents bulk indexing options.
type Options struct {
	Servers     []string
	Index       string
	Purge       bool
	Mapping     string
	DocType     string
	NumWorkers  int
	ZeroReplica bool
	GZipped     bool
	BatchSize   int
	Verbose     bool
	IDField     string
	Scheme      string // http or https; deprecated, use: Servers.
	Username    string
	Password    string
	MaxRetries  int
}

const (
	maxRetriesUntilIndexIsDeleted = 5
)

// CreateIndexFromLDJFile reads input file and creates an index given options using
// multiple workers
func CreateIndexFromLDJFile(r io.Reader, options Options) (int, error) {
	count := 0

	if options.Index == "" {
		return count, errors.New("index name required")
	}

	if options.Verbose {
		log.Println(options)
	}

	if options.Purge {
		if err := DeleteIndex(options); err != nil {
			return count, err
		}

		// Wait until index is deleted
		if err := waitForIndexDeletion(options, 0); err != nil {
			log.Fatalf("unable to check if index is deleted")
		}
	}

	// Create index if not exists.
	if err := CreateIndex(options); err != nil {
		return count, err
	}

	if options.Mapping != "" {
		var reader io.Reader
		if _, err := os.Stat(options.Mapping); os.IsNotExist(err) {
			reader = strings.NewReader(options.Mapping)
		} else {
			file, err := os.Open(options.Mapping)
			if err != nil {
				return count, err
			}
			reader = bufio.NewReader(file)
		}

		if err := PutMapping(options, reader); err != nil {
			return count, err
		}
	}

	queue := make(chan string)
	var wg sync.WaitGroup

	for i := 0; i < options.NumWorkers; i++ {
		wg.Add(1)
		go Worker(fmt.Sprintf("worker-%d", i), options, queue, &wg)
	}

	for i := range options.Servers {
		// Store number_of_replicas settings for restoration later.
		doc, err := GetSettings(i, options)
		if err != nil {
			return count, err
		}

		// TODO(miku): Rework this.
		numberOfReplicas := doc[options.Index].(map[string]interface{})["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"]
		if options.Verbose {
			log.Printf("on shutdown, number_of_replicas will be set back to %s", numberOfReplicas)
		}

		// Shutdown procedure. TODO(miku): Handle signals, too.
		defer func() {
			// Realtime search & reset number of replicas.
			if _, err := updateIndexSettings(fmt.Sprintf(`{"index": {"refresh_interval": "1s", "number_of_replicas": %q}}`, numberOfReplicas), options); err != nil {
				log.Fatal(err)
			}

			// Persist documents.
			if FlushIndex(i, options) != nil {
				log.Fatal(err)
			}
		}()

		// Realtime search and reset number of replicas (if specified).
		var indexRequest = `{"index": {"refresh_interval": "-1"}}`
		if options.ZeroReplica {
			indexRequest = `{"index": {"refresh_interval": "-1", "number_of_replicas": 0}}`
		}
		resp, err := updateIndexSettings(indexRequest, options)
		if err != nil {
			return count, err
		}
		if resp.StatusCode >= 400 {
			log.Fatal(resp)
		}
	}

	reader := bufio.NewReader(r)
	if options.GZipped {
		zreader, err := gzip.NewReader(r)
		if err != nil {
			return count, err
		}
		reader = bufio.NewReader(zreader)
	}

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		queue <- line
		count++
	}

	close(queue)
	wg.Wait()

	return count, nil
}

// IndexOptionsFromFilepath parses filename to get index options for an index insertion
func IndexOptionsFromFilepath(path string, defaults Options) (Options, error) {
	log.Printf("processing file %q...", path)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return Options{}, fmt.Errorf("failed to load %q as it does not exists", path)
	}

	tokens := strings.Split(filepath.Base(path), ".")
	switch len(tokens) {
	case 4:
		defaults.DocType = tokens[0]
		defaults.IDField = tokens[1]
		defaults.Index = tokens[2]
		return defaults, nil
	case 3:
		defaults.DocType = tokens[0]
		defaults.Index = tokens[1]
		return defaults, nil
	default:
		return Options{}, fmt.Errorf("failed to parse source LDJ file %q", path)
	}
}

// updateIndexSettings updates the elasticsearch index settings
func updateIndexSettings(body string, options Options) (*http.Response, error) {
	// Body consist of the JSON document, e.g. `{"index": {"refresh_interval": "1s"}}`.
	server := PickServerURI(options.Servers)
	link := fmt.Sprintf("%s/%s/_settings", server, options.Index)

	req, err := MakeHTTPRequest(options, "PUT", link, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	client := MakeHTTPClient(options.MaxRetries)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if options.Verbose {
		log.Printf("applied setting: %s with status %s\n", body, resp.Status)
	}
	return resp, nil
}

func waitForIndexDeletion(options Options, retry int) error {
	if retry > maxRetriesUntilIndexIsDeleted {
		return errors.New("unable to check if index is deleted")
	}

	exists, err := indexExists(options)
	if err != nil {
		return errors.New("unable to check if index is deleted")
	}
	if exists {
		time.Sleep(5 * time.Second)
		retry++
		return waitForIndexDeletion(options, retry)
	}
	return nil
}

func indexExists(options Options) (bool, error) {
	server := PickServerURI(options.Servers)
	link := fmt.Sprintf("%s/%s", server, options.Index)

	req, err := MakeHTTPRequest(options, "HEAD", link, nil)
	if err != nil {
		return false, err
	}
	client := MakeHTTPClient(options.MaxRetries)
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		return true, nil
	}
	if resp.StatusCode == 404 {
		return false, nil
	}

	return false, errors.New("unable to check if index exists")
}
