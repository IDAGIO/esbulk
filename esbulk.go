package esbulk

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ProcessLDJFile reads input file and creates an index given options using
// multiple workers
func ProcessLDJFile(r io.Reader, options Options) (int, error) {
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
		time.Sleep(5 * time.Second)
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
			// Realtime search.
			if _, err := indexSettingsRequest(`{"index": {"refresh_interval": "1s"}}`, options); err != nil {
				log.Fatal(err)
			}
			// Reset number of replicas.
			if _, err := indexSettingsRequest(fmt.Sprintf(`{"index": {"number_of_replicas": %q}}`, numberOfReplicas), options); err != nil {
				log.Fatal(err)
			}

			// Persist documents.
			if err := FlushIndex(i, options); err != nil {
				log.Fatal(err)
			}
		}()

		// Realtime search.
		resp, err := indexSettingsRequest(`{"index": {"refresh_interval": "-1"}}`, options)
		if err != nil {
			return count, err
		}
		if resp.StatusCode >= 400 {
			return count, fmt.Errorf("failed with response %v", resp)
		}
		if options.ZeroReplica {
			// Reset number of replicas.
			if _, err := indexSettingsRequest(`{"index": {"number_of_replicas": 0}}`, options); err != nil {
				return count, err
			}
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

// ParseLDJFilename parses filename to get index options for an index insertion
func ParseLDJFilename(path string, defaults Options) (Options, error) {
	log.Printf("processing file %q...", path)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return Options{}, fmt.Errorf("failed to load %q as it does not exists", path)
	}

	tokens := strings.Split(filepath.Base(path), ".")
	switch len(tokens) {
	case 4:
		// with id argument
		defaults.Index = tokens[2]
		defaults.DocType = tokens[1]
		defaults.IDField = tokens[0]
		return defaults, nil
	case 3:
		defaults.Index = tokens[1]
		defaults.DocType = tokens[0]
		return defaults, nil
	default:
		// no id argument
		return Options{}, fmt.Errorf("failed to parse source LDL file %q", path)
	}
}

// indexSettingsRequest runs updates an index setting, given a body and options.
func indexSettingsRequest(body string, options Options) (*http.Response, error) {
	// Body consist of the JSON document, e.g. `{"index": {"refresh_interval": "1s"}}`.
	r := strings.NewReader(body)

	rand.Seed(time.Now().Unix())
	server := options.Servers[rand.Intn(len(options.Servers))]
	link := fmt.Sprintf("%s/%s/_settings", server, options.Index)

	req, err := http.NewRequest("PUT", link, r)
	if err != nil {
		return nil, err
	}
	// Auth handling.
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if options.Verbose {
		log.Printf("applied setting: %s with status %s\n", body, resp.Status)
	}
	return resp, nil
}
