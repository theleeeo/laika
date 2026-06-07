package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/theleeeo/indexer/app/config"
	"github.com/theleeeo/indexer/es"
)

func main() {
	configPath := flag.String("config", "resources.yml", "Path to resource config file")
	resourceName := flag.String("resource", "", "Resource name to clean up old indexes for (required)")
	esAddr := flag.String("es-addr", "http://localhost:9200", "Elasticsearch address")
	esUser := flag.String("es-user", "", "Elasticsearch username")
	esPass := flag.String("es-pass", "", "Elasticsearch password")
	flag.Parse()

	if *resourceName == "" {
		flag.Usage()
		os.Exit(1)
	}

	resources, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load resource config: %v", err)
	}

	cfg := resources.Get(*resourceName)
	if cfg == nil {
		log.Fatalf("unknown resource %q", *resourceName)
	}

	addr := strings.TrimRight(*esAddr, "/")
	aliasName := es.AliasName(cfg.Resource)

	aliasTarget, err := getAliasTarget(addr, aliasName, *esUser, *esPass)
	if err != nil {
		log.Fatalf("get alias target: %v", err)
	}

	versions := cfg.SortedVersions()
	activeIndexes := make(map[string]bool, len(versions))
	for _, v := range versions {
		activeIndexes[es.IndexName(cfg.Resource, v)] = true
	}

	prefix := cfg.Resource + "_search_v"
	indexes, err := listIndexes(addr, prefix, *esUser, *esPass)
	if err != nil {
		log.Fatalf("list indexes: %v", err)
	}

	deleted := 0
	for _, idx := range indexes {
		if activeIndexes[idx] {
			continue
		}
		if idx == aliasTarget {
			log.Printf("skipping %s (currently pointed to by alias %s)", idx, aliasName)
			continue
		}

		log.Printf("deleting old index %s", idx)
		if err := deleteIndex(addr, idx, *esUser, *esPass); err != nil {
			log.Fatalf("delete index %s: %v", idx, err)
		}
		deleted++
	}

	if deleted == 0 {
		log.Printf("no old indexes to clean up for resource %q", *resourceName)
	} else {
		log.Printf("deleted %d old index(es)", deleted)
	}
}

func getAliasTarget(addr, aliasName, user, pass string) (string, error) {
	url := fmt.Sprintf("%s/_alias/%s", addr, aliasName)
	statusCode, respBody, err := doRequest(http.MethodGet, url, nil, user, pass)
	if err != nil {
		return "", err
	}
	if statusCode == http.StatusNotFound {
		return "", nil
	}
	if statusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
	}

	var decoded map[string]any
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return "", err
	}
	for indexName := range decoded {
		return indexName, nil
	}
	return "", nil
}

func listIndexes(addr, prefix, user, pass string) ([]string, error) {
	url := fmt.Sprintf("%s/_cat/indices/%s*?format=json", addr, prefix)
	statusCode, respBody, err := doRequest(http.MethodGet, url, nil, user, pass)
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
	}

	var indices []struct {
		Index string `json:"index"`
	}
	if err := json.Unmarshal(respBody, &indices); err != nil {
		return nil, err
	}

	names := make([]string, len(indices))
	for i, idx := range indices {
		names[i] = idx.Index
	}
	return names, nil
}

func deleteIndex(addr, indexName, user, pass string) error {
	url := fmt.Sprintf("%s/%s", addr, indexName)
	statusCode, respBody, err := doRequest(http.MethodDelete, url, nil, user, pass)
	if err != nil {
		return err
	}
	if statusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
}

func doRequest(method, url string, body []byte, user, pass string) (int, []byte, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if user != "" {
		req.SetBasicAuth(user, pass)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}
