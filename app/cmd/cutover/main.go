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
	"github.com/theleeeo/indexer/core"
)

func main() {
	configPath := flag.String("config", "resources.yml", "Path to resource config file")
	resourceName := flag.String("resource", "", "Resource name to cut over (required)")
	version := flag.Int("version", 0, "Target version to point the alias to (required)")
	esAddr := flag.String("es-addr", "http://localhost:9200", "Elasticsearch address")
	esUser := flag.String("es-user", "", "Elasticsearch username")
	esPass := flag.String("es-pass", "", "Elasticsearch password")
	flag.Parse()

	if *resourceName == "" || *version == 0 {
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

	found := false
	for _, v := range cfg.SortedVersions() {
		if v == *version {
			found = true
			break
		}
	}
	if !found {
		log.Fatalf("version %d is not in the active versions %v for resource %q", *version, cfg.SortedVersions(), *resourceName)
	}

	aliasName := core.AliasName(cfg.Resource)
	targetIndex := core.IndexName(cfg.Resource, *version)
	addr := strings.TrimRight(*esAddr, "/")

	currentIndex, err := getAliasTarget(addr, aliasName, *esUser, *esPass)
	if err != nil {
		log.Printf("warning: could not determine current alias target: %v", err)
	}

	if currentIndex == targetIndex {
		log.Printf("alias %s already points to %s, nothing to do", aliasName, targetIndex)
		return
	}

	if err := switchAlias(addr, aliasName, targetIndex, *esUser, *esPass); err != nil {
		log.Fatalf("switch alias: %v", err)
	}

	if currentIndex != "" {
		log.Printf("switched alias %s: %s -> %s", aliasName, currentIndex, targetIndex)
	} else {
		log.Printf("set alias %s -> %s", aliasName, targetIndex)
	}
}

func switchAlias(addr, aliasName, targetIndex, user, pass string) error {
	body := map[string]any{
		"actions": []any{
			map[string]any{
				"remove": map[string]any{
					"index": "*",
					"alias": aliasName,
				},
			},
			map[string]any{
				"add": map[string]any{
					"index": targetIndex,
					"alias": aliasName,
				},
			},
		},
	}

	b, err := json.Marshal(body)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/_aliases", addr)
	statusCode, respBody, err := doRequest(http.MethodPost, url, b, user, pass)
	if err != nil {
		return err
	}
	if statusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
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
