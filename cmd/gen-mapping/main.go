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
	"github.com/theleeeo/indexer/resource"
)

func main() {
	configPath := flag.String("config", "resources.yml", "Path to resource config file")
	index := flag.String("index", "", "Resource name to generate (e.g. \"a\"); omit for all")
	apply := flag.String("apply", "", "Elasticsearch address to apply the mapping to (e.g. http://localhost:9200)")
	esUser := flag.String("es-user", "", "Elasticsearch username")
	esPass := flag.String("es-pass", "", "Elasticsearch password")
	flag.Parse()

	resources, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load resource config: %v", err)
	}
	if err := resources.Validate(); err != nil {
		log.Fatalf("invalid resource config: %v", err)
	}

	// Build the set of mappings to work with.
	mappings := map[string]map[string]any{}
	if *index != "" {
		cfg := resources.Get(*index)
		if cfg == nil {
			log.Fatalf("unknown resource %q", *index)
		}
		for _, vc := range cfg.Versions {
			indexName := es.IndexName(cfg.Resource, vc.Version)
			mappings[indexName] = es.GenerateMapping(&vc)
		}
	} else {
		mappings = es.GenerateMappings(resources)
	}

	if *apply != "" {
		addr := strings.TrimRight(*apply, "/")
		for indexName, mapping := range mappings {
			if err := applyMapping(addr, indexName, mapping, *esUser, *esPass); err != nil {
				log.Fatalf("apply mapping for %s: %v", indexName, err)
			}
			log.Printf("applied mapping to %s", indexName)
		}

		// Set up read aliases: point each alias to the readVersion index.
		targetResources := resources
		if *index != "" {
			targetResources = resource.Configs{resources.Get(*index)}
		}
		for _, cfg := range targetResources {
			aliasName := es.AliasName(cfg.Resource)
			targetIndex := es.IndexName(cfg.Resource, cfg.ReadVersion)
			if err := applyAlias(addr, aliasName, targetIndex, *esUser, *esPass); err != nil {
				log.Fatalf("apply alias %s -> %s: %v", aliasName, targetIndex, err)
			}
			log.Printf("alias %s -> %s", aliasName, targetIndex)
		}
		return
	}

	// Default: print to stdout.
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(mappings); err != nil {
		log.Fatalf("encode mappings: %v", err)
	}
}

// applyMapping PUTs the mapping body to ES. It first tries to create the index;
// if it already exists (409) it falls back to the _mapping endpoint to update.
func applyMapping(addr, indexName string, mapping map[string]any, user, pass string) error {
	body, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	// Try to create the index with the full mapping.
	url := fmt.Sprintf("%s/%s", addr, indexName)
	statusCode, respBody, err := doRequest(http.MethodPut, url, body, user, pass)
	if err != nil {
		return err
	}
	if statusCode == http.StatusOK || statusCode == http.StatusCreated {
		return nil
	}

	// 400 with resource_already_exists_exception — index exists, update the mapping instead.
	if statusCode == http.StatusBadRequest && strings.Contains(string(respBody), "resource_already_exists_exception") {
		mappingBody, err := json.Marshal(mapping["mappings"])
		if err != nil {
			return err
		}
		updateURL := fmt.Sprintf("%s/%s/_mapping", addr, indexName)
		statusCode, respBody, err = doRequest(http.MethodPut, updateURL, mappingBody, user, pass)
		if err != nil {
			return err
		}
		if statusCode == http.StatusOK {
			return nil
		}
	}

	return fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
}

func doRequest(method, url string, body []byte, user, pass string) (int, []byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
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

// applyAlias creates or updates an ES alias to point to the given index using
// the _aliases API. It first removes any existing targets of the alias, then
// adds the new target atomically.
func applyAlias(addr, aliasName, indexName, user, pass string) error {
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
					"index": indexName,
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
