// Command diff-mapping reports how the Elasticsearch mapping of each running
// index differs from the mapping the current resource config would generate.
//
// It is the read-only counterpart to gen-mapping: gen-mapping writes the config's
// mapping into the cluster, diff-mapping tells you what writing it would change.
// A non-zero exit means there is actionable drift — a field the config adds or
// changes, or an index that does not exist yet — so it works as a pre-deploy or
// CI check. Fields present in the index but dropped from config are reported but
// do not affect the exit code (Elasticsearch never removes them on its own).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"sort"

	"github.com/theleeeo/laika/app/config"
	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
)

func main() {
	configPath := flag.String("config", "resources.yml", "Path to resource config file")
	index := flag.String("index", "", "Resource name to diff (e.g. \"a\"); omit for all")
	esAddr := flag.String("es-addr", "http://localhost:9200", "Elasticsearch address")
	esUser := flag.String("es-user", "", "Elasticsearch username")
	esPass := flag.String("es-pass", "", "Elasticsearch password")
	asJSON := flag.Bool("json", false, "Emit the diff as JSON instead of a human-readable summary")
	flag.Parse()

	resources, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load resource config: %v", err)
	}
	if err := resources.Validate(); err != nil {
		log.Fatalf("invalid resource config: %v", err)
	}

	// Build the set of expected mappings, keyed by versioned index name — the
	// same keying gen-mapping uses.
	expected := map[string]map[string]any{}
	if *index != "" {
		cfg := resources.Get(*index)
		if cfg == nil {
			log.Fatalf("unknown resource %q", *index)
		}
		for _, vc := range cfg.Versions {
			expected[core.IndexName(cfg.Resource, vc.Version)] = elasticsearch.GenerateMapping(&vc)
		}
	} else {
		expected = elasticsearch.GenerateMappings(resources)
	}

	client, err := elasticsearch.Dial([]string{*esAddr}, *esUser, *esPass)
	if err != nil {
		log.Fatalf("connect to elasticsearch: %v", err)
	}

	diffs, err := diffAll(context.Background(), client, expected)
	if err != nil {
		log.Fatalf("diff mappings: %v", err)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(diffs); err != nil {
			log.Fatalf("encode diff: %v", err)
		}
	} else {
		elasticsearch.ReportDiffs(os.Stdout, diffs)
	}

	if elasticsearch.HasDrift(diffs) {
		os.Exit(1)
	}
}

// diffAll fetches each expected index's running mapping and diffs it, returning
// results in a stable (index-name) order.
func diffAll(ctx context.Context, client *elasticsearch.Client, expected map[string]map[string]any) ([]elasticsearch.IndexDiff, error) {
	names := make([]string, 0, len(expected))
	for name := range expected {
		names = append(names, name)
	}
	sort.Strings(names)

	diffs := make([]elasticsearch.IndexDiff, 0, len(names))
	for _, name := range names {
		actual, exists, err := client.GetMapping(ctx, name)
		if err != nil {
			return nil, err
		}
		d := elasticsearch.IndexDiff{Index: name, Exists: exists}
		if exists {
			d.Diff = elasticsearch.DiffMapping(expected[name], actual)
		}
		diffs = append(diffs, d)
	}
	return diffs, nil
}
