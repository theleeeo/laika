// Command cutover-check runs the read-only readiness gates for a cutover —
// a readVersion bump in the resource config (ADR 0009). Run it against the
// live infrastructure with the *proposed* config file before deploying it:
// exit 0 means every resource's target index exists, its doc count agrees
// with the current read index within -count-tolerance (the backfill
// finished; where one version's plan legitimately excludes resources the
// other's includes, the counts differ by design and need a tolerance that
// covers the gap, ADR 0013),
// and the type has no aged stale backlog.
//
// The deployed indexer converges the alias unconditionally at startup, so
// this check is the last gate before the change takes effect. Rerunning it
// with the deployed config doubles as a post-cutover soak check.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/theleeeo/laika/app/config"
	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/storage/postgres"
)

func main() {
	configPath := flag.String("config", "resources.yml", "Path to the resource config file carrying the proposed readVersion")
	index := flag.String("index", "", "Resource name to check (e.g. \"a\"); omit for all")
	esAddr := flag.String("es-addr", "http://localhost:9200", "Elasticsearch address")
	esUser := flag.String("es-user", "", "Elasticsearch username")
	esPass := flag.String("es-pass", "", "Elasticsearch password")
	pgAddr := flag.String("pg-addr", "", "Postgres connection string of the relation store (required — the stale-backlog gate reads it)")
	tolerance := flag.Int64("count-tolerance", 0, "Absolute doc-count difference the parity gate tolerates")
	maxStaleAge := flag.Duration("max-stale-age", core.DefaultMaxStaleAge, "Fail when any resource of the type has been stale longer than this")
	asJSON := flag.Bool("json", false, "Emit the report as JSON instead of a human-readable summary")
	flag.Parse()

	if *pgAddr == "" {
		log.Fatal("-pg-addr is required: the stale-backlog gate reads the Postgres relation store")
	}

	resources, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load resource config: %v", err)
	}
	if err := resources.Validate(); err != nil {
		log.Fatalf("invalid resource config: %v", err)
	}
	if *index != "" {
		cfg := resources.Get(*index)
		if cfg == nil {
			log.Fatalf("unknown resource %q", *index)
		}
		resources = resource.Configs{cfg}
	}

	client, err := elasticsearch.Dial([]string{*esAddr}, *esUser, *esPass)
	if err != nil {
		log.Fatalf("connect to elasticsearch: %v", err)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *pgAddr)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	results := core.CheckCutoverReadiness(ctx, client, postgres.NewStore(pool), resources,
		core.ReadinessOptions{CountTolerance: *tolerance, MaxStaleAge: *maxStaleAge})

	var ready bool
	if *asJSON {
		ready = true
		for _, r := range results {
			ready = ready && r.Ready
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(results); err != nil {
			log.Fatalf("encode report: %v", err)
		}
	} else {
		ready = renderReport(os.Stdout, results)
	}

	if !ready {
		os.Exit(1)
	}
}

// renderReport writes a human-readable readiness report and reports whether
// every resource is ready.
func renderReport(w io.Writer, results []core.ResourceReadiness) bool {
	allReady := true
	for _, r := range results {
		verdict := "READY"
		if !r.Ready {
			verdict = "NOT READY"
			allReady = false
		}
		fmt.Fprintf(w, "%s (%s): %s\n", r.Resource, transition(r), verdict)
		for _, c := range r.Checks {
			mark := "ok  "
			if !c.OK {
				mark = "FAIL"
			}
			fmt.Fprintf(w, "  %s %-17s %s\n", mark, c.Name, c.Detail)
		}
	}
	return allReady
}

// transition renders where the read alias is and where the config wants it.
func transition(r core.ResourceReadiness) string {
	switch r.CurrentIndex {
	case "":
		return "(no alias) -> " + r.TargetIndex
	case r.TargetIndex:
		return r.CurrentIndex
	default:
		return r.CurrentIndex + " -> " + r.TargetIndex
	}
}
