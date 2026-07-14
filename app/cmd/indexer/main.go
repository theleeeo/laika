package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/theleeeo/laika/app/config"
	"github.com/theleeeo/laika/app/dsl"
	"github.com/theleeeo/laika/app/gen/index/v1/indexconnect"
	"github.com/theleeeo/laika/app/gen/search/v1/searchconnect"
	"github.com/theleeeo/laika/app/server"
	"github.com/theleeeo/laika/app/source"
	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/storage/postgres"

	"connectrpc.com/grpcreflect"
	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
)

func main() {
	appConfigPath := os.Getenv("APP_CONFIG_PATH")
	if appConfigPath == "" {
		appConfigPath = "indexer.yml"
	}

	cfg, err := loadAppConfig(appConfigPath)
	if err != nil {
		log.Fatalf("load app config: %v", err)
	}

	resources, err := config.LoadConfig(cfg.ResourceConfigPath)

	if err != nil {
		log.Fatalf("load resource config: %v", err)
	}

	if err := resources.Validate(); err != nil {
		log.Fatalf("invalid resource config: %v", err)
	}

	log.Printf("loaded %d resource configurations", len(resources))
	for _, rc := range resources {
		vc := rc.ReadVersionConfig()
		log.Printf(" - resource %q with %d field/s and %d relation/s", rc.Resource, len(vc.Fields), len(vc.Relations))
	}

	esClient, err := esv8.NewClient(esv8.Config{
		Addresses: cfg.ES.Addrs,
		Username:  cfg.ES.Username,
		Password:  cfg.ES.Password,
	})
	if err != nil {
		log.Fatalf("setting up es client: %v", err)
	}

	esClientImpl := elasticsearch.New(esClient, false)

	dbpool, err := pgxpool.New(context.Background(), cfg.PG.Addr)
	if err != nil {
		log.Fatalf("pgxpool: %v", err)
	}
	defer dbpool.Close()

	logLevel, err := core.ParseLevel(cfg.Log.Level)
	if err != nil {
		log.Fatalf("parse log level: %v", err)
	}
	core.InitTextLogging(os.Stderr, logLevel)

	st := postgres.NewStore(dbpool)

	sourceProvider, err := source.NewGRPCProvider(cfg.Provider.Addr)
	if err != nil {
		log.Fatalf("connect to provider plugin: %v", err)
	}
	defer sourceProvider.Close()

	plans := dsl.BuildPlansFromConfig(sourceProvider, resources)

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.Fatalf("temporal dial: %v", err)
	}
	defer temporalClient.Close()

	idx := core.New(core.Config{
		Plans:      plans,
		Resources:  resources,
		ES:         esClientImpl,
		Store:      st,
		Temporal:   temporalClient,
		TaskQueue:  cfg.Temporal.TaskQueue,
		PoolSize:   cfg.Pool.Size,
		SubmitWait: cfg.Pool.SubmitWait,
	})

	w := idx.NewWorker()
	if err := w.Start(); err != nil {
		log.Fatalf("temporal worker start: %v", err)
	}

	if err := idx.EnsureSweepSchedule(context.Background(), cfg.Sweep.Interval, core.SweepParams{
		Threshold: cfg.Sweep.Threshold,
		BatchSize: cfg.Sweep.BatchSize,
	}); err != nil {
		log.Fatalf("ensure sweep schedule: %v", err)
	}

	idxSrv := server.NewIndexer(idx)
	searchSrv := server.NewSearcher(idx)

	// The surface is split across two listeners: a public port for the
	// read/search surface (browser-facing, CORS) and an admin port for the
	// write/control surface (IndexService). Both serve gRPC, gRPC-Web, and the
	// Connect protocol (HTTP POST + JSON) from the same handlers.
	publicMux, adminMux := newServeMuxes(idxSrv, searchSrv)

	// Enable unencrypted (h2c) HTTP/2 so gRPC clients can speak HTTP/2 over
	// cleartext on the same port, while HTTP/1.1 stays available for Connect/JSON.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	// Public port is CORS-wrapped for browsers; the admin port stays bare so the
	// write API is never reachable from the CORS-open browser surface.
	publicSrv := &http.Server{
		Addr:      cfg.GRPC.PublicAddr,
		Handler:   withCORS(publicMux),
		Protocols: protocols,
	}
	adminSrv := &http.Server{
		Addr:      cfg.GRPC.AdminAddr,
		Handler:   adminMux,
		Protocols: protocols,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt)

	wg := sync.WaitGroup{}

	wg.Go(func() {
		log.Printf("public HTTP server (search) listening on %s", cfg.GRPC.PublicAddr)
		if err := publicSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("public HTTP server error: %v", err)
		}
		log.Printf("public HTTP server stopped")
	})

	wg.Go(func() {
		log.Printf("admin HTTP server (writes) listening on %s", cfg.GRPC.AdminAddr)
		if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("admin HTTP server error: %v", err)
		}
		log.Printf("admin HTTP server stopped")
	})

	<-stopChan
	log.Printf("shutting down")

	go func() {
		<-stopChan
		log.Printf("force shutdown")
		os.Exit(1)
	}()

	// Ask the HTTP servers to stop gracefully; cancelling ctx would force an
	// immediate stop.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()

	// Stop accepting new requests first, then drain in-flight inline builds.
	if err := publicSrv.Shutdown(stopCtx); err != nil {
		log.Printf("public HTTP server shutdown error: %v", err)
	}
	if err := adminSrv.Shutdown(stopCtx); err != nil {
		log.Printf("admin HTTP server shutdown error: %v", err)
	}

	// Drain in-flight inline builds; anything unfinished stays stale and is
	// recovered by the sweep.
	if err := idx.Shutdown(stopCtx); err != nil {
		log.Printf("indexer drain: %v", err)
	}
	w.Stop()

	wg.Wait()
}

// newServeMuxes builds the two HTTP muxes for the split surface: the public
// mux serves the read/search surface, the admin mux serves the write/control
// surface (indexing + rebuild). gRPC reflection is served on both, scoped so
// each port advertises only the services it actually hosts.
func newServeMuxes(idxSrv indexconnect.IndexServiceHandler, searchSrv searchconnect.SearchServiceHandler) (public, admin *http.ServeMux) {
	public = http.NewServeMux()
	public.Handle(searchconnect.NewSearchServiceHandler(searchSrv))
	publicReflector := grpcreflect.NewStaticReflector(searchconnect.SearchServiceName)
	public.Handle(grpcreflect.NewHandlerV1(publicReflector))
	public.Handle(grpcreflect.NewHandlerV1Alpha(publicReflector))

	admin = http.NewServeMux()
	admin.Handle(indexconnect.NewIndexServiceHandler(idxSrv))
	adminReflector := grpcreflect.NewStaticReflector(indexconnect.IndexServiceName)
	admin.Handle(grpcreflect.NewHandlerV1(adminReflector))
	admin.Handle(grpcreflect.NewHandlerV1Alpha(adminReflector))

	return public, admin
}

// withCORS wraps h with permissive CORS headers so the standalone demo page,
// loaded from a different origin (e.g. file:// or another host), can call the
// Connect/JSON endpoints from a browser. The demo issues plain JSON POSTs, so
// allowing the Connect protocol headers and answering preflight is enough; we
// deliberately allow any origin because this is a development/demo surface.
func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}
