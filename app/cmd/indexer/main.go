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
	"github.com/theleeeo/laika/app/webui"
	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/storage/postgres"

	"connectrpc.com/grpcreflect"
	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
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

	// Apply River migrations before starting the client.
	riverDriver := riverpgxv5.New(dbpool)
	migrator, err := rivermigrate.New(riverDriver, nil)
	if err != nil {
		log.Fatalf("river migrator: %v", err)
	}
	if _, err := migrator.Migrate(context.Background(), rivermigrate.DirectionUp, nil); err != nil {
		log.Fatalf("apply river migrations: %v", err)
	}

	sourceProvider, err := source.NewGRPCProvider(cfg.Provider.Addr)
	if err != nil {
		log.Fatalf("connect to provider plugin: %v", err)
	}
	defer sourceProvider.Close()

	plans := dsl.BuildPlansFromConfig(sourceProvider, resources)

	idx := core.New(core.Config{
		Plans:     plans,
		Resources: resources,
		ES:        esClientImpl,
		Store:     st,
	})

	workers := river.NewWorkers()
	core.RegisterWorkers(workers, idx)

	riverClient, err := river.NewClient(riverDriver, &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		Workers: workers,
	})
	if err != nil {
		log.Fatalf("river client: %v", err)
	}
	idx.SetRiverClient(riverClient)

	idxSrv := server.NewIndexer(idx)
	searchSrv := server.NewSearcher(idx)

	// A single Connect-backed HTTP server serves gRPC, gRPC-Web, and the
	// Connect protocol (HTTP POST + JSON) from the same handlers. Existing
	// plain-gRPC clients keep working; web clients get JSON for free.
	mux := http.NewServeMux()
	mux.Handle(indexconnect.NewIndexServiceHandler(idxSrv))
	mux.Handle(searchconnect.NewSearchServiceHandler(searchSrv))

	reflector := grpcreflect.NewStaticReflector(
		indexconnect.IndexServiceName,
		searchconnect.SearchServiceName,
	)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

	// Serve the self-contained demo search console at "/". The service routes
	// above are registered on longer prefixes, so ServeMux keeps them intact.
	mux.Handle("/", webui.Handler())

	// Enable unencrypted (h2c) HTTP/2 so gRPC clients can speak HTTP/2 over
	// cleartext on the same port, while HTTP/1.1 stays available for Connect/JSON.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	httpSrv := &http.Server{
		Addr:      cfg.GRPC.Addr,
		Handler:   mux,
		Protocols: protocols,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt)

	wg := sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Go(func() {
		log.Printf("starting river client")
		if err := riverClient.Start(ctx); err != nil {
			log.Printf("river client start error: %v", err)
			return
		}
		<-riverClient.Stopped()
		log.Printf("river client stopped")
	})

	wg.Go(func() {
		log.Printf("HTTP server (gRPC + gRPC-Web + Connect) listening on %s", cfg.GRPC.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
		log.Printf("HTTP server stopped")
	})

	<-stopChan
	log.Printf("shutting down")

	go func() {
		<-stopChan
		log.Printf("force shutdown")
		os.Exit(1)
	}()

	// Ask River and the HTTP server to stop gracefully; cancelling ctx would
	// force an immediate stop.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()

	// Stop accepting new requests first, then drain River jobs.
	if err := httpSrv.Shutdown(stopCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	if err := riverClient.Stop(stopCtx); err != nil {
		log.Printf("river client stop error: %v", err)
	}

	cancel()

	wg.Wait()
}
