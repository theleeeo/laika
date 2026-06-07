package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/theleeeo/indexer/app/config"
	"github.com/theleeeo/indexer/app/dsl"
	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/gen/index/v1"
	"github.com/theleeeo/indexer/gen/search/v1"
	"github.com/theleeeo/indexer/server"
	"github.com/theleeeo/indexer/source"
	"github.com/theleeeo/indexer/storage/postgres"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.ES.Addrs,
		Username:  cfg.ES.Username,
		Password:  cfg.ES.Password,
	})
	if err != nil {
		log.Fatalf("setting up es client: %v", err)
	}

	esClientImpl := es.New(esClient, false)

	dbpool, err := pgxpool.New(context.Background(), cfg.PG.Addr)
	if err != nil {
		log.Fatalf("pgxpool: %v", err)
	}
	defer dbpool.Close()

	slog.SetLogLoggerLevel(slog.LevelDebug)

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

	lis, err := net.Listen("tcp", cfg.GRPC.Addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	g := grpc.NewServer()
	index.RegisterIndexServiceServer(g, idxSrv)
	search.RegisterSearchServiceServer(g, searchSrv)
	reflection.Register(g)

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
		log.Printf("gRPC server listening on %s", cfg.GRPC.Addr)
		if err := g.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
		log.Printf("gRPC server stopped")
	})

	<-stopChan
	log.Printf("shutting down")

	go func() {
		<-stopChan
		log.Printf("force shutdown")
		os.Exit(1)
	}()

	// Ask River to stop gracefully; cancelling ctx would force an immediate stop.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := riverClient.Stop(stopCtx); err != nil {
		log.Printf("river client stop error: %v", err)
	}
	stopCancel()

	cancel()

	g.GracefulStop()

	wg.Wait()
}
