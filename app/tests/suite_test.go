package tests

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/indexer/app/dsl"
	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/core/resource"
	"github.com/theleeeo/indexer/app/source"
	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/storage/postgres"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	esContainer "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	pgContainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// FakeProvider is a test source.Provider that serves data from in-memory maps.
type FakeProvider struct {
	mu         sync.Mutex
	resources  map[string]map[string]any           // "type|id" -> data
	relations  map[string][]source.RelatedResource // "type|key" -> []RelatedResource
	fetchGates map[string]*fetchGate
}

type fetchGate struct {
	reached chan struct{}
	release chan struct{}
}

// ListResources implements [source.Provider].
func (f *FakeProvider) ListResources(ctx context.Context, params source.ListResourcesParams) (source.ListResourcesResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var resources []source.ListedResource
	prefix := params.ResourceType + "|"
	for key, data := range f.resources {
		if strings.HasPrefix(key, prefix) {
			parts := strings.SplitN(key, "|", 2)
			if len(parts) != 2 {
				continue
			}
			resources = append(resources, source.ListedResource{
				ID:   parts[1],
				Data: data,
			})
		}
	}

	return source.ListResourcesResult{
		Resources:     resources,
		NextPageToken: "", // Pagination not implemented in this fake
	}, nil
}

func NewFakeProvider() *FakeProvider {
	return &FakeProvider{
		resources:  make(map[string]map[string]any),
		relations:  make(map[string][]source.RelatedResource),
		fetchGates: make(map[string]*fetchGate),
	}
}

func (f *FakeProvider) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for token, gate := range f.fetchGates {
		close(gate.release)
		delete(f.fetchGates, token)
	}
	f.resources = make(map[string]map[string]any)
	f.relations = make(map[string][]source.RelatedResource)
}

// SetFetchGate blocks matching FetchResource calls until ReleaseFetchGate is
// called for the same token. It returns a channel that is closed when the
// first matching fetch reaches the gate.
func (f *FakeProvider) SetFetchGate(token string) <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	gate := &fetchGate{
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
	f.fetchGates[token] = gate

	return gate.reached
}

func (f *FakeProvider) ReleaseFetchGate(token string) {
	f.mu.Lock()
	gate, ok := f.fetchGates[token]
	if ok {
		delete(f.fetchGates, token)
	}
	f.mu.Unlock()

	if ok {
		close(gate.release)
	}
}

func (f *FakeProvider) SetResource(resourceType, resourceID string, data map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resources[resourceType+"|"+resourceID] = data
}

func (f *FakeProvider) DeleteResource(resourceType, resourceID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.resources, resourceType+"|"+resourceID)
}

// TODO: Remove
func (f *FakeProvider) SetRelated(resourceType string, keyValues []string, related []map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := resourceType
	for _, v := range keyValues {
		key += "|" + v
	}
	rr := make([]source.RelatedResource, len(related))
	for i, d := range related {
		id, _ := d["id"].(string)
		rr[i] = source.RelatedResource{ID: id, Data: d}
	}
	f.relations[key] = rr
}

// SetRelatedVersioned stores related resources with per-item version data.
func (f *FakeProvider) SetRelatedVersioned(resourceType string, keyValues []string, related []source.RelatedResource) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := resourceType
	for _, v := range keyValues {
		key += "|" + v
	}
	rr := make([]source.RelatedResource, len(related))
	for i, d := range related {
		cloned := make(map[string]any, len(d.Data))
		for k, v := range d.Data {
			cloned[k] = v
		}
		rr[i] = source.RelatedResource{ID: d.ID, Data: cloned, Version: d.Version}
	}
	f.relations[key] = rr
}

func (f *FakeProvider) FetchResource(_ context.Context, params source.FetchResourceParams) (source.FetchResourceResult, error) {
	if token := params.Metadata["test_fetch_gate"]; token != "" {
		gateResourceType := params.Metadata["test_fetch_gate_resource"]
		if gateResourceType == "" || gateResourceType == params.ResourceType {
			f.mu.Lock()
			gate := f.fetchGates[token]
			f.mu.Unlock()

			if gate != nil {
				select {
				case <-gate.reached:
				default:
					close(gate.reached)
				}
				<-gate.release
			}
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.resources[params.ResourceType+"|"+params.ResourceID]
	if !ok {
		return source.FetchResourceResult{}, nil
	}

	cloned := make(map[string]any, len(data))
	for k, v := range data {
		cloned[k] = v
	}

	if overrideField1 := params.Metadata["test_override_field1"]; overrideField1 != "" {
		cloned["field1"] = overrideField1
	}
	if overrideF1 := params.Metadata["test_override_f1"]; overrideF1 != "" {
		cloned["f1"] = overrideF1
	}

	return source.FetchResourceResult{Data: cloned}, nil
}

func (f *FakeProvider) FetchRelated(_ context.Context, params source.FetchRelatedParams) (source.FetchRelatedResult, error) {
	// Snapshot the relation data and resolve a possible gate under the lock,
	// then release it before blocking on the gate. Snapshot semantics let
	// tests advance the source data (and resources.version) while the call
	// is paused — modelling a build that fetched stale child data.
	f.mu.Lock()
	key := params.ResourceType + "|" + params.Key.Value
	data, ok := f.relations[key]
	var snapshot []source.RelatedResource
	if ok {
		snapshot = make([]source.RelatedResource, len(data))
		for i, d := range data {
			cloned := make(map[string]any, len(d.Data))
			for k, v := range d.Data {
				cloned[k] = v
			}
			snapshot[i] = source.RelatedResource{ID: d.ID, Data: cloned, Version: d.Version}
		}
	}
	var gate *fetchGate
	if token := params.Metadata["test_related_gate"]; token != "" {
		gateResource := params.Metadata["test_related_gate_resource"]
		if gateResource == "" || gateResource == params.ResourceType {
			gate = f.fetchGates[token]
		}
	}
	f.mu.Unlock()

	if gate != nil {
		select {
		case <-gate.reached:
		default:
			close(gate.reached)
		}
		<-gate.release
	}

	if !ok {
		return source.FetchRelatedResult{}, nil
	}
	return source.FetchRelatedResult{Related: snapshot}, nil
}

type TestSuite struct {
	suite.Suite

	esContainer *esContainer.ElasticsearchContainer
	pgContainer *pgContainer.PostgresContainer

	pool *pgxpool.Pool

	esClient *elasticsearch.Client

	idx *core.Indexer

	cancelWorker context.CancelFunc
	worker       *riverDrainer

	fakeProvider *FakeProvider
	st           *postgres.Store
}

var DefaultResourceConfig = resource.Configs{
	{
		Resource: "a",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields: []resource.FieldConfig{
					{Name: "field1"},
					{Name: "field2"},
				},
				Relations: []resource.RelationConfig{
					{
						Resource: "b",
						Key:      resource.KeyConfig{Source: "a", Field: "id"},
						Fields: []resource.FieldConfig{
							{Name: "field1"},
							{Name: "field2"},
						},
					},
				},
			},
		},
	},
	{
		Resource: "b",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields: []resource.FieldConfig{
					{Name: "field1"},
					{Name: "field2"},
				},
				Relations: []resource.RelationConfig{},
			},
		},
	},
}

var RelatedResourceConfig = resource.Configs{
	{
		Resource: "a",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields: []resource.FieldConfig{
					{Name: "f1"},
				},
				Relations: []resource.RelationConfig{
					{
						Resource: "b",
						Key:      resource.KeyConfig{Source: "a", Field: "id"},
						Fields:   []resource.FieldConfig{{Name: "f1"}},
					},
				},
			},
		},
	},
	{
		Resource: "b",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields: []resource.FieldConfig{
					{Name: "f1"},
				},
				Relations: []resource.RelationConfig{
					{
						Resource: "a",
						Key:      resource.KeyConfig{Source: "b", Field: "id"},
						Fields:   []resource.FieldConfig{{Name: "f1"}},
					},
				},
			},
		},
	},
	{
		Resource: "c",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields: []resource.FieldConfig{
					{Name: "f1"},
				},
				Relations: []resource.RelationConfig{
					{
						Resource: "a",
						Key:      resource.KeyConfig{Source: "c", Field: "id"},
						Fields:   []resource.FieldConfig{{Name: "f1"}},
					},
					{
						Resource: "b",
						Key:      resource.KeyConfig{Source: "c", Field: "id"},
						Fields:   []resource.FieldConfig{{Name: "f1"}},
					},
				},
			},
		},
	},
}

func (t *TestSuite) verifyResourceConfigs() {
	for _, c := range DefaultResourceConfig {
		c.ApplyDefaults()
	}
	for _, c := range RelatedResourceConfig {
		c.ApplyDefaults()
	}
	must(t.T(), DefaultResourceConfig.Validate())
	must(t.T(), RelatedResourceConfig.Validate())
}

func must(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func (t *TestSuite) SetupSuite() {
	log.SetOutput(os.Stderr)
	t.T().Log("setting up the suite")

	t.verifyResourceConfigs()

	wg := sync.WaitGroup{}
	containerCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	wg.Go(func() {
		elasticsearchContainer, err := esContainer.Run(containerCtx, "docker.elastic.co/elasticsearch/elasticsearch:8.9.0")
		if err != nil {
			t.FailNow("failed to start elasticsearch container", err)
		}
		t.esContainer = elasticsearchContainer
	})

	var (
		pgDB   = "indexer"
		pgUser = "user"
		pgPass = "pass"
	)

	wg.Go(func() {
		postgresContainer, err := pgContainer.Run(containerCtx,
			"postgres:17",
			pgContainer.WithDatabase(pgDB),
			pgContainer.WithUsername(pgUser),
			pgContainer.WithPassword(pgPass),
			pgContainer.BasicWaitStrategies(),
		)
		if err != nil {
			t.FailNow("failed to start postgres container", err)
		}
		t.pgContainer = postgresContainer
	})

	wg.Wait()

	esAddr, err := t.esContainer.Endpoint(containerCtx, "https")
	if err != nil {
		t.FailNow("failed to get elasticsearch endpoint", err)
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esAddr},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Username: t.esContainer.Settings.Username,
		Password: t.esContainer.Settings.Password,
	})
	if err != nil {
		log.Fatalf("setting up es client: %v", err)
	}
	t.esClient = esClient

	pgAddr, err := t.pgContainer.Endpoint(containerCtx, "")
	if err != nil {
		t.FailNow("failed to get postgres endpoint", err)
	}
	dbpool, err := pgxpool.New(context.Background(), fmt.Sprintf("postgres://%s:%s@%s/%s", pgUser, pgPass, pgAddr, pgDB))
	if err != nil {
		log.Fatalf("pgxpool: %v", err)
	}
	t.pool = dbpool

	appSchema, err := os.ReadFile(filepath.Join("..", "..", "storage", "postgres", "pg_schema.sql"))
	if err != nil {
		t.T().Fatal(err)
	}

	if _, err := t.pool.Exec(t.T().Context(), string(appSchema)); err != nil {
		t.T().Fatalf("failed to apply schema: %v", err)
	}

	// Apply River migrations.
	riverDriver := riverpgxv5.New(dbpool)
	migrator, err := rivermigrate.New(riverDriver, nil)
	if err != nil {
		t.T().Fatalf("failed to create river migrator: %v", err)
	}
	if _, err := migrator.Migrate(t.T().Context(), rivermigrate.DirectionUp, nil); err != nil {
		t.T().Fatalf("failed to apply river migrations: %v", err)
	}

	t.st = postgres.NewStore(dbpool)
	t.fakeProvider = NewFakeProvider()

	plans := dsl.BuildPlansFromConfig(t.fakeProvider, DefaultResourceConfig)

	t.idx = core.New(core.Config{
		Plans:     plans,
		Resources: DefaultResourceConfig,
		ES:        es.New(esClient, true),
		Store:     t.st,
	})

	workers := river.NewWorkers()
	core.RegisterWorkers(workers, t.idx)

	riverClient, err := river.NewClient(riverDriver, &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		Workers: workers,
	})
	if err != nil {
		t.T().Fatalf("failed to create river client: %v", err)
	}
	t.idx.SetRiverClient(riverClient)

	t.worker = &riverDrainer{client: riverClient, pool: dbpool}

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.cancelWorker = cancelWorker
	if err := riverClient.Start(workerCtx); err != nil {
		t.T().Fatalf("failed to start river client: %v", err)
	}
}

func (t *TestSuite) TearDownSuite() {
	t.cancelWorker()

	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := t.worker.client.Stop(stopCtx); err != nil {
		log.Printf("failed to stop river client: %s", err)
	}

	if err := testcontainers.TerminateContainer(t.esContainer); err != nil {
		log.Printf("failed to terminate elasticsearch container: %s", err)
	}

	t.pool.Close()

	if err := testcontainers.TerminateContainer(t.pgContainer); err != nil {
		log.Printf("failed to terminate postgres container: %s", err)
	}
}

// setResourceConfig rebuilds the aggregation plans from the given resource
// config and updates the indexer's builder. This is the test equivalent of
// dynamically changing the resource configuration at runtime.
// It also creates the versioned ES indices and read aliases.
func (t *TestSuite) setResourceConfig(resources resource.Configs) {
	plans := dsl.BuildPlansFromConfig(t.fakeProvider, resources)
	t.idx.SetPlans(plans, resources)

	// Create versioned indexes and aliases for each resource.
	for _, cfg := range resources {
		for _, vc := range cfg.Versions {
			indexName := core.IndexName(cfg.Resource, vc.Version)
			mapping := es.GenerateMapping(&vc)
			body, err := json.Marshal(mapping)
			t.Require().NoError(err)

			res, err := t.esClient.Indices.Create(
				indexName,
				t.esClient.Indices.Create.WithBody(bytes.NewReader(body)),
			)
			t.Require().NoError(err)
			res.Body.Close()
			// Ignore if already exists (e.g. re-set within same test)
		}

		// Set up read alias.
		aliasName := core.AliasName(cfg.Resource)
		targetIndex := core.IndexName(cfg.Resource, cfg.ReadVersion)

		aliasBody := map[string]any{
			"actions": []any{
				map[string]any{"remove": map[string]any{"index": "*", "alias": aliasName}},
				map[string]any{"add": map[string]any{"index": targetIndex, "alias": aliasName}},
			},
		}
		ab, err := json.Marshal(aliasBody)
		t.Require().NoError(err)
		res, err := t.esClient.Indices.UpdateAliases(bytes.NewReader(ab))
		t.Require().NoError(err)
		res.Body.Close()
	}
}

func (t *TestSuite) BeforeTest(suiteName, testName string) {
}

func (t *TestSuite) AfterTest(suiteName, testName string) {
	// Clear fake provider data between tests.
	t.fakeProvider.Clear()

	// ES 8.x disallows _all / wildcard deletes by default.
	// List concrete index names first, then delete each one.
	catRes, err := t.esClient.Cat.Indices(
		t.esClient.Cat.Indices.WithFormat("json"),
	)
	if err != nil {
		t.T().Fatalf("failed to list indices: %v", err)
	}
	defer catRes.Body.Close()

	var indices []struct {
		Index string `json:"index"`
	}
	if err := json.NewDecoder(catRes.Body).Decode(&indices); err != nil {
		t.T().Fatalf("failed to decode cat indices: %v", err)
	}

	for _, idx := range indices {
		res, err := t.esClient.Indices.Delete([]string{idx.Index})
		if err != nil {
			t.T().Fatalf("failed to delete index %s: %v", idx.Index, err)
		}
		res.Body.Close()
	}

	rows, err := t.pool.Query(t.T().Context(), `
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    `)
	if err != nil {
		t.T().Fatalf("failed to list tables: %v", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.T().Fatalf("failed to scan table name: %v", err)
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		t.T().Fatalf("rows error: %v", err)
	}

	for _, tableName := range tableNames {
		if _, err := t.pool.Exec(t.T().Context(), fmt.Sprintf("TRUNCATE TABLE %s CASCADE", tableName)); err != nil {
			t.T().Fatalf("failed to drop table %s: %v", tableName, err)
		}
	}
}

func Test_TestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}
