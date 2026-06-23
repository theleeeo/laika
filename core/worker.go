package core

import (
	"context"

	"github.com/riverqueue/river"
)

type BuildArgs struct {
	ResourceType string            `json:"resource_type"`
	ResourceIds  []string          `json:"resource_ids,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func (BuildArgs) Kind() string { return "build" }

type BuildWorker struct {
	river.WorkerDefaults[BuildArgs]
	Idx *Indexer
}

func (w *BuildWorker) Work(ctx context.Context, job *river.Job[BuildArgs]) error {
	return w.Idx.Build(ctx, job.Args)
}

type RebuildArgs struct {
	ResourceType string            `json:"resource_type"`
	Versions     []int             `json:"versions"`
	ResourceIDs  []string          `json:"resource_ids,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func (RebuildArgs) Kind() string { return "rebuild" }

type RebuildWorker struct {
	river.WorkerDefaults[RebuildArgs]
	Idx *Indexer
}

func (w *RebuildWorker) Work(ctx context.Context, job *river.Job[RebuildArgs]) error {
	return w.Idx.rebuild(ctx, job.Args)
}

type DeleteArgs struct {
	ResourceType string            `json:"resource_type"`
	ResourceID   string            `json:"resource_id"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func (DeleteArgs) Kind() string { return "delete" }

type DeleteWorker struct {
	river.WorkerDefaults[DeleteArgs]
	Idx *Indexer
}

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[DeleteArgs]) error {
	return w.Idx.handleDelete(ctx, RebuildPayload{
		ResourceType: job.Args.ResourceType,
		ResourceID:   job.Args.ResourceID,
	})
}

func RegisterWorkers(workers *river.Workers, idx *Indexer) {
	river.AddWorker(workers, &BuildWorker{Idx: idx})
	river.AddWorker(workers, &RebuildWorker{Idx: idx})
	river.AddWorker(workers, &DeleteWorker{Idx: idx})
}
