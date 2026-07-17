package core

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
)

// ResourceSelector identifies a set of resources and versions to rebuild.
type ResourceSelector struct {
	ResourceType string
	Versions     []int
	ResourceIDs  []string
	// Metadata is forwarded to every provider call the rebuild makes, exactly
	// like notification metadata on the ingest path (e.g. the acting tenant).
	Metadata map[string]string
}

// RebuildNow synchronously rebuilds the selected resources in-process. It is
// the body of the RebuildWalk activity, and is exported for embedders and
// tests that want rebuild semantics without Temporal.
func (idx *Indexer) RebuildNow(ctx context.Context, selectors []ResourceSelector) error {
	if err := idx.validateSelectors(selectors); err != nil {
		return err
	}
	for _, sel := range selectors {
		if err := idx.rebuild(ctx, RebuildArgs{
			ResourceType: sel.ResourceType,
			Versions:     sel.Versions,
			ResourceIDs:  sel.ResourceIDs,
			Metadata:     sel.Metadata,
		}); err != nil {
			return fmt.Errorf("rebuild %s: %w", sel.ResourceType, err)
		}
	}
	return nil
}

// Rebuild validates the selectors and starts one durable RebuildWalk workflow
// per selector, returning the workflow IDs (inspect/retry/cancel in the
// Temporal UI).
func (idx *Indexer) Rebuild(ctx context.Context, selectors []ResourceSelector) ([]string, error) {
	if err := idx.validateSelectors(selectors); err != nil {
		return nil, err
	}
	workflowIDs := make([]string, 0, len(selectors))
	for _, sel := range selectors {
		run, err := idx.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: idx.taskQueue,
		}, rebuildWalkWorkflowName, sel)
		if err != nil {
			return workflowIDs, fmt.Errorf("start rebuild workflow for %s: %w", sel.ResourceType, err)
		}
		workflowIDs = append(workflowIDs, run.GetID())
	}
	return workflowIDs, nil
}

func (idx *Indexer) validateSelectors(selectors []ResourceSelector) error {
	if len(selectors) == 0 {
		return &InvalidArgumentError{Msg: "at least one selector is required"}
	}
	for _, sel := range selectors {
		cfg := idx.resources.Get(sel.ResourceType)
		if cfg == nil {
			return fmt.Errorf("resource type %q: %w", sel.ResourceType, ErrUnknownResource)
		}
		for _, v := range sel.Versions {
			if cfg.GetVersion(v) == nil {
				return &InvalidArgumentError{Msg: fmt.Sprintf("resource %q has no version %d", sel.ResourceType, v)}
			}
		}
	}
	return nil
}
