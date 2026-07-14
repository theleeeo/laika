package core

import (
	"context"
	"fmt"
)

// ResourceSelector identifies a set of resources and versions to rebuild.
type ResourceSelector struct {
	ResourceType string
	Versions     []int
	ResourceIDs  []string
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
		}); err != nil {
			return fmt.Errorf("rebuild %s: %w", sel.ResourceType, err)
		}
	}
	return nil
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
