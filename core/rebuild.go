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

// Rebuild validates the selectors and enqueues a "rebuild" job per selector.
func (idx *Indexer) Rebuild(ctx context.Context, selectors []ResourceSelector) error {
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

	for _, sel := range selectors {
		if _, err := idx.river.Insert(ctx, RebuildArgs{
			ResourceType: sel.ResourceType,
			Versions:     sel.Versions,
			ResourceIDs:  sel.ResourceIDs,
			// TODO: Should we allow passing metadata for rebuilds?
			Metadata: nil,
		}, nil); err != nil {
			return fmt.Errorf("enqueue rebuild for %s: %w", sel.ResourceType, err)
		}
	}

	return nil
}
