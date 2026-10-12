package core

import (
	"fmt"

	"github.com/theleeeo/laika/core/resource"
)

// setBuildBatchSizes applies Config.BuildBatchSize's default and keeps
// BuildBatchSizes' non-zero entries, refusing a negative size and an entry
// for a type resources doesn't configure.
func (idx *Indexer) setBuildBatchSizes(size int, sizes map[string]int, resources resource.Configs) error {
	if size < 0 {
		return fmt.Errorf("build batch size %d is negative", size)
	}
	if size == 0 {
		size = defaultBuildBatchSize
	}
	idx.buildBatchSize = size
	idx.buildBatchSizes = make(map[string]int, len(sizes))
	for typ, n := range sizes {
		if resources.Get(typ) == nil {
			return fmt.Errorf("build batch size for resource type %q: %w", typ, ErrUnknownResource)
		}
		if n < 0 {
			return fmt.Errorf("build batch size %d for resource type %q is negative", n, typ)
		}
		if n > 0 {
			idx.buildBatchSizes[typ] = n
		}
	}
	return nil
}

// batchSize is how many ids a build of resourceType asks a plan about at
// once: its BuildBatchSizes entry, else BuildBatchSize.
func (idx *Indexer) batchSize(resourceType string) int {
	if n, ok := idx.buildBatchSizes[resourceType]; ok {
		return n
	}
	return idx.buildBatchSize
}

// idBatches cuts ids into batches of at most size, in order, each id in the
// first batch it appears in only: a request's ids are distinct, and
// BeginBuilds fails a call that gives one resource two tokens.
func idBatches(ids []string, size int) [][]string {
	seen := make(map[string]bool, len(ids))
	var out [][]string
	var cur []string
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		cur = append(cur, id)
		if len(cur) == size {
			out = append(out, cur)
			cur = nil
		}
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}
