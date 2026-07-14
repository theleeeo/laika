package tests

import (
	"context"
	"fmt"

	"github.com/theleeeo/laika/core"
)

// drainer waits until the indexer's inline build pool has fully settled —
// including cascaded parent builds and drift re-builds.
type drainer struct {
	idx *core.Indexer
}

func (d *drainer) Drain(ctx context.Context) {
	if err := d.idx.WaitForIdle(ctx); err != nil {
		panic(fmt.Errorf("wait for idle: %w", err))
	}
}
