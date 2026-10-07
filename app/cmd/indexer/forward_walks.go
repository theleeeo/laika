package main

import (
	"context"
	"fmt"
	"maps"

	"github.com/theleeeo/laika/core"
)

// coreForwardWalks translates the forward_walks entries into
// core.Config.ForwardWalks: an enabled entry becomes its type's walk, a
// disabled one is left out, and zero values pass through for core to default.
// A type with two entries is an error. Core validates the rest (an unknown
// type, a negative value).
func coreForwardWalks(walks []forwardWalkConfig) (map[string]core.ForwardWalkConfig, error) {
	seen := make(map[string]bool, len(walks))
	var out map[string]core.ForwardWalkConfig
	for _, w := range walks {
		if seen[w.ResourceType] {
			return nil, fmt.Errorf("forward_walks: resource type %q has more than one entry", w.ResourceType)
		}
		seen[w.ResourceType] = true
		if !w.Enabled {
			continue
		}
		if out == nil {
			out = make(map[string]core.ForwardWalkConfig)
		}
		out[w.ResourceType] = core.ForwardWalkConfig{
			Interval:     w.Interval,
			PageSize:     w.PageSize,
			PageInterval: w.PageInterval,
			Metadata:     staticMetadata(w.Metadata),
		}
	}
	return out, nil
}

// staticMetadata returns a core.ForwardWalkConfig.Metadata func that hands
// each run its own copy of the configured maps, or nil — one walk with no
// metadata — when there are none.
func staticMetadata(configured []map[string]string) func(context.Context) ([]map[string]string, error) {
	if len(configured) == 0 {
		return nil
	}
	return func(context.Context) ([]map[string]string, error) {
		out := make([]map[string]string, len(configured))
		for i, m := range configured {
			out[i] = maps.Clone(m)
		}
		return out, nil
	}
}
