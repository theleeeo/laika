package elasticsearch

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"slices"

	"github.com/theleeeo/laika/core"
)

// federatedFanout executes a Federated Search as one sub-search per Type — a
// single _msearch round trip — merged client-side: the execution swap ADR 0007
// reserved as a documented future experiment.
//
// Each leg is the shared two-tier text query plus that Type's own resolved
// filters against just its read alias: no _index pinning, no cross-Type
// should-groups. MatchNothing groups are skipped outright. Legs run with the
// default query_then_fetch, so scores come from each index's local term
// statistics and cross-Type comparability rests on the standardized search
// fields rather than DFS. The merged ranking orders by score with a
// deterministic (index, id) tie-break — the total order every leg also sorts
// by in ES — so page boundaries are stable. Counts (spec D12) are each leg's
// hits.total, keyed by the group's alias (core resolves aliases as well as
// concrete index names) — under ES's default track_total_hits cap, the same
// looseness the single query's total has: federated totals are tallies for a
// UI, not exact bookkeeping. A missing index empties its own leg only, where
// the single query's 404 empties the whole response.
func (c *Client) federatedFanout(ctx context.Context, p core.FederatedSearchParams) (core.FederatedSearchResult, error) {
	// Every leg must over-fetch the full merged window from 0, because the
	// merge cannot know in advance how the top of the combined ranking
	// distributes across Types; maxPagingWindow bounds each leg as it bounds
	// the single query's from+size.
	from, window, err := pagingWindow(p.Page, p.PageSize)
	if err != nil {
		return core.FederatedSearchResult{}, err
	}

	globalFilters := []any{}
	for _, f := range p.Filters {
		if f.Field == "" {
			continue
		}
		clause, err := buildFilterClause(f)
		if err != nil {
			return core.FederatedSearchResult{}, err
		}
		globalFilters = append(globalFilters, clause)
	}

	result := core.FederatedSearchResult{IndexCounts: map[string]int64{}}

	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf)
	legs := make([]core.IndexFilterGroup, 0, len(p.FilterGroups))
	for _, g := range p.FilterGroups {
		if g.MatchNothing {
			// No document of this Type can match; skip the leg entirely. The
			// zero count keeps the Type present in the result.
			result.IndexCounts[g.Alias] = 0
			continue
		}
		body, err := buildFanoutLegBody(p, g, globalFilters, int(window))
		if err != nil {
			return core.FederatedSearchResult{}, err
		}
		if err := json.MarshalEncode(enc, map[string]any{"index": g.Alias}); err != nil {
			return core.FederatedSearchResult{}, fmt.Errorf("marshal msearch header: %w", err)
		}
		if err := json.MarshalEncode(enc, body); err != nil {
			return core.FederatedSearchResult{}, fmt.Errorf("marshal msearch body: %w", err)
		}
		legs = append(legs, g)
	}

	if len(legs) == 0 {
		return result, nil
	}

	core.LoggerFromContext(ctx).Debug("federated es fanout query", slog.String("body", buf.String()))

	ctx, cancel := context.WithTimeout(ctx, federatedSearchTimeout)
	defer cancel()

	res, err := c.es.Msearch(bytes.NewReader(buf.Bytes()), c.es.Msearch.WithContext(ctx))
	if err != nil {
		return core.FederatedSearchResult{}, err
	}
	defer res.Body.Close()

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return core.FederatedSearchResult{}, fmt.Errorf("es federated fanout error: %s %s", res.Status(), string(raw))
	}

	var decoded struct {
		Responses []map[string]any `json:"responses"`
	}
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return core.FederatedSearchResult{}, err
	}
	if len(decoded.Responses) != len(legs) {
		return core.FederatedSearchResult{}, fmt.Errorf(
			"es federated fanout: %d responses for %d legs", len(decoded.Responses), len(legs))
	}

	var merged []core.FederatedRawHit
	for i, item := range decoded.Responses {
		g := legs[i]
		if errObj, ok := item["error"]; ok {
			// A missing index behaves like the single query's 404: that Type
			// simply has nothing indexed yet. Anything else fails the search.
			if status, _ := item["status"].(float64); int(status) == 404 {
				result.IndexCounts[g.Alias] = 0
				continue
			}
			raw, _ := json.Marshal(errObj)
			return core.FederatedSearchResult{}, fmt.Errorf("es federated fanout leg %q: %s", g.Alias, raw)
		}
		total, hits := decodeFederatedHits(item)
		result.IndexCounts[g.Alias] = total
		result.Total += total
		merged = append(merged, hits...)
	}

	slices.SortFunc(merged, func(a, b core.FederatedRawHit) int {
		if v := cmp.Compare(b.Score, a.Score); v != 0 {
			return v
		}
		if v := cmp.Compare(a.Index, b.Index); v != 0 {
			return v
		}
		return cmp.Compare(a.ID, b.ID)
	})
	if from < int64(len(merged)) {
		result.Hits = merged[from:min(window, int64(len(merged)))]
	}
	return result, nil
}

// buildFanoutLegBody builds one Type's sub-search: the shared two-tier text
// query plus the global filters and this group's own resolved filters. Every
// leg fetches the full merged window from 0 — the merge needs each leg's
// candidates for the requested page. hits.total feeds the per-Type counts
// (D12) at ES's default track_total_hits accuracy; federated search does not
// need exact totals. A leg sorts by federatedSort, the merge's own order, so
// the hits its window cuts off are the ones the merge would rank below it.
func buildFanoutLegBody(p core.FederatedSearchParams, g core.IndexFilterGroup, globalFilters []any, window int) (map[string]any, error) {
	filter := slices.Clone(globalFilters)
	for _, f := range g.Filters {
		if f.Field == "" {
			continue
		}
		clause, err := buildFilterClause(f)
		if err != nil {
			return nil, err
		}
		filter = append(filter, clause)
	}

	boolQ := map[string]any{"filter": filter}
	if p.Query != "" {
		boolQ["must"] = []any{buildFederatedTextQuery(p.Query, p.SecondaryScope)}
	}

	body := map[string]any{
		"query": map[string]any{"bool": boolQ},
		"from":  0,
		"size":  window,
		"sort":  federatedSort(),
	}
	if !p.IncludeSource {
		body["_source"] = false
	}
	return body, nil
}
