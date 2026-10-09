package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"slices"
)

// CreateAlias creates an alias pointing to the given index.
// If the alias already exists pointing elsewhere, it is moved atomically.
func (c *Client) CreateAlias(ctx context.Context, aliasName, indexName string) error {
	body := map[string]any{
		"actions": []any{
			map[string]any{
				"remove": map[string]any{
					"index": "*",
					"alias": aliasName,
				},
			},
			map[string]any{
				"add": map[string]any{
					"index": indexName,
					"alias": aliasName,
				},
			},
		},
	}

	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal alias body: %w", err)
	}

	res, err := c.es.Indices.UpdateAliases(
		bytes.NewReader(b),
		c.es.Indices.UpdateAliases.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("update aliases: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return fmt.Errorf("update aliases error: %s %s", res.Status(), string(raw))
	}

	return nil
}

// GetAlias returns the concrete index name that the alias currently points to.
// Returns empty string and no error if the alias does not exist.
func (c *Client) GetAlias(ctx context.Context, aliasName string) (string, error) {
	res, err := c.es.Indices.GetAlias(
		c.es.Indices.GetAlias.WithName(aliasName),
		c.es.Indices.GetAlias.WithContext(ctx),
	)
	if err != nil {
		return "", fmt.Errorf("get alias: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return "", nil
	}

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return "", fmt.Errorf("get alias error: %s %s", res.Status(), string(raw))
	}

	// Response shape: { "index_name": { "aliases": { "alias_name": {} } } }
	var decoded map[string]any
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return "", fmt.Errorf("decode alias response: %w", err)
	}

	for indexName := range decoded {
		return indexName, nil
	}

	return "", nil
}

// GetAliasTargets returns every index the alias points to, sorted. Returns
// none and no error if the alias does not exist.
func (c *Client) GetAliasTargets(ctx context.Context, aliasName string) ([]string, error) {
	res, err := c.es.Indices.GetAlias(
		c.es.Indices.GetAlias.WithName(aliasName),
		c.es.Indices.GetAlias.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("get alias: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, nil
	}

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("get alias error: %s %s", res.Status(), string(raw))
	}

	// Response shape: { "index_name": { "aliases": { "alias_name": {} } } }
	var decoded map[string]any
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return nil, fmt.Errorf("decode alias response: %w", err)
	}

	targets := make([]string, 0, len(decoded))
	for indexName := range decoded {
		targets = append(targets, indexName)
	}
	slices.Sort(targets)
	return targets, nil
}

// GetMapping returns the running "mappings" object for indexName (the value
// under the index's "mappings" key, i.e. containing "properties"). The second
// return is false, with a nil error, when the index does not exist.
func (c *Client) GetMapping(ctx context.Context, indexName string) (map[string]any, bool, error) {
	res, err := c.es.Indices.GetMapping(
		c.es.Indices.GetMapping.WithIndex(indexName),
		c.es.Indices.GetMapping.WithContext(ctx),
	)
	if err != nil {
		return nil, false, fmt.Errorf("get mapping: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, false, nil
	}

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return nil, false, fmt.Errorf("get mapping error: %s %s", res.Status(), string(raw))
	}

	// Response shape: { "<index>": { "mappings": { "properties": {...} } } }.
	var decoded map[string]any
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return nil, false, fmt.Errorf("decode mapping response: %w", err)
	}

	for _, v := range decoded {
		entry, ok := v.(map[string]any)
		if !ok {
			continue
		}
		mappings, ok := entry["mappings"].(map[string]any)
		if !ok {
			return nil, false, fmt.Errorf("mapping response for %q missing mappings object", indexName)
		}
		return mappings, true, nil
	}

	return nil, false, fmt.Errorf("mapping response for %q was empty", indexName)
}

// IndexExists reports whether the concrete index exists.
func (c *Client) IndexExists(ctx context.Context, indexName string) (bool, error) {
	res, err := c.es.Indices.Exists(
		[]string{indexName},
		c.es.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, fmt.Errorf("index exists: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return false, nil
	}
	if res.IsError() {
		return false, fmt.Errorf("index exists error: %s", res.Status())
	}
	return true, nil
}

// CountDocs returns the number of documents in the concrete index. A missing
// index is an error, not a zero count — existence is IndexExists's question.
func (c *Client) CountDocs(ctx context.Context, indexName string) (int64, error) {
	res, err := c.es.Count(
		c.es.Count.WithIndex(indexName),
		c.es.Count.WithContext(ctx),
	)
	if err != nil {
		return 0, fmt.Errorf("count docs: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return 0, fmt.Errorf("count docs error: %s %s", res.Status(), string(raw))
	}

	var decoded struct {
		Count int64 `json:"count"`
	}
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return 0, fmt.Errorf("decode count response: %w", err)
	}
	return decoded.Count, nil
}
