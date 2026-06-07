package core

import (
	"context"
	"errors"
	"testing"

	"github.com/theleeeo/indexer/core/resource"
)

func testResources() resource.Configs {
	cfgs := resource.Configs{
		{
			Resource: "product",
			Versions: []resource.VersionConfig{
				{
					Version: 1,
					Fields: []resource.FieldConfig{
						{Name: "title", Type: "text"},
					},
				},
			},
		},
	}
	for _, c := range cfgs {
		c.ApplyDefaults()
	}
	return cfgs
}

func TestRebuild_EmptySelectors(t *testing.T) {
	idx := New(Config{Resources: testResources()})

	err := idx.Rebuild(context.Background(), nil)

	invalidArg, ok := errors.AsType[*InvalidArgumentError](err)
	if !ok {
		t.Fatalf("expected InvalidArgumentError, got %v", err)
	}
	if invalidArg.Msg != "at least one selector is required" {
		t.Fatalf("unexpected message: %q", invalidArg.Msg)
	}
}

func TestRebuild_UnknownResourceType(t *testing.T) {
	idx := New(Config{Resources: testResources()})

	err := idx.Rebuild(context.Background(), []ResourceSelector{
		{ResourceType: "nonexistent"},
	})
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
}

func TestRebuild_InvalidVersion(t *testing.T) {
	idx := New(Config{Resources: testResources()})

	err := idx.Rebuild(context.Background(), []ResourceSelector{
		{ResourceType: "product", Versions: []int{99}},
	})
	invalidArg, ok := errors.AsType[*InvalidArgumentError](err)
	if !ok {
		t.Fatalf("expected InvalidArgumentError, got %v", err)
	}
	if invalidArg.Msg != `resource "product" has no version 99` {
		t.Fatalf("unexpected message: %q", invalidArg.Msg)
	}
}

func TestRebuild_MultiVersionValidation(t *testing.T) {
	cfgs := resource.Configs{
		{
			Resource: "product",
			Versions: []resource.VersionConfig{
				{Version: 1, Fields: []resource.FieldConfig{{Name: "title"}}},
				{Version: 2, Fields: []resource.FieldConfig{{Name: "title"}, {Name: "price"}}},
			},
			ReadVersion: 1,
		},
	}
	idx := New(Config{Resources: cfgs})

	// Version 3 does not exist.
	err := idx.Rebuild(context.Background(), []ResourceSelector{
		{ResourceType: "product", Versions: []int{3}},
	})
	invalidArg, ok := errors.AsType[*InvalidArgumentError](err)
	if !ok {
		t.Fatalf("expected InvalidArgumentError, got %v", err)
	}
	if invalidArg.Msg != `resource "product" has no version 3` {
		t.Fatalf("unexpected message: %q", invalidArg.Msg)
	}

	// Mix of valid and invalid versions: should still fail.
	err = idx.Rebuild(context.Background(), []ResourceSelector{
		{ResourceType: "product", Versions: []int{1, 99}},
	})
	invalidArg, ok = errors.AsType[*InvalidArgumentError](err)
	if !ok {
		t.Fatalf("expected InvalidArgumentError, got %v", err)
	}
	if invalidArg.Msg != `resource "product" has no version 99` {
		t.Fatalf("unexpected message: %q", invalidArg.Msg)
	}
}

func TestRebuild_MultipleSelectorsValidation(t *testing.T) {
	cfgs := resource.Configs{
		{Resource: "product", Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "title"}}}}},
		{Resource: "order", Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "number"}}}}},
	}
	for _, c := range cfgs {
		c.ApplyDefaults()
	}
	idx := New(Config{Resources: cfgs})

	// First selector valid, second invalid.
	err := idx.Rebuild(context.Background(), []ResourceSelector{
		{ResourceType: "product"},
		{ResourceType: "nonexistent"},
	})
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
}
