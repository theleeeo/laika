package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCoreForwardWalks(t *testing.T) {
	got, err := coreForwardWalks([]forwardWalkConfig{
		{
			ResourceType: "Product",
			Enabled:      true,
			Interval:     12 * time.Hour,
			PageSize:     50,
			PageInterval: 2 * time.Second,
			Metadata: []map[string]string{
				{"tenantId": "a"},
				{"tenantId": "b"},
			},
		},
		{ResourceType: "order", Enabled: true},
		{ResourceType: "customer", Enabled: false, Interval: time.Hour},
	})
	if err != nil {
		t.Fatalf("coreForwardWalks: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d entries %#v, want Product and order (a disabled entry is left out)", len(got), got)
	}

	product, ok := got["Product"]
	if !ok {
		t.Fatalf("no entry for Product: %#v", got)
	}
	if product.Interval != 12*time.Hour || product.PageSize != 50 || product.PageInterval != 2*time.Second {
		t.Errorf("Product pacing = %v/%d/%v, want 12h/50/2s", product.Interval, product.PageSize, product.PageInterval)
	}
	if product.Metadata == nil {
		t.Fatal("Product Metadata func is nil, want one returning its static maps")
	}
	maps, err := product.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Product Metadata: %v", err)
	}
	wantMaps := []map[string]string{{"tenantId": "a"}, {"tenantId": "b"}}
	if !reflect.DeepEqual(maps, wantMaps) {
		t.Errorf("Product Metadata = %#v, want %#v", maps, wantMaps)
	}

	order := got["order"]
	if order.Interval != 0 || order.PageSize != 0 || order.PageInterval != 0 {
		t.Errorf("order pacing = %v/%d/%v, want zeros (core applies the defaults)", order.Interval, order.PageSize, order.PageInterval)
	}
	if order.Metadata != nil {
		t.Error("order Metadata func is set, want nil for an empty list (one walk with no metadata)")
	}
}

func TestCoreForwardWalks_MetadataIsACopy(t *testing.T) {
	got, err := coreForwardWalks([]forwardWalkConfig{{
		ResourceType: "product",
		Enabled:      true,
		Metadata:     []map[string]string{{"tenantId": "a"}},
	}})
	if err != nil {
		t.Fatalf("coreForwardWalks: %v", err)
	}
	first, _ := got["product"].Metadata(context.Background())
	first[0]["tenantId"] = "changed"
	second, _ := got["product"].Metadata(context.Background())
	if second[0]["tenantId"] != "a" {
		t.Fatalf("a run's change to its maps reached the next run: %#v", second)
	}
}

func TestCoreForwardWalks_None(t *testing.T) {
	got, err := coreForwardWalks(nil)
	if err != nil {
		t.Fatalf("coreForwardWalks: %v", err)
	}
	if got != nil {
		t.Fatalf("got %#v, want nil", got)
	}
}

func TestCoreForwardWalks_DuplicateType(t *testing.T) {
	_, err := coreForwardWalks([]forwardWalkConfig{
		{ResourceType: "product", Enabled: true},
		{ResourceType: "product", Enabled: false},
	})
	if err == nil || !strings.Contains(err.Error(), "product") {
		t.Fatalf("err = %v, want an error naming the type configured twice", err)
	}
}
