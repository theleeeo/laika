package core

import (
	"errors"
	"fmt"
	"testing"
)

func TestNew_BuildBatchSizes(t *testing.T) {
	idx := mustNew(Config{Resources: testResources(), BuildBatchSizes: map[string]int{"product": 50}})
	if got := idx.batchSize("product"); got != 50 {
		t.Errorf("product's batch size = %d, want its override 50", got)
	}

	idx = mustNew(Config{Resources: testResources()})
	if got := idx.batchSize("product"); got != defaultBuildBatchSize {
		t.Errorf("product's batch size without an override = %d, want the default %d", got, defaultBuildBatchSize)
	}

	idx = mustNew(Config{Resources: testResources(), BuildBatchSize: 7, BuildBatchSizes: map[string]int{"product": 0}})
	if got := idx.batchSize("product"); got != 7 {
		t.Errorf("product's batch size with a 0 override = %d, want BuildBatchSize 7", got)
	}

	if _, err := New(Config{Resources: testResources(), BuildBatchSize: -1}); err == nil {
		t.Error("New accepted a negative BuildBatchSize")
	}
	if _, err := New(Config{Resources: testResources(), BuildBatchSizes: map[string]int{"product": -1}}); err == nil {
		t.Error("New accepted a negative BuildBatchSizes entry")
	}
	if _, err := New(Config{Resources: testResources(), BuildBatchSizes: map[string]int{"nope": 5}}); !errors.Is(err, ErrUnknownResource) {
		t.Errorf("New with an override for an unconfigured type: err = %v, want ErrUnknownResource", err)
	}
}

func TestIDBatches_ChunksInOrderAndDeduplicates(t *testing.T) {
	got := idBatches([]string{"a", "b", "a", "c", "d", "b", "e"}, 2)
	if want := "[[a b] [c d] [e]]"; fmt.Sprint(got) != want {
		t.Errorf("idBatches = %v, want %s", got, want)
	}
	if got := idBatches(nil, 3); len(got) != 0 {
		t.Errorf("idBatches(nil) = %v, want none", got)
	}
}
