package core

import (
	"reflect"
	"testing"

	"github.com/theleeeo/indexer/model"
)

func TestGroupResourceIDsByType_GroupsAndSorts(t *testing.T) {
	roots := []model.Resource{
		{Type: "b", Id: "2"},
		{Type: "a", Id: "3"},
		{Type: "a", Id: "1"},
		{Type: "b", Id: "1"},
	}

	got := groupResourceIDsByType(roots)
	want := map[string][]string{
		"a": {"1", "3"},
		"b": {"1", "2"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected grouping: got %#v, want %#v", got, want)
	}
}

func TestGroupResourceIDsByType_DeduplicatesIDsPerType(t *testing.T) {
	roots := []model.Resource{
		{Type: "a", Id: "1"},
		{Type: "a", Id: "1"},
		{Type: "a", Id: "2"},
		{Type: "b", Id: "1"},
		{Type: "b", Id: "1"},
	}

	got := groupResourceIDsByType(roots)
	want := map[string][]string{
		"a": {"1", "2"},
		"b": {"1"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected grouping: got %#v, want %#v", got, want)
	}
}
