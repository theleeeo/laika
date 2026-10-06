package postgres

import (
	"context"
	"testing"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// A resource's metadata is its own: a registration writes it on the item's
// row only, so a child's registration marks its Parent and leaves the
// Parent's metadata as it was — whether the mark claims the Parent or finds
// it owned, and whether the Parent holds metadata or none.
func TestRegisterChanges_AParentMarkLeavesTheParentsMetadata(t *testing.T) {
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "mdp", Id: id} }
	held, owned, none, child := r("held"), r("owned"), r("none"), r("child")

	register(t, st,
		core.Registration{Resource: held, Version: 1, Metadata: meta("held")},
		core.Registration{Resource: owned, Version: 1, Metadata: meta("owned")},
	)
	expireOwner(t, testPool, held) // the child's mark claims held; owned keeps its live owner
	seed(t, none, 0, 0, false)
	relate(t,
		[2]model.Resource{held, child},
		[2]model.Resource{owned, child},
		[2]model.Resource{none, child},
	)
	before := map[model.Resource]int64{}
	for _, p := range []model.Resource{held, owned, none} {
		_, _, before[p], _, _ = row(t, p)
	}

	got := register(t, st, core.Registration{Resource: child, Version: 1, Metadata: meta("child")})

	if len(got.Parents) != 3 {
		t.Fatalf("Parents: got %+v, want held, owned and none", got.Parents)
	}
	for _, p := range got.Parents {
		if _, _, seq, since, _ := row(t, p.Resource); seq <= before[p.Resource] || since == nil {
			t.Fatalf("%s: the child's registration must mark it: seq %d (was %d) since %v", p.Id, seq, before[p.Resource], since)
		}
		if p.Resource == owned && p.Token != 0 || p.Resource != owned && p.Token == 0 {
			t.Fatalf("%s: token %d, want a claim on every Parent but the live-owned one", p.Id, p.Token)
		}
	}
	requireMetadata(t, "held's metadata", metadataOf(t, held), meta("held"))
	requireMetadata(t, "owned's metadata", metadataOf(t, owned), meta("owned"))
	if m := rawMetadata(t, none); m != nil {
		t.Fatalf("none: metadata %q, want NULL still", *m)
	}
	requireMetadata(t, "the child's own metadata", metadataOf(t, child), meta("child"))
}

// BeginBuild reads the row's metadata, so an owned build that waited runs
// with metadata registered after its claim: the second registration finds a
// live owner and claims nothing, but stores its metadata on the row.
func TestBeginBuild_ReturnsMetadataRegisteredAfterTheClaim(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "mdb", Id: "1"}

	claim := register(t, st, core.Registration{Resource: res, Version: 1, Metadata: meta("md0")}).Items[0]
	if claim.Token == 0 {
		t.Fatalf("setup: the first registration must claim the row: %+v", claim)
	}
	if later := register(t, st, core.Registration{Resource: res, Version: 2, Metadata: meta("md1")}).Items[0]; !later.Accepted || later.Token != 0 {
		t.Fatalf("setup: the second registration must be accepted unclaimed under the live owner: %+v", later)
	}

	begun, err := st.BeginBuild(ctx, res, claim.Token)
	if err != nil {
		t.Fatal(err)
	}
	requireMetadata(t, "BeginBuild's metadata", begun.Metadata, meta("md1"))
}

// Ruling R4: a row without metadata — NULL or the empty object — begins a
// build with nil Metadata, as does a row BeginBuild creates.
func TestBeginBuild_ReturnsNilMetadataForARowWithNone(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	empty := `{}`
	for _, c := range []struct {
		name string
		seed func(res model.Resource)
	}{
		{"NULL", func(res model.Resource) { seedMetadata(t, res, nil) }},
		{"empty object", func(res model.Resource) { seedMetadata(t, res, &empty) }},
		{"a row BeginBuild creates", func(model.Resource) {}},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := model.Resource{Type: "mdn", Id: c.name}
			c.seed(res)

			begun, err := st.BeginBuild(ctx, res, 0)
			if err != nil {
				t.Fatal(err)
			}
			if begun.Metadata != nil {
				t.Fatalf("BeginBuild's metadata: got %#v, want nil", begun.Metadata)
			}
		})
	}
}
