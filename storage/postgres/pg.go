package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// GetParentResources returns the Parents of childResource: every resource
// with an edge to it in any Schema Version's set, each once.
func (s *Store) GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT DISTINCT resource, resource_id FROM relations WHERE related_resource=$1 AND related_resource_id=$2`,
		childResource.Type, childResource.Id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var parents []model.Resource
	for rows.Next() {
		var parentResource, parentResourceId string
		if err := rows.Scan(&parentResource, &parentResourceId); err != nil {
			return nil, err
		}
		parents = append(parents, model.Resource{Type: parentResource, Id: parentResourceId})
	}
	return parents, rows.Err()
}

// GetChildResources returns the Children of parentResource: the union of
// its stored edge sets across Schema Versions, each Child once.
func (s *Store) GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT DISTINCT related_resource, related_resource_id FROM relations WHERE resource=$1 AND resource_id=$2`,
		parentResource.Type, parentResource.Id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var children []model.Resource
	for rows.Next() {
		var childResource, childResourceId string
		if err := rows.Scan(&childResource, &childResourceId); err != nil {
			return nil, err
		}
		children = append(children, model.Resource{Type: childResource, Id: childResourceId})
	}
	return children, rows.Err()
}

// RemoveResource removes every stored edge set of resource, with its edges,
// in one transaction. It first locks the sets in ascending schema_version
// order, as ReplaceEdges does, so the two never deadlock, and then removes
// exactly the versions it locked: a version a concurrent replace stores
// after the lock statement's snapshot is left to that replace, as if it ran
// after this removal. The transaction is pinned to READ COMMITTED, whatever
// the server default: a lock that waited for a concurrent replace then locks
// the row that replace committed, where REPEATABLE READ would fail it with a
// serialization error. (The removal itself is unguarded; ordering it against
// a build is L2.1's.)
func (s *Store) RemoveResource(ctx context.Context, resource model.Resource) error {
	return pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT schema_version FROM edge_sets WHERE type=$1 AND id=$2
			 ORDER BY schema_version FOR UPDATE`,
			resource.Type, resource.Id,
		)
		if err != nil {
			return err
		}
		locked, err := pgx.CollectRows(rows, pgx.RowTo[int32])
		if err != nil || len(locked) == 0 {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM relations WHERE resource=$1 AND resource_id=$2 AND schema_version = ANY($3::int[])`,
			resource.Type, resource.Id, locked,
		); err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`DELETE FROM edge_sets WHERE type=$1 AND id=$2 AND schema_version = ANY($3::int[])`,
			resource.Type, resource.Id, locked,
		)
		return err
	})
}

// ReplaceEdges stores a build's edge sets per Schema Version, guarded by the
// Build Sequence (see core.Store), in one transaction of two phases.
//
// Phase one takes the edge_sets row of every version the call touches, one
// statement per version, in ascending version order: a version in sets
// with a guarded upsert that accepts buildSeq when the stored stamp is not
// above it, and — when declared is non-nil — a stored version outside both
// declared and sets with a delete guarded the same way. A returned row is
// an accepted or dropped version. Phase two rewrites the relations of
// those versions in statements of their own: under READ COMMITTED a guard
// that waited for a concurrent replace's row lock re-checks against the row
// that replace committed, but a later clause of the same statement would
// read the pre-lock snapshot and miss the edges it inserted. Each phase-two
// statement starts after every lock is held, so it sees them. The
// transaction is pinned to READ COMMITTED, whatever the server default: under
// REPEATABLE READ that waiting guard would fail with a serialization error
// instead of re-checking.
//
// It cannot deadlock against another ReplaceEdges, whatever declared each
// carries, or against RemoveResource: each takes a resource's edge_sets
// rows in ascending schema_version order and holds them to commit, and
// touches a version's relations only while holding that version's row. So
// a call only ever waits for a row above every row it holds, and no cycle
// of waits can form.
//
// The undeclared versions are read without a lock before phase one, so the
// call drops only versions stored before that read. The outcome is decided
// per version, not as if the whole of a concurrent replace ran after this
// call: say this call (sequence 20, declared {1}, sets {1}) reads no
// undeclared version, and then a replace at 10 with no declared list stores
// versions 1 and 2 and commits. This call overwrites version 1 and leaves
// version 2, stamped 10 and undeclared. That is harmless: the leftover set
// only adds edges — extra fanout to this resource — until the next live
// build, which declares its versions, prunes it.
func (s *Store) ReplaceEdges(ctx context.Context, resource model.Resource, buildSeq int64, sets []core.EdgeSet, declared []int) error {
	children := make(map[int][]model.Resource, len(sets))
	for _, set := range sets {
		if _, dup := children[set.SchemaVersion]; dup {
			return fmt.Errorf("replace edges of %s/%s: schema version %d named twice", resource.Type, resource.Id, set.SchemaVersion)
		}
		children[set.SchemaVersion] = set.Children
	}
	return pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		versions := slices.Collect(maps.Keys(children))
		if declared != nil {
			rows, err := tx.Query(ctx,
				`SELECT schema_version FROM edge_sets
				 WHERE type=$1 AND id=$2 AND NOT schema_version = ANY($3::int[])`,
				resource.Type, resource.Id, declared,
			)
			if err != nil {
				return err
			}
			undeclared, err := pgx.CollectRows(rows, pgx.RowTo[int32])
			if err != nil {
				return err
			}
			for _, v := range undeclared {
				if _, inSets := children[int(v)]; !inSets {
					versions = append(versions, int(v))
				}
			}
		}
		slices.Sort(versions)

		var cleared, addVersions []int
		var addTypes, addIds []string
		for _, v := range versions {
			kids, inSets := children[v]
			guarded := `DELETE FROM edge_sets
			            WHERE type=$1 AND id=$2 AND schema_version=$3 AND build_seq <= $4
			            RETURNING true`
			if inSets {
				guarded = `INSERT INTO edge_sets AS e (type, id, schema_version, build_seq)
				           VALUES ($1, $2, $3, $4)
				           ON CONFLICT (type, id, schema_version) DO UPDATE
				           SET build_seq = EXCLUDED.build_seq
				           WHERE e.build_seq <= EXCLUDED.build_seq
				           RETURNING true`
			}
			var ok bool
			err := tx.QueryRow(ctx, guarded, resource.Type, resource.Id, v, buildSeq).Scan(&ok)
			if errors.Is(err, pgx.ErrNoRows) {
				continue // stamped above buildSeq: left unchanged
			}
			if err != nil {
				return err
			}
			cleared = append(cleared, v)
			for _, c := range kids {
				addVersions = append(addVersions, v)
				addTypes = append(addTypes, c.Type)
				addIds = append(addIds, c.Id)
			}
		}
		if len(cleared) == 0 {
			return nil
		}

		if _, err := tx.Exec(ctx,
			`DELETE FROM relations r
			 WHERE r.resource=$1 AND r.resource_id=$2 AND r.schema_version = ANY($3::int[])
			   AND NOT EXISTS (
			       SELECT 1 FROM unnest($4::int[], $5::text[], $6::text[]) AS x(v, t, i)
			       WHERE x.v = r.schema_version AND x.t = r.related_resource AND x.i = r.related_resource_id
			   )`,
			resource.Type, resource.Id, cleared, addVersions, addTypes, addIds,
		); err != nil {
			return err
		}
		if len(addVersions) == 0 {
			return nil
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO relations (resource, resource_id, schema_version, related_resource, related_resource_id)
			 SELECT $1, $2, v, t, i FROM unnest($3::int[], $4::text[], $5::text[]) AS x(v, t, i)
			 ON CONFLICT DO NOTHING`,
			resource.Type, resource.Id, addVersions, addTypes, addIds,
		)
		return err
	})
}

// RegisterChanges records a batch of changes in one statement, so each
// accepted item's version, its stale mark and its Parents' marks commit
// together or not at all. The item upsert's WHERE is the stale-version check:
// a delete or a version-0 item is always accepted, a versioned one only when
// strictly newer. Parents are found from the accepted items only, so a stale
// item marks nothing; accepted in-batch items are excluded from the Parent
// mark because one statement may modify a row only once, and their own row
// already carries the mark. A Parent shared by several accepted children
// stores the metadata of the last one in batch order.
//
// Every accepted row — upsert, version-0 item or delete — is stamped with a
// fresh Change Sequence value; a rejected item and a marked Parent keep
// theirs. The value is drawn for every input row, so a rejected item leaves
// a gap in the sequence, which the drift check doesn't mind.
//
// Every row the statement marks — accepted item or Parent — is also claimed
// for a Build owner when it has none or its owner's lease has expired: the
// claim is decided in the upsert's SET, under the row lock ON CONFLICT takes,
// so of two concurrent marks of one row only the first to lock it claims.
// RETURNING sees only the new row (Postgres 17 has no RETURNING OLD), so a
// claim reads as owner_seq = stale_seq: an owner that was not replaced holds
// an older stale_seq, since marks only ever bump it.
func (s *Store) RegisterChanges(ctx context.Context, items []core.Registration, lease time.Duration) (core.Registered, error) {
	if len(items) == 0 {
		return core.Registered{}, nil
	}
	types := make([]string, len(items))
	ids := make([]string, len(items))
	deleted := make([]bool, len(items))
	versions := make([]int64, len(items))
	metadata := make([]map[string]string, len(items))
	for i, it := range items {
		types[i], ids[i] = it.Resource.Type, it.Resource.Id
		deleted[i] = it.Deleted
		if !it.Deleted {
			versions[i] = it.Version
		}
		metadata[i] = it.Metadata
	}
	// One JSON array index-aligned with the items; a nil map marshals to
	// JSON null, stored as SQL NULL like MarkStale stores nil metadata.
	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return core.Registered{}, err
	}

	rows, err := s.pool.Query(ctx,
		`WITH input AS (
		     SELECT x.ord, x.t, x.i, x.del, x.v, NULLIF(m.meta, 'null'::jsonb) AS meta
		     FROM unnest($1::text[], $2::text[], $3::bool[], $4::bigint[]) WITH ORDINALITY AS x(t, i, del, v, ord)
		     JOIN jsonb_array_elements($5::jsonb) WITH ORDINALITY AS m(meta, ord) USING (ord)
		 ),
		 accepted AS (
		     INSERT INTO resources AS r (type, id, version, deleted, stale_seq, stale_since, metadata, change_seq, owner_seq, owner_since)
		     SELECT t, i, v, del, 1, now(), meta, nextval('change_sequence'), 1, now() FROM input
		     ON CONFLICT (type, id) DO UPDATE
		     SET version = CASE WHEN EXCLUDED.deleted THEN 0
		                        WHEN EXCLUDED.version = 0 THEN r.version
		                        ELSE EXCLUDED.version END,
		         deleted = EXCLUDED.deleted,
		         change_seq = EXCLUDED.change_seq,
		         stale_seq = r.stale_seq + 1,
		         stale_since = COALESCE(r.stale_since, now()),
		         metadata = EXCLUDED.metadata,
		         owner_seq = CASE WHEN `+claimable("$6")+` THEN r.stale_seq + 1 ELSE r.owner_seq END,
		         owner_since = CASE WHEN `+claimable("$6")+` THEN now() ELSE r.owner_since END
		     WHERE EXCLUDED.deleted OR EXCLUDED.version = 0 OR r.version < EXCLUDED.version
		     RETURNING r.type, r.id, r.stale_seq, r.owner_seq IS NOT DISTINCT FROM r.stale_seq AS claimed
		 ),
		 parents AS (
		     SELECT DISTINCT ON (rel.resource, rel.resource_id)
		            rel.resource AS t, rel.resource_id AS i, input.meta
		     FROM accepted a
		     JOIN input ON input.t = a.type AND input.i = a.id
		     JOIN relations rel ON rel.related_resource = a.type AND rel.related_resource_id = a.id
		     WHERE NOT EXISTS (SELECT 1 FROM accepted a2 WHERE a2.type = rel.resource AND a2.id = rel.resource_id)
		     ORDER BY rel.resource, rel.resource_id, input.ord DESC
		 ),
		 marked AS (
		     INSERT INTO resources AS r (type, id, stale_seq, stale_since, metadata, owner_seq, owner_since)
		     SELECT t, i, 1, now(), meta, 1, now() FROM parents
		     ON CONFLICT (type, id) DO UPDATE
		     SET stale_seq = r.stale_seq + 1,
		         stale_since = COALESCE(r.stale_since, now()),
		         metadata = EXCLUDED.metadata,
		         owner_seq = CASE WHEN `+claimable("$6")+` THEN r.stale_seq + 1 ELSE r.owner_seq END,
		         owner_since = CASE WHEN `+claimable("$6")+` THEN now() ELSE r.owner_since END
		     RETURNING r.type, r.id, r.stale_seq, r.owner_seq IS NOT DISTINCT FROM r.stale_seq AS claimed, r.metadata
		 )
		 SELECT input.ord, a.stale_seq, a.claimed, NULL::text, NULL::text, NULL::jsonb
		 FROM input LEFT JOIN accepted a ON a.type = input.t AND a.id = input.i
		 UNION ALL
		 SELECT NULL, stale_seq, claimed, type, id, metadata FROM marked`,
		types, ids, deleted, versions, metaJSON, lease.Microseconds(),
	)
	if err != nil {
		return core.Registered{}, err
	}
	defer rows.Close()

	out := core.Registered{Items: make([]core.RegisteredItem, len(items))}
	for rows.Next() {
		var ord, staleSeq *int64
		var claimed *bool
		var typ, id *string
		var meta map[string]string
		if err := rows.Scan(&ord, &staleSeq, &claimed, &typ, &id, &meta); err != nil {
			return core.Registered{}, err
		}
		if staleSeq == nil {
			continue // a rejected item
		}
		var token int64
		if *claimed {
			token = *staleSeq
		}
		if ord == nil {
			out.Parents = append(out.Parents, core.MarkedParent{Resource: model.Resource{Type: *typ, Id: *id}, Metadata: meta, Token: token})
			continue
		}
		out.Items[*ord-1] = core.RegisteredItem{Accepted: true, StaleSeq: *staleSeq, Token: token}
	}
	if err := rows.Err(); err != nil {
		return core.Registered{}, err
	}
	return out, nil
}

var _ core.Store = (*Store)(nil)

// claimable is the SQL condition under which a statement claims the row
// aliased r for a Build owner: it has no owner, or its owner's lease has
// expired. lease is the placeholder of the lease in microseconds. A lease of
// zero treats every owner as expired; MarkStale is the one statement that
// reads zero as "claim nothing" instead (core never passes zero otherwise).
// now() is the transaction start, so a claim that waited for the row lock
// stamps owner_since with the wait included: the lease runs short by it.
func claimable(lease string) string {
	return `(r.owner_seq IS NULL OR r.owner_since IS NULL OR r.owner_since < now() - ` + lease + `::bigint * interval '1 microsecond')`
}

// MarkStale durably records build intent for the given resources: bump
// stale_seq and set stale_since — keeping the OLDEST timestamp, so
// "stale for too long" measures the oldest unserved change. The notification
// metadata is stored alongside the mark (last mark wins) so a sweep-recovered
// build runs with the same context an inline build would have.
//
// A lease above zero also claims each row without a live owner, as
// RegisterChanges does, and returns those; a lease of zero leaves the owner
// columns alone and returns nil.
func (s *Store) MarkStale(ctx context.Context, resources []model.Resource, metadata map[string]string, lease time.Duration) ([]core.Owned, error) {
	if len(resources) == 0 {
		return nil, nil
	}
	types := make([]string, len(resources))
	ids := make([]string, len(resources))
	for i, r := range resources {
		types[i] = r.Type
		ids[i] = r.Id
	}
	if lease <= 0 {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO resources AS r (type, id, stale_seq, stale_since, metadata)
			 SELECT DISTINCT t, i, 1, now(), $3::jsonb FROM unnest($1::text[], $2::text[]) AS x(t, i)
			 ON CONFLICT (type, id) DO UPDATE
			 SET stale_seq = r.stale_seq + 1,
			     stale_since = COALESCE(r.stale_since, now()),
			     metadata = EXCLUDED.metadata`,
			types, ids, metadata,
		)
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`WITH marked AS (
		     INSERT INTO resources AS r (type, id, stale_seq, stale_since, metadata, owner_seq, owner_since)
		     SELECT DISTINCT t, i, 1, now(), $3::jsonb, 1, now() FROM unnest($1::text[], $2::text[]) AS x(t, i)
		     ON CONFLICT (type, id) DO UPDATE
		     SET stale_seq = r.stale_seq + 1,
		         stale_since = COALESCE(r.stale_since, now()),
		         metadata = EXCLUDED.metadata,
		         owner_seq = CASE WHEN `+claimable("$4")+` THEN r.stale_seq + 1 ELSE r.owner_seq END,
		         owner_since = CASE WHEN `+claimable("$4")+` THEN now() ELSE r.owner_since END
		     RETURNING r.type, r.id, r.stale_seq, r.owner_seq
		 )
		 SELECT type, id, stale_seq FROM marked WHERE owner_seq = stale_seq`,
		types, ids, metadata, lease.Microseconds(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var owned []core.Owned
	for rows.Next() {
		var o core.Owned
		if err := rows.Scan(&o.Type, &o.Id, &o.Token); err != nil {
			return nil, err
		}
		owned = append(owned, o)
	}
	return owned, rows.Err()
}

// BeginBuild atomically bumps the Build Sequence (ES external_gte OCC version),
// captures the current stale_seq for the race-safe finish at the end of the
// build, and takes the build's start from the Change Sequence in the same
// statement. It leaves the row's change_seq alone: a build is not a change.
// A token that is the row's owner token renews the owner's lease.
func (s *Store) BeginBuild(ctx context.Context, resource model.Resource, token int64) (core.BuildBegun, error) {
	var b core.BuildBegun
	err := s.pool.QueryRow(ctx,
		`INSERT INTO resources AS r (type, id, build_idx)
		 VALUES ($1, $2, 1)
		 ON CONFLICT (type, id) DO UPDATE
		 SET build_idx = r.build_idx + 1,
		     owner_since = CASE WHEN $3::bigint <> 0 AND r.owner_seq = $3 THEN now() ELSE r.owner_since END
		 RETURNING build_idx, stale_seq, nextval('change_sequence')`,
		resource.Type, resource.Id, token,
	).Scan(&b.BuildIdx, &b.StaleSeq, &b.Start)
	if err != nil {
		return core.BuildBegun{}, err
	}
	return b, nil
}

// ownedArrays splits ownerships into the parallel arrays the owner
// statements unnest.
func ownedArrays(owned []core.Owned) (types, ids []string, tokens []int64) {
	types = make([]string, len(owned))
	ids = make([]string, len(owned))
	tokens = make([]int64, len(owned))
	for i, o := range owned {
		types[i], ids[i], tokens[i] = o.Type, o.Id, o.Token
	}
	return types, ids, tokens
}

// RenewOwners renews the lease of every given ownership whose token is still
// the row's owner token and returns those: the ownerships still held.
func (s *Store) RenewOwners(ctx context.Context, owned []core.Owned) ([]core.Owned, error) {
	if len(owned) == 0 {
		return nil, nil
	}
	types, ids, tokens := ownedArrays(owned)
	rows, err := s.pool.Query(ctx,
		`UPDATE resources r SET owner_since = now()
		 FROM unnest($1::text[], $2::text[], $3::bigint[]) AS x(t, i, token)
		 WHERE r.type = x.t AND r.id = x.i AND x.token <> 0 AND r.owner_seq = x.token
		 RETURNING r.type, r.id, r.owner_seq`,
		types, ids, tokens,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var held []core.Owned
	for rows.Next() {
		var o core.Owned
		if err := rows.Scan(&o.Type, &o.Id, &o.Token); err != nil {
			return nil, err
		}
		held = append(held, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return held, nil
}

// ReleaseOwners drops every given ownership whose token is still the row's
// owner token. The stale mark stays for the next change or the sweep.
func (s *Store) ReleaseOwners(ctx context.Context, owned []core.Owned) error {
	if len(owned) == 0 {
		return nil
	}
	types, ids, tokens := ownedArrays(owned)
	_, err := s.pool.Exec(ctx,
		`UPDATE resources r SET owner_seq = NULL, owner_since = NULL
		 FROM unnest($1::text[], $2::text[], $3::bigint[]) AS x(t, i, token)
		 WHERE r.type = x.t AND r.id = x.i AND x.token <> 0 AND r.owner_seq = x.token`,
		types, ids, tokens,
	)
	return err
}

// NextChangeSeq takes a value of the Change Sequence.
func (s *Store) NextChangeSeq(ctx context.Context) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT nextval('change_sequence')`).Scan(&seq)
	return seq, err
}

// AnyChangedSince reports whether any checked resource's change_seq exceeds
// its check's Start, all checks in one query. A resource without a row has
// never changed. It takes no row locks: a registration numbered above a
// start but not yet committed is not seen (seam S6).
func (s *Store) AnyChangedSince(ctx context.Context, checks []core.ChangeCheck) (bool, error) {
	if len(checks) == 0 {
		return false, nil
	}
	types := make([]string, len(checks))
	ids := make([]string, len(checks))
	starts := make([]int64, len(checks))
	for i, c := range checks {
		types[i], ids[i], starts[i] = c.Resource.Type, c.Resource.Id, c.Start
	}
	var changed bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (
		     SELECT 1
		     FROM unnest($1::text[], $2::text[], $3::bigint[]) AS x(t, i, start)
		     JOIN resources r ON r.type = x.t AND r.id = x.i
		     WHERE r.change_seq > x.start
		 )`,
		types, ids, starts,
	).Scan(&changed)
	return changed, err
}

// ClearStale clears the stale mark, and any ownership with it, only if no
// newer change arrived since the build captured staleSeq. A moved seq makes
// this a no-op, leaving the row stale for its owner's follow-up or the sweep.
func (s *Store) ClearStale(ctx context.Context, resource model.Resource, staleSeq int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE resources SET stale_since = NULL, owner_seq = NULL, owner_since = NULL
		 WHERE type=$1 AND id=$2 AND stale_seq=$3`,
		resource.Type, resource.Id, staleSeq,
	)
	return err
}

// FinishOwned finishes an owned build in one statement: a settled mark
// (stale_seq still staleSeq) is cleared with its ownership whatever the
// token; a moved one is re-claimed for a follow-up if token is still the
// owner token. The WHERE is re-checked against the latest row version when a
// concurrent mark committed after the statement's snapshot, so a mark that
// lands while it waits for the row lock moves it into the re-claim branch
// rather than being cleared.
func (s *Store) FinishOwned(ctx context.Context, resource model.Resource, staleSeq, token int64) (core.FollowUp, error) {
	var f core.FollowUp
	var owner *int64
	err := s.pool.QueryRow(ctx,
		`UPDATE resources
		 SET stale_since = CASE WHEN stale_seq = $3 THEN NULL ELSE stale_since END,
		     owner_seq = CASE WHEN stale_seq = $3 THEN NULL ELSE stale_seq END,
		     owner_since = CASE WHEN stale_seq = $3 THEN NULL ELSE now() END
		 WHERE type=$1 AND id=$2 AND (stale_seq = $3 OR ($4::bigint <> 0 AND owner_seq = $4))
		 RETURNING owner_seq, metadata, deleted`,
		resource.Type, resource.Id, staleSeq, token,
	).Scan(&owner, &f.Metadata, &f.Deleted)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && owner == nil) {
		return core.FollowUp{}, nil
	}
	if err != nil {
		return core.FollowUp{}, err
	}
	f.Token = *owner
	return f, nil
}

// DeleteResourceIfSeq hard-deletes a tombstoned row, guarded by stale_seq so a
// concurrent re-create (which bumps the seq) wins over the in-flight delete;
// the ownership goes with the row. When the seq moved and token is still the
// owner token, the owner re-claims the row for its follow-up. The two
// statements run in order, each on its own snapshot, so a re-create that
// commits while the delete waits for the row lock is seen by the re-claim.
func (s *Store) DeleteResourceIfSeq(ctx context.Context, resource model.Resource, staleSeq, token int64) (core.FollowUp, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM resources WHERE type=$1 AND id=$2 AND stale_seq=$3 AND deleted`,
		resource.Type, resource.Id, staleSeq,
	)
	if err != nil || tag.RowsAffected() > 0 {
		return core.FollowUp{}, err
	}
	var f core.FollowUp
	err = s.pool.QueryRow(ctx,
		`UPDATE resources SET owner_seq = stale_seq, owner_since = now()
		 WHERE type=$1 AND id=$2 AND stale_seq <> $3 AND $4::bigint <> 0 AND owner_seq = $4
		 RETURNING owner_seq, metadata, deleted`,
		resource.Type, resource.Id, staleSeq, token,
	).Scan(&f.Token, &f.Metadata, &f.Deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return core.FollowUp{}, nil
	}
	if err != nil {
		return core.FollowUp{}, err
	}
	return f, nil
}

// ListStale returns up to limit resources whose stale mark is older than
// before and that have no live owner, oldest first, including delete
// tombstones, and claims each one in the same statement at its current
// stale_seq (no bump), which is its Token. Each entry carries the metadata
// stored by its most recent mark. The candidates are locked FOR UPDATE SKIP
// LOCKED inside the claiming UPDATE, so of two concurrent sweeps only one
// claims a row: the other skips it while it is locked, and re-checks the
// owner condition against the claimed row once it has committed.
func (s *Store) ListStale(ctx context.Context, before time.Time, limit int, lease time.Duration) ([]core.StaleResource, error) {
	rows, err := s.pool.Query(ctx,
		`WITH candidates AS (
		     SELECT r.type, r.id FROM resources r
		     WHERE r.stale_since IS NOT NULL AND r.stale_since < $1 AND `+claimable("$3")+`
		     ORDER BY r.stale_since
		     LIMIT $2
		     FOR UPDATE SKIP LOCKED
		 ),
		 claimed AS (
		     UPDATE resources r SET owner_seq = r.stale_seq, owner_since = now()
		     FROM candidates c WHERE r.type = c.type AND r.id = c.id
		     RETURNING r.type, r.id, r.stale_seq, r.deleted, r.metadata, r.stale_since
		 )
		 SELECT type, id, stale_seq, deleted, metadata FROM claimed ORDER BY stale_since`,
		before, limit, lease.Microseconds(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []core.StaleResource
	for rows.Next() {
		var e core.StaleResource
		if err := rows.Scan(&e.Type, &e.Id, &e.StaleSeq, &e.Deleted, &e.Metadata); err != nil {
			return nil, err
		}
		e.Token = e.StaleSeq
		out = append(out, e)
	}
	return out, rows.Err()
}

// CountStale returns how many resources of the type (tombstones included)
// have a stale mark older than before, and the oldest such mark — zero when
// the count is zero. This is core.StaleCounter, the cutover readiness gate's
// view of the backlog.
func (s *Store) CountStale(ctx context.Context, resourceType string, before time.Time) (int, time.Time, error) {
	var count int
	var oldest *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT count(*), min(stale_since) FROM resources
		 WHERE type=$1 AND stale_since IS NOT NULL AND stale_since < $2`,
		resourceType, before,
	).Scan(&count, &oldest)
	if err != nil {
		return 0, time.Time{}, err
	}
	if oldest == nil {
		return count, time.Time{}, nil
	}
	return count, *oldest, nil
}
