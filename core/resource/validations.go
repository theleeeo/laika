package resource

import (
	"fmt"
)

func (c Configs) Validate() error {
	if len(c) == 0 {
		return fmt.Errorf("at least one resource config required")
	}

	// Verify that every individual config is valid
	for i, rc := range c {
		if err := rc.Validate(); err != nil {
			if rc.Resource != "" {
				return fmt.Errorf("resource %q: %w", rc.Resource, err)
			}
			return fmt.Errorf("resource %d: %w", i, err)
		}
	}

	if err := c.verifyFieldRelations(); err != nil {
		return err
	}

	return nil
}

// verifyFieldRelations verifies that all relations reference existing resources,
// that their field lists match the target resource's field definitions,
// that key sources reference valid resources, and that there are no dependency cycles.
func (c Configs) verifyFieldRelations() error {
	for _, rCfg := range c {
		// Check relations in every version definition.
		for i := range rCfg.Versions {
			vc := &rCfg.Versions[i]
			v := vc.Version
			for _, currentRel := range vc.Relations {
				// Verify that the related resource exists
				relRCfg := c.Get(currentRel.Resource)
				if relRCfg == nil {
					return fmt.Errorf("version %d: relation '%s'->'%s' is specified but resource '%s' does not exist", v, rCfg.Resource, currentRel.Resource, currentRel.Resource)
				}

				// Collect all field names across all versions of the target resource.
				allTargetFields := make(map[string]bool)
				for _, tvc := range relRCfg.Versions {
					for _, f := range tvc.Fields {
						allTargetFields[f.Name] = true
					}
				}

				// Verify that the related resource has the fields defined in the relation
				for _, f := range currentRel.Fields {
					if !allTargetFields[f.Name] {
						return fmt.Errorf("version %d: relation '%s'->'%s' specifies field '%s' which does not exist on '%s'", v, rCfg.Resource, currentRel.Resource, f.Name, currentRel.Resource)
					}
				}

				// Verify join from: when set it must name a sibling relation in this version,
				// and that sibling must not itself be a reference relation (whose data is
				// never materialized, so its key cannot be used as a join source).
				if currentRel.Join.From != "" {
					found := false
					for _, siblingRel := range vc.Relations {
						if siblingRel.Resource == currentRel.Join.From {
							found = true
							if siblingRel.IsReference() {
								return fmt.Errorf("version %d: relation '%s'->'%s' sources its key from '%s', which is a reference relation; a reference sibling's data is not materialized, so its key cannot be used as a join source", v, rCfg.Resource, currentRel.Resource, currentRel.Join.From)
							}
							break
						}
					}
					if !found {
						return fmt.Errorf("version %d: relation '%s'->'%s' join from '%s' is not a sibling relation", v, rCfg.Resource, currentRel.Resource, currentRel.Join.From)
					}
				}

				if currentRel.IsReference() {
					if err := verifyReferenceKeyReachable(rCfg.Resource, v, vc, currentRel); err != nil {
						return err
					}
				}
			}

			// Verify no cycles in relation key dependencies for this version
			if err := verifyNoCyclesVersion(rCfg.Resource, vc); err != nil {
				return err
			}
		}
	}

	return nil
}

// verifyNoCyclesVersion checks that the relation key dependencies within a single
// version config form a DAG (no cycles).
func verifyNoCyclesVersion(resourceName string, vc *VersionConfig) error {
	// Build adjacency: relation resource name -> the sibling relation its local field comes from
	deps := make(map[string]string) // relation -> dependency (the sibling its local field comes from)
	for _, rel := range vc.Relations {
		if rel.Join.From != "" {
			deps[rel.Resource] = rel.Join.From
		}
	}

	// Walk each chain to detect cycles
	visited := make(map[string]bool)
	inStack := make(map[string]bool)

	var visit func(node string) error
	visit = func(node string) error {
		if inStack[node] {
			return fmt.Errorf("resource %q has a cycle in relation key dependencies involving %q", resourceName, node)
		}
		if visited[node] {
			return nil
		}
		visited[node] = true
		inStack[node] = true

		if dep, ok := deps[node]; ok {
			if err := visit(dep); err != nil {
				return err
			}
		}

		inStack[node] = false
		return nil
	}

	for _, rel := range vc.Relations {
		if err := visit(rel.Resource); err != nil {
			return err
		}
	}

	return nil
}

func (c Config) Validate() error {
	if c.Resource == "" {
		return fmt.Errorf("resource required")
	}

	// Validate version configuration.
	for _, vc := range c.Versions {
		if vc.Version <= 0 {
			return fmt.Errorf("version %d: must be a positive integer", vc.Version)
		}
		if err := vc.Validate(c.Resource, vc.Version); err != nil {
			return err
		}
	}
	if c.ReadVersion != 0 {
		if c.GetVersion(c.ReadVersion) == nil {
			return fmt.Errorf("readVersion %d is not in versions %v", c.ReadVersion, c.SortedVersions())
		}
	}

	return nil
}

func (vc VersionConfig) Validate(resourceName string, version int) error {
	for i, f := range vc.Fields {
		if err := f.Validate(); err != nil {
			if f.Name != "" {
				return fmt.Errorf("version %d: field %q: %w", version, f.Name, err)
			}
			return fmt.Errorf("version %d: field %d: %w", version, i, err)
		}
	}

	for i, r := range vc.Relations {
		if err := r.Validate(); err != nil {
			if r.Resource != "" {
				return fmt.Errorf("version %d: relation %q: %w", version, r.Resource, err)
			}
			return fmt.Errorf("version %d: relation %d: %w", version, i, err)
		}
	}

	return nil
}

func (c FieldConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name required")
	}
	return nil
}

func (c RelationConfig) Validate() error {
	if c.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if err := c.Join.Validate(); err != nil {
		return fmt.Errorf("join: %w", err)
	}

	if c.Cardinality != "" && c.Cardinality != "one" && c.Cardinality != "many" {
		return fmt.Errorf("cardinality must be \"one\" or \"many\"")
	}

	if c.Strategy != "" && c.Strategy != StrategyDenormalize && c.Strategy != StrategyReference {
		return fmt.Errorf("strategy must be %q or %q", StrategyDenormalize, StrategyReference)
	}

	// TODO: Default to "Use all fields" if none specified?
	if len(c.Fields) == 0 {
		return fmt.Errorf("at least one field required")
	}

	for i, f := range c.Fields {
		if err := f.Validate(); err != nil {
			if f.Name != "" {
				return fmt.Errorf("field %q: %w", f.Name, err)
			}
			return fmt.Errorf("field %d: %w", i, err)
		}
	}

	return nil
}

func (j JoinConfig) Validate() error {
	if j.Local == "" {
		return fmt.Errorf("local required")
	}
	if j.Foreign == "" {
		return fmt.Errorf("foreign required")
	}
	return nil
}

// verifyReferenceKeyReachable ensures a reference relation's local join key is
// present on the indexed document so the two-phase search join can fold matching
// child IDs into a terms filter. The key is reachable either as a root field of
// the resource (Join.From == "") or as a denormalized field of the sibling
// relation named by Join.From (which must itself be a denormalize relation).
func verifyReferenceKeyReachable(resourceName string, version int, vc *VersionConfig, rel RelationConfig) error {
	key := rel.Join.Local

	if rel.Join.From == "" {
		for _, f := range vc.Fields {
			if f.Name == key {
				return nil
			}
		}
		return fmt.Errorf("version %d: reference relation '%s'->'%s' local key '%s' must be an indexed field on '%s'", version, resourceName, rel.Resource, key, resourceName)
	}

	for _, sib := range vc.Relations {
		if sib.Resource != rel.Join.From {
			continue
		}
		if sib.IsReference() {
			return fmt.Errorf("version %d: reference relation '%s'->'%s' sources its key from '%s', which must be a denormalize relation", version, resourceName, rel.Resource, rel.Join.From)
		}
		for _, f := range sib.Fields {
			if f.Name == key {
				return nil
			}
		}
		return fmt.Errorf("version %d: reference relation '%s'->'%s' local key '%s' must be a denormalized field of sibling '%s'", version, resourceName, rel.Resource, key, rel.Join.From)
	}

	return fmt.Errorf("version %d: reference relation '%s'->'%s' join from '%s' is not a sibling relation", version, resourceName, rel.Resource, rel.Join.From)
}
