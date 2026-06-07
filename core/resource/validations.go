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

				// Verify key source: must be the owning resource or a sibling relation in this version
				if currentRel.Key.Source != rCfg.Resource {
					found := false
					for _, siblingRel := range vc.Relations {
						if siblingRel.Resource == currentRel.Key.Source {
							found = true
							break
						}
					}
					if !found {
						return fmt.Errorf("version %d: relation '%s'->'%s' key source '%s' is not the root resource and not a sibling relation", v, rCfg.Resource, currentRel.Resource, currentRel.Key.Source)
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
	// Build adjacency: relation resource name -> list of deps (key sources that are sibling relations)
	deps := make(map[string]string) // relation -> dependency (its key source, if it's a sibling)
	for _, rel := range vc.Relations {
		if rel.Key.Source != resourceName {
			deps[rel.Resource] = rel.Key.Source
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

	if err := c.Key.Validate(); err != nil {
		return fmt.Errorf("key: %w", err)
	}

	if c.Cardinality != "" && c.Cardinality != "one" && c.Cardinality != "many" {
		return fmt.Errorf("cardinality must be \"one\" or \"many\"")
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

func (k KeyConfig) Validate() error {
	if k.Source == "" {
		return fmt.Errorf("source required")
	}
	if k.Field == "" {
		return fmt.Errorf("field required")
	}
	return nil
}
