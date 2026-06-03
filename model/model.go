package model

type Resource struct {
	Type string
	Id   string
}

// VersionedResource is a resource with an observed version from the source.
// Version == 0 means the provider did not supply a version.
type VersionedResource struct {
	Resource
	Version int64
}
