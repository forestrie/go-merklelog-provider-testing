package mmrtesting

import (
	"fmt"

	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/google/uuid"
)

const (
	V1MMRPrefix = "v1/mmrs"

	V1MMRBlobNameFmt               = "%016d.log"
	V1MMRSignedTreeHeadBlobNameFmt = "%016d.sth"

	// Note: this is due to datatrails tenant schema
	V1MMRTenantPrefix = "v1/mmrs/tenant"
	LogInstanceN      = 0 // the log instance number, used to identify the log in the massif path
)

type DatatrailsPathPrefixProvider struct{}

func (d DatatrailsPathPrefixProvider) Prefix(logID storage.LogID, otype storage.ObjectType) (string, error) {
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData, storage.ObjectPathMassifs:
		return fmt.Sprintf("%s/%s/%d/massifs/", V1MMRPrefix, Log2TenantID(logID), storage.LogInstanceN), nil
	case storage.ObjectCheckpoint, storage.ObjectPathCheckpoints:
		return fmt.Sprintf("%s/%s/%d/massifseals/", V1MMRPrefix, Log2TenantID(logID), storage.LogInstanceN), nil
	default:
		return "", fmt.Errorf("unknown object type %v", otype)
	}
}

// PrefixWithHeight returns the base path format (without service-specific prefix) for testing.
// Returns: {massifHeight}/{uuid}/ for both massifs and checkpoints.
func (d DatatrailsPathPrefixProvider) PrefixWithHeight(logID storage.LogID, massifHeight uint8, otype storage.ObjectType) (string, error) {
	// Convert LogID to UUID string (without "tenant/" prefix for base format)
	uuidStr := fmt.Sprintf("%s", uuid.UUID(logID))

	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData, storage.ObjectPathMassifs:
		// Base format: {massifHeight}/{uuid}/
		return fmt.Sprintf("%d/%s/", massifHeight, uuidStr), nil
	case storage.ObjectCheckpoint, storage.ObjectPathCheckpoints:
		// Base format: {massifHeight}/{uuid}/ (same for checkpoints)
		return fmt.Sprintf("%d/%s/", massifHeight, uuidStr), nil
	default:
		return "", fmt.Errorf("unknown object type %v", otype)
	}
}

// LogID from the storage path according to the datatrails massif storage schema.
// The storage path is expected to be in the format:
// /v1/mmrs/tenant/<tenant_uuid>/<log_instance>/massifs/
// or
// /v1/mmrs/tenant/<tenant_uuid>/<log_instance>/massifseals/
func (d DatatrailsPathPrefixProvider) LogID(storagePath string) (storage.LogID, error) {
	logID := TenantID2LogID(storagePath)
	if logID != nil {
		return logID, nil
	}

	return nil, fmt.Errorf("invalid storage path prefix: %s", storagePath)
}

func Log2TenantID(logID storage.LogID) string {
	// Convert the LogID to a UUID and then to a string
	return fmt.Sprintf("tenant/%s", uuid.UUID(logID))
}

func TenantID2LogID(storagePath string) storage.LogID {

	return storage.ParsePrefixedLogID("tenant/", storagePath)
}
